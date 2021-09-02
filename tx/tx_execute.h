#ifndef TX_EXECUTE_H
#define TX_EXECUTE_H

// Prepares the cache and issues directory requests. Currently, the directory
// only acts as a mechanism to enable cache coherence not to change the
// ownership of a data item.
forceinline tx_status_t Tx::prepare_tx() {
  for (size_t i = rs_index; i < read_set.size(); i++) {
    tx_rwset_item_t item = read_set[i];
    if (item.is_cached) continue;  // Already read.

    Cache *cache = cache_mgr->get_cache(item.rpc_reqtype);
    CacheResult result = cache->placeholder(
        coro_id, item.keyhash, reinterpret_cast<const char *>(&item.key),
        sizeof(hots_key_t), sizeof(hots_obj_t), &(item.incarnation));
    switch (result) {
      case CacheResult::kSuccess:
      case CacheResult::kExists:
        break;
      case CacheResult::kInsufficientSpace:
      default:
        assert(false);  // Something bad happened
    }

    directory_entry_t dir_entry;
    DirectoryResult dir_result = dir_client->lookup(item.key, &dir_entry);
    if (dir_result != DirectoryResult::kSuccess) {
      // Item is being updated, so abort.
      return tx_status_t::must_abort;
    } else {
      item.primary_mn = mappings->get_primary_mn(item.keyhash);
      for (int i = 0; i < mappings->num_backups; i++) {
        item.backup_mn[i] =
            mappings->get_backup_mn_from_primary(item.primary_mn, i);
      }
    }
  }

  for (size_t i = ws_index; i < write_set.size(); i++) {
    tx_rwset_item_t item = write_set[i];
    if (item.is_cached) continue;

    Cache *cache = cache_mgr->get_cache(item.rpc_reqtype);
    CacheResult result = cache->placeholder(
        coro_id, item.keyhash, reinterpret_cast<const char *>(&item.key),
        sizeof(hots_key_t), sizeof(hots_obj_t), &item.incarnation);
    switch (result) {
      case CacheResult::kSuccess:
      case CacheResult::kExists:
        break;
      case CacheResult::kInsufficientSpace:
      default:
        assert(false);  // Something bad happened
    }

    // Acquiring ownership at the directory means that no other writers can
    // write to this value unless they invalidate its owner first. It is like a
    // lock in that sense, but that others can ask for ownership to be released
    // through the invalidation callback provided by the cache.
    directory_entry_t dir_entry;
    DirectoryResult dir_result = dir_client->acquire(item.key, &dir_entry);
    if (dir_result != DirectoryResult::kSuccess) {
      // Item is being updated, so abort.
      return tx_status_t::must_abort;
    } else {
      item.primary_mn = mappings->get_primary_mn(item.keyhash);
      for (int i = 0; i < mappings->num_backups; i++) {
        item.backup_mn[i] =
            mappings->get_backup_mn_from_primary(item.primary_mn, i);
      }
      item.dir_entry = dir_entry;
    }
  }
  return tx_status_t::in_progress;
}

/* Read keys */
forceinline tx_status_t Tx::do_read(coro_yield_t &yield) {
  tx_dassert(tx_status == tx_status_t::in_progress);

  tx_dassert(read_set.size() + write_set.size() <= RPC_MAX_MSG_CORO);
  tx_dassert(rs_index <= read_set.size());
  tx_dassert(ws_index <= write_set.size());

#if TX_ENABLE_LOCK_SERVER == 1
  if (mappings->use_lock_server) {
    tx_stat_inc(stat_lockserver_lock_req, 1);
    bool lock_success = send_lockserver_req(yield, locksrv_reqtype_t::lock);
    if (!lock_success) {
      tx_status = tx_status_t::must_abort;
      return tx_status;
    } else {
      tx_stat_inc(stat_lockserver_lock_req_success, 1);
      /* Record so we know whether to unlock on abort */
      lockserver_locked = true;
    }
  }
#endif

  tx_status = prepare_tx();
  if (tx_status == tx_status_t::must_abort) {
    // Abort early if cache cannot be prepared.
    return tx_status;
  }

  rpc->clear_req_batch(coro_id);
  size_t req_i = 0; /* Separate index bc we'll fetch both read, write set */

  /* Read the read set */
  for (size_t i = rs_index; i < read_set.size(); i++) {
    tx_rwset_item_t &item = read_set[i];

    if (item.is_cached) continue;  // Skip cached items.

    rpc_req_t *req =
        rpc->start_new_req(coro_id, item.rpc_reqtype, item.primary_mn,
                           (uint8_t *)&item.obj->hdr, sizeof(hots_obj_t));

    tx_req_arr[req_i] = req;
    req_i++;

    size_t size_req = ds_forge_generic_get_req(
        req, caller_id, item.key, item.keyhash, ds_reqtype_t::get_rdonly);
    req->freeze(size_req);
  }

  /* Read + lock the write set */
  for (size_t i = ws_index; i < write_set.size(); i++) {
    tx_rwset_item_t &item = write_set[i];

    if (item.is_cached) continue;  // Skip cached items.

    rpc_req_t *req =
        rpc->start_new_req(coro_id, item.rpc_reqtype, item.primary_mn,
                           (uint8_t *)&item.obj->hdr, sizeof(hots_obj_t));

    tx_req_arr[req_i] = req;
    req_i++;

    size_t size_req;
    /* In the execute phase, update and delete keys are handled similarly */
    if (item.write_mode != tx_write_mode_t::insert) {
      /* Update or delete */
      size_req = ds_forge_generic_get_req(
          req, caller_id, item.key, item.keyhash, ds_reqtype_t::get_for_upd);
    } else {
      /* Insert */
      size_req = ds_forge_generic_get_req(
          req, caller_id, item.key, item.keyhash, ds_reqtype_t::lock_for_ins);
    }

    req->freeze(size_req);
  }

  tx_dassert(req_i > 0 && req_i <= RPC_MAX_MSG_CORO);

  rpc->send_reqs(coro_id);
  tx_yield(yield);

  req_i = 0;

  /*
   * a. Sanity-check the response.
   * b. Record read set versions for validation.
   * c. Record locking status of all write set keys to unlock on abort.
   */
  for (size_t i = rs_index; i < read_set.size(); i++) {
    tx_rwset_item_t &item = read_set[i];
    ds_resptype_t resp_type = (ds_resptype_t)tx_req_arr[req_i]->resp_type;

    if (item.is_cached) continue;  // Already read from cache.

    /* Hdr for successfully read keys need not be locked (bkt collison) */
    switch (resp_type) {
      case ds_resptype_t::get_rdonly_success: {
        /* Response contains header and value */
        item.obj->val_size = tx_req_arr[req_i]->resp_len - sizeof(hots_hdr_t);
        check_item(item); /* Checks @val_size */

        /* Save fields needed for validation */
        item.exec_rs_exists = true;
        item.exec_rs_version = item.obj->hdr.version;

        // If the item was not originally cached, then we have to add it to our
        // cache now. To do so, we finalize the prepared value (done earlier) so
        // that it is stored locally.
        Cache *cache = cache_mgr->get_cache(item.rpc_reqtype);
        CacheResult result = cache->prepare_read(
            coro_id, item.keyhash, reinterpret_cast<const char *>(&item.key),
            sizeof(hots_key_t), reinterpret_cast<const char *>(&item.obj),
            sizeof(hots_obj_t), item.incarnation,
            /*deleted=*/false);
        _unused(result);
        break;
      }
      case ds_resptype_t::get_rdonly_not_found:
        /* Txn need not be aborted if a rdonly key is not found. */
        tx_dassert(tx_req_arr[req_i]->resp_len == sizeof(uint64_t));

        item.obj->val_size = 0;

        /* Save fields needed for validation */
        item.exec_rs_exists = false;
        item.exec_rs_version = item.obj->hdr.version;

        //+ Finalize cache (?)
        //
        // At this point we can leave the placeholder entry since another Tx may
        // use it eventually. The same reasoning can be used below. Nothing
        // needs to be done because eventually it will be overwritten or will be
        // finalized by a concurrent transaction.
        break;
      case ds_resptype_t::get_rdonly_locked:
        tx_dassert(tx_req_arr[req_i]->resp_len == 0);
        tx_status = tx_status_t::must_abort;
        break;
      default:
        printf(
            "Tx: Unknown response type %u for read set key "
            "%" PRIu64 "\n.",
            tx_req_arr[req_i]->resp_type, item.key);
    }

    req_i++;
  }

  for (size_t i = ws_index; i < write_set.size(); i++) {
    tx_rwset_item_t &item = write_set[i];

    if (item.is_cached) {
      // Lock the bucket for cached items.
      Cache *cache = cache_mgr->get_cache(item.rpc_reqtype);
      CacheResult result = cache->prepare_write(
          coro_id, item.keyhash, reinterpret_cast<const char *>(&item.key),
          sizeof(hots_key_t), reinterpret_cast<const char *>(&item.obj),
          sizeof(hots_obj_t), item.incarnation,
          item.write_mode == tx_write_mode_t::del);
      switch (result) {
        case CacheResult::kSuccess:
        case CacheResult::kExists:
          item.is_cached = true;  // Item is now cached.
          break;
        case CacheResult::kNotFound:
        case CacheResult::kInsufficientSpace:
          tx_status = tx_status_t::must_abort;
          break;
        default:
          break;
      }
    }

    ds_resptype_t resp_type = (ds_resptype_t)tx_req_arr[req_i]->resp_type;

    if (item.write_mode != tx_write_mode_t::insert) {
      // Update or delete
      switch (resp_type) {
        case ds_resptype_t::get_for_upd_success: {
          tx_dassert(item.obj->hdr.locked == 1);

          item.obj->val_size = tx_req_arr[req_i]->resp_len - sizeof(hots_hdr_t);
          check_item(item); /* Checks @val_size */

          item.exec_ws_locked = true; /* Mark for unlock on abort */

          // Writes the value to the cache but leaves the bucket locked so that
          // write-write conflicts are protected. If a non-pending item already
          // exists (because of a concurrent operation) then prepare_write()
          // just locks the bucket.
          Cache *cache = cache_mgr->get_cache(item.rpc_reqtype);
          CacheResult result = cache->prepare_write(
              coro_id, item.keyhash, reinterpret_cast<const char *>(&item.key),
              sizeof(hots_key_t), reinterpret_cast<const char *>(&item.obj),
              sizeof(hots_obj_t), item.incarnation,
              /*deleted=*/item.write_mode == tx_write_mode_t::del);

          switch (result) {
            case CacheResult::kSuccess:
            case CacheResult::kExists:
              item.is_cached = true;  // Item is now cached.
              break;
            case CacheResult::kNotFound:
            case CacheResult::kInsufficientSpace:
              tx_status = tx_status_t::must_abort;
              break;
            default:
              break;
          }

          break;
        }
        case ds_resptype_t::get_for_upd_not_found:
        case ds_resptype_t::get_for_upd_locked:
          tx_dassert(tx_req_arr[req_i]->resp_len == 0);

          item.exec_ws_locked = false; /* Don't unlock on abort */
          tx_status = tx_status_t::must_abort;
          break;
        default:
          printf(
              "Tx: Unknown response type %u for write set "
              "(non-insert) key %" PRIu64 "\n.",
              tx_req_arr[req_i]->resp_type, item.key);
          exit(-1);
      }
    } else {
      // Insert
      switch (resp_type) {
        case ds_resptype_t::lock_for_ins_success: {
          tx_dassert(item.obj->hdr.locked == 1);
          tx_dassert(tx_req_arr[req_i]->resp_len ==
                     sizeof(hots_hdr_t)); /* Just the header */
          item.exec_ws_locked = true;     /* Mark for delete on abort */

          // Writes the value to the cache but leaves the bucket locked so that
          // write-write conflicts are protected. If a non-pending item already
          // exists (because of a concurrent operation) then prepare_write()
          // just locks the bucket.
          Cache *cache = cache_mgr->get_cache(item.rpc_reqtype);
          CacheResult result = cache->prepare_write(
              coro_id, item.keyhash, reinterpret_cast<const char *>(&item.key),
              sizeof(hots_key_t), reinterpret_cast<const char *>(&item.obj),
              sizeof(hots_obj_t), item.incarnation,
              /*deleted=*/false);
          switch (result) {
            case CacheResult::kSuccess:
              item.is_cached = true;  // Item is now cached.
              break;
            case CacheResult::kExists:
              cache->abort_write(coro_id, item.keyhash);  // Unlocks bucket.
            case CacheResult::kNotFound:
            case CacheResult::kInsufficientSpace:
              tx_status = tx_status_t::must_abort;
              break;
            default:
              break;
          }

          break;
        }
        case ds_resptype_t::lock_for_ins_exists:
        case ds_resptype_t::lock_for_ins_locked:
          tx_dassert(tx_req_arr[req_i]->resp_len == 0);
          tx_status = tx_status_t::must_abort;
          item.exec_ws_locked = false; /* Don't unlock on abort */
          break;
        default:
          printf(
              "Tx: Unknown response type %u for write set "
              "(insert) key %" PRIu64 "\n.",
              tx_req_arr[req_i]->resp_type, item.key);
          exit(-1);
      }
    }

    req_i++;
  }

  /*
   * These indices only make sense if we return ex_success, so no need to
   * update them in error cases.
   */
  rs_index = read_set.size();
  ws_index = write_set.size();

  tx_dassert(tx_status == tx_status_t::in_progress ||
             tx_status == tx_status_t::must_abort);
  return tx_status;
}

#endif /* TX_EXECUTE_H */
