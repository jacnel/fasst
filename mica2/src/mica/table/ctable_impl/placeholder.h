#pragma once
#ifndef MICA_TABLE_CTABLE_IMPL_INSERT_PLACEHOLDER_H_
#define MICA_TABLE_CTABLE_IMPL_INSERT_PLACEHOLDER_H_

namespace mica {
namespace table {

// Invalidates all keys in the same bucket as keyhash
template <class StaticConfig>
Result CTable<StaticConfig>::placeholder(uint32_t caller_id, uint64_t key_hash,
                                         const char* key, size_t key_length,
                                         size_t value_length,
                                         uint64_t* out_version) {
  // Obtain the bucket containing the key and insert a placeholder entry for the
  // given key.
  uint32_t bucket_index = calc_bucket_index(key_hash);
  uint16_t tag = calc_tag(key_hash);

  Bucket* bucket = buckets_ + bucket_index;

  lock_bucket(bucket, caller_id);

  // The reaainder of this function is basically identical to a set, but
  // overwrites the item no matter what.
  Bucket* located_bucket;
  size_t item_index =
      find_item_index(bucket, key_hash, tag, key, key_length, &located_bucket);

  // Set always expects that the item exists in the cache.
  if (item_index != StaticConfig::kBucketSize) {
    unlock_bucket(bucket, caller_id);
    return Result::kExists;
  }

  // Since there is no existing entry, pick an item with the same tag; this is
  // potentially the same item but invalid due to insufficient log space this
  // helps limiting the effect of "ghost" items in the table when the log space
  // is slightly smaller than enough and makes better use of the log space by
  // not evicting unrelated old items in the same bucket
  item_index = find_same_tag(bucket, tag, &located_bucket);
  if (item_index == StaticConfig::kBucketSize) {
    // if there is no matching tag, we just use the empty or oldest item
    item_index = get_empty_or_oldest(bucket, &located_bucket);
  }

  // Whether we are evicting
  bool evicting = located_bucket->item_vec[item_index] != 0;

  uint32_t new_item_size = static_cast<uint32_t>(
      sizeof(Item) + ::mica::util::roundup<8>(key_length) +
      ::mica::util::roundup<8>(value_length));

  // we have to lock the pool because is_valid check must be correct in the
  // overwrite mode; unlike reading, we cannot afford writing data at a wrong
  // place
  pool_->lock();

  // Allocate a new item and populate it
  uint64_t new_item_offset = pool_->allocate(new_item_size);
  if (new_item_offset == Pool::kInsufficientSpace) {
    // no more space
    // TODO: add a statistics entry
    unlock_bucket(bucket, caller_id);
    return Result::kInsufficientSpace;
  }

  uint64_t new_tail = Specialization::get_tail(pool_);
  Item* new_item = reinterpret_cast<Item*>(pool_->get_item(new_item_offset));

  stat_inc(&Stats::set_new);
  set_pending_item(new_item, key_hash, key, (uint32_t)key_length,
                   (uint32_t)value_length);

  // Release any evicted item and perform callback if necessary.
  if (evicting) {
    uint64_t item_offset =
        get_item_offset(located_bucket->item_vec[item_index]);

    // Execute eviction callback on modified items only.
    Item* item = reinterpret_cast<Item*>(pool_->get_item(item_offset));
    if (item->modified) {
      size_t key_length = get_key_length(item->kv_length_vec);
      size_t value_length = get_value_length(item->kv_length_vec);
      callback_(item->data, key_length, item->data + key_length, value_length);
      stat_inc(&Stats::callbacks_made);
    }
    pool_->release(item_offset);
  }

  // unlocking is delayed until we finish writing data at the new location;
  // otherwise, the new location may be invalidated (in a very rare case)
  pool_->unlock();

  located_bucket->item_vec[item_index] = make_item_vec(tag, new_item_offset);

  // Save the bucket version after it is unlocked.
  *out_version = get_next_version(bucket);

  unlock_bucket(bucket, caller_id);

  // XXX: this may be too late; before cleaning, other threads may have read
  // some invalid location
  //      ideally, this should be done before writing actual data
  cleanup_bucket(caller_id, new_item_offset, new_tail);

  stat_inc(&Stats::count);

  return Result::kSuccess;
}

}  // namespace table
}  // namespace mica

#endif