#ifndef CACHE_H
#define CACHE_H

#include "hots.h"
#include "mappings/mappings.h"
#include "rpc/rpc.h"
#include "util/rte_memcpy.h"

#include "mica/table/ctable.h"
#include "mica/util/hash.h"

#include <assert.h>

typedef ::mica::table::BasicLossyCTableConfig CTableConfig;
typedef ::mica::table::CTable<CTableConfig> Cache;
typedef ::mica::table::Result CacheResult; /* An enum */

// General datastore defines
#define cache_dassert(x)               \
  do {                                 \
    if (CACHE_DEBUG_ASSERT) assert(x); \
  } while (0)
#define cache_dassert_msg(x, msg)                    \
  do {                                               \
    if (CACHE_DEBUG_ASSERT) HOTS_ASSERT_MSG(x, msg); \
  } while (0)

// Steal the top two bits to encode the request type.
#define cache_hashmask ((1ull << 62) - 1)
static uint64_t cache_keyhash(hots_key_t key) {
  return ::mica::util::hash((char *)&key, sizeof(hots_key_t)) & cache_hashmask;
}

//+ Implement read_and_inval
enum class cache_reqtype_t {
  inval,
  read_and_inval,
};

/* Resp type is sent in the RPC's coalesced message request (response) header */
enum class cache_resptype_t {
  inval_success,
  inval_failure,
  read_and_inval_success,
  read_and_inval_failure,
};

// A cache only has one kind of request type, since it only processes
// invalidations.
struct cache_inval_req_t {
  uint32_t unused; /* Make struct size multiple of 8 bytes */
  uint32_t caller_id;
  uint64_t req_type : 2;
  uint64_t keyhash : 62;
  hots_key_t key;
};
static_assert(sizeof(cache_inval_req_t) == 3 * sizeof(uint64_t), "");

forceinline Cache *cache_init(std::string config_filepath,
                              Cache::EvictionCallback callback) {
  auto config = ::mica::util::Config::load_file(config_filepath);

  /* Check that CRCW mode is set */
  auto table_config = config.get("table");
  if (!table_config.get("concurrent_read").get_bool() ||
      !table_config.get("concurrent_write").get_bool()) {
    fprintf(stderr, "HoTS Error: CTable only supports CRCW. Exiting.\n");
    exit(-1);
  }

  CTableConfig::Alloc *alloc = new CTableConfig::Alloc(config.get("alloc"));
  CTableConfig::Pool *pool = new CTableConfig::Pool(config.get("pool"), alloc);

  Cache *cache = new Cache(config.get("table"), alloc, pool, callback);
  return cache;
}

/* Datastore request format checks : done once ever */
// static void ds_do_checks() {
//   /*
//    * Check if the keyhash field of both GET and PUT requests is aligned. This
//    * is required for prefetching in the RPC subsystem.
//    */
//   ds_generic_get_req_t gg_req;
//   ds_generic_put_req_t *gp_req = (ds_generic_put_req_t *)&gg_req;
//   gg_req.keyhash = 2207; /* Some magic number */
//   assert(gg_req.keyhash == gp_req->keyhash);
//   _unused(gg_req);
//   _unused(gp_req);
// }

forceinline size_t cache_rpc_handler(uint8_t *resp_buf,
                                     rpc_resptype_t *resp_type,
                                     const uint8_t *req_buf, size_t req_len,
                                     void *_cache) {
  // Sanity checks
  cache_dassert(resp_buf != nullptr);
  cache_dassert(is_aligned(resp_buf, sizeof(int32_t)));

  cache_dassert(req_buf != nullptr);
  cache_dassert(is_aligned(resp_buf, sizeof(int32_t)));

  cache_dassert(_cache != nullptr);

  CacheResult cache_result;
  Cache *cache = reinterpret_cast<Cache *>(_cache);

  const cache_inval_req_t *req =
      reinterpret_cast<const cache_inval_req_t *>(req_buf);
  uint32_t caller_id = req->caller_id;
  cache_reqtype_t req_type = static_cast<cache_reqtype_t>(req->req_type);
  uint64_t keyhash = req->keyhash;

  uint64_t *_hdr = (uint64_t *)resp_buf;
  _unused(_hdr);
  char *_val_buf = (char *)resp_buf + sizeof(uint64_t);
  _unused(_val_buf);

  switch (req_type) {
    case cache_reqtype_t::inval:
      cache_result = cache->invalidate(caller_id, keyhash);
      *resp_type = static_cast<uint64_t>(cache_resptype_t::inval_success);
      assert(cache_result == CacheResult::kSuccess);
      break;
    case cache_reqtype_t::read_and_inval:
      //+ Read the value then invalidate.
      // cache_result =
      //     cache->get(keyhash, &req->key, sizeof(hots_key_t), _val_buf, );
      std::cerr << "Not implemented!" << std::endl;
      cache_dassert(false);
      break;
    default:
      std::cerr << "Invalid request type: " << req->req_type << std::endl;
      cache_dassert(false);
      break;
  }

  return 0;
}

forceinline size_t cache_forge_inval_req(rpc_req_t *rpc_req, uint32_t caller_id,
                                         hots_key_t key,
                                         cache_reqtype_t req_type) {
  cache_dassert(req_type == cache_reqtype_t::inval);

  cache_dassert(rpc_req != NULL && rpc_req->req_buf != NULL);
  cache_dassert(is_aligned(rpc_req->req_buf, sizeof(uint32_t)));
  cache_dassert(rpc_req->available_bytes() >= sizeof(cache_inval_req_t));

  {
    /* Real work */
    cache_inval_req_t *inval_req = (cache_inval_req_t *)rpc_req->req_buf;
    inval_req->caller_id = caller_id;
    inval_req->req_type = static_cast<uint64_t>(req_type);
    inval_req->keyhash = cache_keyhash(key);

    return sizeof(cache_inval_req_t);
  }
}
#endif /* CACHE_H */
