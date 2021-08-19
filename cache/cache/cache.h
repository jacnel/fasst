#ifndef CACHE_H
#define CACHE_H

#include "hots.h"
#include "mappings/mappings.h"
#include "rpc/rpc.h"
#include "util/rte_memcpy.h"

#include "mica/table/ctable.h"
#include "mica/util/hash.h"

#include <assert.h>

typedef mica::table::BasicLossyCTableConfig CTableConfig;
typedef mica::table::CTable<CTableConfig> Cache;

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
};

// A cache only has one kind of request type, since it only processes
// invalidations.
struct cache_inval_req_t {
  uint32_t unused; /* Make struct size multiple of 8 bytes */
  uint32_t caller_id;
  uint64_t req_type : 2;
  uint64_t keyhash : 62;
};
static_assert(sizeof(cache_inval_req_t) == 2 * sizeof(uint64_t), "");

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

forceinline size_t cache_forge_inval_req(rpc_req_t *rpc_req, uint32_t caller_id,
                                         hots_key_t key, uint64_t keyhash,
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
    inval_req->keyhash = keyhash;

    return sizeof(cache_inval_req_t);
  }
}
#endif /* CACHE_H */
