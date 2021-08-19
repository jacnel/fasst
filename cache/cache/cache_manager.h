#ifndef CACHE_CACHE_MANAGER_H
#define CACHE_CACHE_MANAGER_H

#include <unordered_map>
#include "cache.h"

class CacheManager {
 public:
  // Adds a cache for a given request type, if none exists already.
  bool RegisterCache(rpc_reqtype_t req_type, Cache* cache) {
    if (cache_map_.find(req_type) != cache_map_.end()) {
      // Already registered
      return false;
    }
    cache_map_.insert({req_type, cache});
    return true;
  }

  // Returns the cache associated with a given request type, or nullptr if none
  // exists. Used by the transaction manager to lookup the cache to use.
  Cache* GetCache(rpc_reqtype_t req_type) {
    auto cache_iter = cache_map_.find(req_type);
    if (cache_iter == cache_map_.end()) {
      // Already registered
      return nullptr;
    }
    return cache_iter->second;
  }

 private:
  std::unordered_map<uint8_t, Cache*> cache_map_;
};

#endif