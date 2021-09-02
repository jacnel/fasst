#pragma once
#ifndef MICA_TABLE_CTABLE_IMPL_COMMIT_DEL_H_
#define MICA_TABLE_CTABLE_IMPL_COMMIT_DEL_H_

namespace mica {
namespace table {

// Whenever we read a deleted item, its non-existence needs to be recorded in
// the cache. To do so, we assume that the value was either prepared or set
// already. Then, we mark the item as logically deleted so that any future read
// on that cached value will observe its deleted state.

template <class StaticConfig>
Result CTable<StaticConfig>::commit_del(uint32_t caller_id, uint64_t key_hash,
                                        const char* key, size_t key_length) {
  assert(key_length <= kMaxKeyLength);

  uint32_t bucket_index = calc_bucket_index(key_hash);
  uint16_t tag = calc_tag(key_hash);

  Bucket* bucket = buckets_ + bucket_index;

  // The bucket must be previously locked during prepare_write()
  uint32_t out_locker_id;
  if (!is_locked(bucket, &caller_id) && caller_id != out_locker_id) {
    return Result::kError;
  }

  Bucket* located_bucket;
  size_t item_index =
      find_item_index(bucket, key_hash, tag, key, key_length, &located_bucket);

  if (item_index == StaticConfig::kBucketSize) {
    unlock_bucket(bucket, caller_id);
    stat_inc(&Stats::delete_notfound);
    return Result::kNotFound;
  }

  // Get a handle to the current item.
  size_t size;
  uint64_t item_offset = get_item_offset(located_bucket->item_vec[item_index]);
  Item* item = reinterpret_cast<Item*>(pool_->get_item(item_offset, &size));

  if (item->pending) {  // Should never be called on a pending item.
    unlock_bucket(bucket, caller_id);
    return Result::kError;
  }

  // Logically delete the existing item.
  update_item_value(item, nullptr, 0, /*deleted=*/true);

  unlock_bucket(bucket, caller_id);

  stat_dec(&Stats::count);
  stat_inc(&Stats::delete_found);
  return Result::kSuccess;
}
}  // namespace table
}  // namespace mica

#endif
