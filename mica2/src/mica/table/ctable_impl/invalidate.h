#pragma once
#ifndef MICA_TABLE_CTABLE_IMPL_INVAL_H_
#define MICA_TABLE_CTABLE_IMPL_INVAL_H_

namespace mica {
namespace table {

// Invalidates all keys in the same bucket as keyhash
template <class StaticConfig>
Result CTable<StaticConfig>::invalidate(uint32_t caller_id, uint64_t keyhash) {
  // Obtain the bucket containing the key and lock it since we are modifying
  // it.
  uint64_t bucket_index = calc_bucket_index(keyhash);
  Bucket* bucket = buckets_ + bucket_index;

  if (!try_lock_bucket(bucket, caller_id)) {
    return Result::kLocked;
  }

  // For each entry in the bucket, perform the eviction callback, return the
  // memory to the pool, then mark it as unused in the bucket.
  size_t item_index;
  for (item_index = 0; item_index < StaticConfig::kBucketSize; ++item_index) {
    uint64_t item_vec = bucket->item_vec[item_index];
    if (item_vec == 0) continue;
    uint64_t item_offset = get_item_offset(item_vec);

    Item* item = reinterpret_cast<Item*>(pool_->get_item(item_offset));
    if (item->modified) {
      // Execute eviction callback on modified items only.
      size_t key_length = get_key_length(item->kv_length_vec);
      size_t value_length = get_value_length(item->kv_length_vec);
      callback_(item->data, key_length, item->data + key_length, value_length);
      stat_inc(&Stats::callbacks_made);
    }

    //! Not sure if this is correct. Cases exist where the pool is locked when
    //! allocating from a CicularLogTag or when releasing to a SegregatedFitTag,
    //! but never both. This is confusing... but maybe the other ops are
    //! lock-free.
    if (std::is_base_of<::mica::pool::CircularLogTag,
                        typename Pool::Tag>::value) {
      pool_->release(item_offset);  // Does nothing
    } else {
      pool_->lock();
      pool_->release(item_offset);
      pool_->unlock();
    }

    bucket->item_vec[item_index] = 0;  // Logical deletion
  }

  unlock_bucket(bucket, caller_id);
  return Result::kSuccess;
}

}  // namespace table
}  // namespace mica

#endif