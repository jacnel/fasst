#pragma once
#ifndef MICA_TABLE_CTABLE_IMPL_SET_H_
#define MICA_TABLE_CTABLE_IMPL_SET_H_

namespace mica {
namespace table {

template <class StaticConfig>
Result CTable<StaticConfig>::prepare_read(uint32_t caller_id, uint64_t key_hash,
                                          const char* key, size_t key_length,
                                          const char* value,
                                          size_t value_length,
                                          uint64_t expected_bucket_version,
                                          bool deleted) {
  assert(key_length <= kMaxKeyLength);
  assert(value_length <= kMaxValueLength);

  uint32_t bucket_index = calc_bucket_index(key_hash);
  uint16_t tag = calc_tag(key_hash);

  Bucket* bucket = buckets_ + bucket_index;

  lock_bucket(bucket, caller_id);

  // This check must be performed after the bucket is locked to avoid another
  // invalidation interleaving. If it fails, it means that an invalidation came
  // between the prepare() call that populated expected_incarnation and now.
  if (expected_bucket_version != get_version(bucket)) {
    stat_inc(&Stats::cache_invalidated);
    unlock_bucket(bucket, caller_id);
    return Result::kInvalidated;
  }

  // The reaainder of this function is basically identical to a set, but
  // overwrites the item no matter what.
  Bucket* located_bucket;
  size_t item_index =
      find_item_index(bucket, key_hash, tag, key, key_length, &located_bucket);

  // Set always expects that the item exists in the cache.
  if (item_index == StaticConfig::kBucketSize) {
    unlock_bucket(bucket, caller_id);
    return Result::kNotFound;
  }

  uint32_t new_item_size = static_cast<uint32_t>(
      sizeof(Item) + ::mica::util::roundup<8>(key_length) +
      ::mica::util::roundup<8>(value_length));
  uint64_t item_offset = 0;

  // we have to lock the pool because is_valid check must be correct in the
  // overwrite mode; unlike reading, we cannot afford writing data at a wrong
  // place
  pool_->lock();

  // Get a handle to the current item.
  item_offset = get_item_offset(located_bucket->item_vec[item_index]);

  size_t old_item_size;
  Item* item =
      reinterpret_cast<Item*>(pool_->get_item(item_offset, &old_item_size));

  if (!item->pending) {
    // Already finalized.
    pool_->unlock();
    unlock_bucket(bucket, caller_id);
    return Result::kExists;
  }

  if (Specialization::is_valid(pool_, item_offset)) {
    if (old_item_size >= new_item_size) {
      stat_inc(&Stats::set_inplace);
      finalize_item_value(item, value, (uint32_t)value_length, deleted);
      pool_->unlock();
      unlock_bucket(bucket, caller_id);
      return Result::kSuccess;
    }
  }

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
  set_new_item(new_item, key_hash, key, (uint32_t)key_length, value,
               (uint32_t)value_length, deleted);

  // unlocking is delayed until we finish writing data at the new location;
  // otherwise, the new location may be invalidated (in a very rare case)
  pool_->unlock();

  if (located_bucket->item_vec[item_index] != 0) {
    stat_dec(&Stats::count);
  }

  located_bucket->item_vec[item_index] = make_item_vec(tag, new_item_offset);

  unlock_bucket(bucket, caller_id);

  // XXX: this may be too late; before cleaning, other threads may have read
  // some invalid location
  //      ideally, this should be done before writing actual data
  cleanup_bucket(caller_id, new_item_offset, new_tail);

  // this is done after bucket is updated and unlocked
  pool_->release(item_offset);

  stat_inc(&Stats::count);
  return Result::kSuccess;
}

}  // namespace table
}  // namespace mica

#endif