#ifndef MICA_TABLE_CTABLE_IMPL_ABORT_WRITE_H_
#define MICA_TABLE_CTABLE_IMPL_ABORT_WRITE_H_

namespace mica {
namespace table {

template <class StaticConfig>
Result CTable<StaticConfig>::abort_write(uint32_t caller_id,
                                         uint64_t key_hash) {
  uint32_t bucket_index = calc_bucket_index(key_hash);

  Bucket* bucket = buckets_ + bucket_index;

  uint32_t locker_id;
  assert(is_locked(bucket, &locker_id));

  unlock_bucket(bucket, caller_id);
  return Result::kSuccess;
}

}  // namespace table
}  // namespace mica

#endif