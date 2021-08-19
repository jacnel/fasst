#pragma once
#ifndef MICA_TABLE_CTABLE_IMPL_PREPARE_H_
#define MICA_TABLE_CTABLE_IMPL_PREPARE_H_

namespace mica {
namespace table {

// Invalidates all keys in the same bucket as keyhash
template <class StaticConfig>
Result CTable<StaticConfig>::prepare(uint64_t keyhash,
                                     uint32_t* out_incarnation) {
  // Obtain the bucket containing the key and lock it since we are modifying
  // it.
  uint64_t bucket_index = calc_bucket_index(keyhash);
  Bucket* bucket = buckets_ + bucket_index;
  *out_incarnation = *(const_cast<volatile uint32_t*>(&bucket->incarnation));
  return Result::kSuccess;
}

}  // namespace table
}  // namespace mica

#endif