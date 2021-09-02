#pragma once
#ifndef MICA_TABLE_CTABLE_IMPL_LOCK_H_
#define MICA_TABLE_CTABLE_IMPL_LOCK_H_

namespace mica {
namespace table {

template <class StaticConfig>
bool CTable<StaticConfig>::try_lock_bucket(Bucket* bucket, uint32_t locker_id) {
  uint64_t v = *(volatile uint64_t*)&bucket->version & ~1U;
  if ((v & 1U) && bucket->locker_id == locker_id) return true;
  uint64_t new_v = v | 1U;
  if (__sync_bool_compare_and_swap((volatile uint64_t*)&bucket->version, v,
                                   new_v)) {
    assert(bucket->locker_id == Bucket::kInvalidLockerID);
    bucket->locker_id = locker_id;
    return true;
  }
  return false;
}

template <class StaticConfig>
void CTable<StaticConfig>::lock_bucket(Bucket* bucket, uint32_t locker_id) {
  while (true) {
    uint64_t v = *(volatile uint64_t*)&bucket->version & ~1U;
    if ((v & 1U) && bucket->locker_id == locker_id) break;
    uint64_t new_v = v | 1U;
    if (__sync_bool_compare_and_swap((volatile uint64_t*)&bucket->version, v,
                                     new_v)) {
      assert(bucket->locker_id == Bucket::kInvalidLockerID);
      bucket->locker_id = locker_id;
      break;
    }
  }
}

template <class StaticConfig>
void CTable<StaticConfig>::unlock_bucket(Bucket* bucket, uint32_t locker_id) {
  assert(bucket->locker_id == locker_id);
  bucket->locker_id = Bucket::kInvalidLockerID;
  ::mica::util::memory_barrier();
  assert((*(volatile uint64_t*)&bucket->version & 1U) == 1U);
  // no need to use atomic add because this thread is the only one writing
  // to version
  (*(volatile uint64_t*)&bucket->version)++;
}

template <class StaticConfig>
bool CTable<StaticConfig>::is_locked(Bucket* bucket, uint32_t* out_locker_id) {
  *out_locker_id = *(volatile uint32_t*)&bucket->locker_id;
  return (*(volatile uint64_t*)&bucket->version & 1U) == 1U;
}

template <class StaticConfig>
uint64_t CTable<StaticConfig>::read_version_begin(const Bucket* bucket) const {
  while (true) {
    uint64_t v = *(volatile uint64_t*)&bucket->version;
    ::mica::util::memory_barrier();
    if ((v & 1U) != 0U) continue;
    return v;
  }
}

template <class StaticConfig>
uint64_t CTable<StaticConfig>::read_version_end(const Bucket* bucket) const {
  ::mica::util::memory_barrier();
  uint64_t v = *(volatile uint64_t*)&bucket->version;
  return v;
}

template <class StaticConfig>
uint64_t CTable<StaticConfig>::get_version(const Bucket* bucket) const {
  uint64_t v = *(volatile uint64_t*)&bucket->version;
  ::mica::util::memory_barrier();
  return (v & ~1U);
}

template <class StaticConfig>
uint64_t CTable<StaticConfig>::get_next_version(const Bucket* bucket) const {
  uint64_t v = *(volatile uint64_t*)&bucket->version;
  ::mica::util::memory_barrier();
  return ((v + 1) & ~1U);
}

}  // namespace table
}  // namespace mica

#endif