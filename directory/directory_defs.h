#include "hots.h"
#include "rpc/rpc.h"
#include "tx/tx_defs.h"
#include "libhrd/hrd.h"

#ifndef DIRECTORY_DEFS_H
#define DIRECTORY_DEFS_H

// General datastore defines
#define dir_dassert(x)               \
  do {                               \
    if (DIR_DEBUG_ASSERT) assert(x); \
  } while (0)
#define dir_dassert_msg(x, msg)                    \
  do {                                             \
    if (DIR_DEBUG_ASSERT) HOTS_ASSERT_MSG(x, msg); \
  } while (0)

#define NUM_LOCKS 262144

#define DIRECTORY_SQ_DEPTH 16
#define DIRECTORY_MAX_ENTRIES (1ull << 48)
#define DIRECTORY_MAX_DIRS (1ul << 8)

struct dir_args_t {
  int machine_id;
  int num_clients;
  int num_dirs;
  int num_entries;
  int port_index;
};

enum class directory_reqtype_t : uint16_t { invalidate = 7, acquire = 8 };

enum class directory_resptype_t : uint16_t {
  success = 3,
  fail = 4,
};

// #define dir_hashmask ((1ull << 48) - 1)
static uint64_t dir_keyhash(hots_key_t key) {
  // return ::mica::util::hash((char*)&key, sizeof(hots_key_t)) & dir_hashmask;
  return ::mica::util::hash((char*)&key, sizeof(hots_key_t));
}

struct directory_req_t {
  directory_reqtype_t req_type;
  uint64_t keyhash;
  uint32_t requester_id;
  hots_key_t key;
};

//* `owner_accessors` encodes either the owner (if the most significant bit is
//* set) or the set of accessors for the given entry. It is updated exclusively
//* with a remote CAS to ensure consistent access.
//
//+ owner_accessors should encode a bloom filter to handle larger cluster sizes
struct directory_entry_t {
  uint64_t owner_accessors;
  uint32_t primary;
  uint32_t backups[HOTS_MAX_BACKUPS];
};

// #define directory_size(num_entries) 
//   (static_cast<uint64_t>(sizeof(directory_entry_t)) * num_entries)

forceinline uint64_t directory_size(uint64_t num_entries) {
  return static_cast<uint64_t>(sizeof(directory_entry_t)) * num_entries;
}

forceinline bool is_owned(directory_entry_t* entry) {
  return entry->owner_accessors >> 63;
}

// A machine is the owner of an entry if the owner bit is set, and the machine
// id matches the bit set in the lower 63-bits of `owner_accessors`.
forceinline bool is_owner(directory_entry_t* entry, uint32_t machine_id) {
  dir_dassert(machine_id < 63);
  return (entry->owner_accessors >> 63) &&
         (entry->owner_accessors & (1 << machine_id));
}

forceinline void set_owned(directory_entry_t* entry, uint32_t machine_id) {
  dir_dassert(machine_id < 63);
  entry->owner_accessors = 0;
  entry->owner_accessors = (1UL << 63) & (1 << machine_id);
}

// A machine is an accessor if the owner bit is not set and the machine id is
// encoded in the lower 63-bits of the entry's `owner_accessors` field.
forceinline bool is_accessor(directory_entry_t* entry, uint32_t machine_id) {
  dir_dassert(machine_id < 63);
  return !(entry->owner_accessors >> 63) &&
         (entry->owner_accessors & (1 << machine_id));
}

forceinline void add_accessor(directory_entry_t* entry, uint32_t machine_id) {
  dir_dassert(machine_id < 63);
  dir_dassert(!(entry->owner_accessors >> 63));
  entry->owner_accessors |= (1 << machine_id);
}

forceinline std::vector<uint32_t> get_accessors(directory_entry_t* entry) {
  std::vector<uint32_t> accessors;
  for (int i = 0; i < 63; ++i) {
    if (entry->owner_accessors & (1 << i)) {
      accessors.push_back(i);
    }
  }
  return accessors;
}

static const struct directory_entry_t EmptyEntry = {UINT64_MAX, 0};

#endif /* DIRECTORY_DEFS_H */