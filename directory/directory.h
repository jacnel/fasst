#ifndef DIRECTORY_H
#define DIRECTORY_H

#include <pthread.h>
#include <stdio.h>
#include <stdexcept>
#include <unordered_map>

#include "directory/directory_defs.h"
#include "hots.h"
#include "rpc/rpc.h"
#include "tx/tx_defs.h"
#include "libhrd/hrd.h"

// A Directory is a passive object that defines a remotely accessible memory
// region to be interacted with solely through the client. Each hots_key_t
// should map to exactly one directory entry, and one directory entry to a key.
// This requires an entry for every possible key, which may not always be ideal.
class Directory {
 private:
  directory_entry_t* entries;
  struct hrd_ctrl_blk* cb;
  struct dir_args_t info;

 public:
  Directory(struct dir_args_t args) : info(args) {
    cb = hrd_ctrl_blk_init(info.machine_id,     /* local hid */
                           info.port_index, 0,  /* port index, numa node */
                           info.num_clients, 0, /* conn qps, UC */
                           NULL,                /* conn prealloc buf */
                           directory_size(args.num_entries), /* buf size */
                           DIRECTORY_SHM_KEY, /* conn buf shm key */
                           NULL, 0, /* dgram prealloc buf, dgram qps */
                           0,       /* buf size */
                           -1);     /* dgram buf shm key */
  }
};

#endif
