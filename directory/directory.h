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

    for (int i = 0; i < info.num_clients; ++i) {
      char name[HRD_QP_NAME_SIZE];
      sprintf(name, "for-client-%d-from-directory-%d", i, info.machine_id);
      hrd_publish_conn_qp(cb, i, name);
      hrd_publish_ready(name);
    }
  }

  void connect() {
    hrd_qp_attr** qp_attrs = new hrd_qp_attr*[info.num_clients];
    for (int i = 0; i < info.num_clients; ++i) {
      char name[HRD_QP_NAME_SIZE];
      sprintf(name, "for-directory-%d-from-client-%d", info.machine_id, i);
      hrd_wait_till_ready(name);
      qp_attrs[i] = hrd_get_published_qp(name);
      assert(qp_attrs[i] != nullptr);
      hrd_connect_qp(cb, i, qp_attrs[i]);
    }
  }
};

#endif
