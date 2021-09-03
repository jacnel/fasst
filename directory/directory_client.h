#ifndef DIRECTORY_CLIENT_H
#define DIRECTORY_CLIENT_H

#include <pthread.h>
#include <stdio.h>
#include <stdexcept>
#include <unordered_map>

#include "directory/directory_defs.h"
#include "hots.h"
#include "mappings/mappings.h"
#include "rpc/rpc.h"
#include "tx/tx_defs.h"
#include "libhrd/hrd.h"

// For now, we assume that each worker thread will get its own DirectoryClient
// and so we do not need to worry about concurrency w.r.t the verbs resources.
// But, we might also assume that the cache is shared among all workers, since
// MICA is already concurrent.
class DirectoryClient {
 private:
  volatile directory_entry_t *entry_;
  struct hrd_ctrl_blk *cb;
  struct dir_args_t info;
  struct hrd_qp_attr **qp_attrs;
  int wr_id = 0;

  Mappings *mappings_;

 public:
  enum class Result { kSuccess = 0, kOwned, kError };

  DirectoryClient(struct dir_args_t args, Mappings *mappings)
      : info(args), mappings_(mappings) {
    cb = hrd_ctrl_blk_init(info.machine_id,    /* local hid */
                           info.port_index, 0, /* port index, numa node */
                           info.num_dirs, 0,   /* conn qps, UC */
                           NULL,               /* conn prealloc buf */
                           sizeof(directory_entry_t), /* buf size */
                           DIR_CLIENT_SHM_KEY,        /* conn buf shm key */
                           NULL, 0, /* dgram prealloc buf, dgram qps */
                           0,       /* buf size */
                           -1);     /* dgram buf shm key */

    entry_ = reinterpret_cast<volatile directory_entry_t *>(cb->conn_buf);

    for (int i = 0; i < info.num_dirs; ++i) {
      char name[HRD_QP_NAME_SIZE];
      sprintf(name, "for-directory-%d-from-client-%d", i,
              mappings_->machine_id);
      hrd_publish_conn_qp(cb, i, name);
      hrd_publish_ready(name);
    }
  }

  void connect() {
    qp_attrs = new hrd_qp_attr *[info.num_dirs];
    for (int i = 0; i < info.num_dirs; ++i) {
      char name[HRD_QP_NAME_SIZE];
      sprintf(name, "for-client-%d-from-directory-%d", mappings_->machine_id,
              i);
      hrd_wait_till_ready(name);
      qp_attrs[i] = hrd_get_published_qp(name);
      assert(qp_attrs[i] != nullptr);
      hrd_connect_qp(cb, i, qp_attrs[i]);
    }
  }

  Result lookup(hots_key_t key, directory_entry_t *entry_out) {
    uint64_t keyhash = dir_keyhash(key);
    uint32_t directory_id = mappings_->get_directory_mn(keyhash);

    struct ibv_sge sge;
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_wc wc;
    uint64_t remote_addr =
        qp_attrs[directory_id]->buf_addr +
        (mappings_->get_directory_offset(keyhash) % info.num_entries);
    uint32_t rkey = qp_attrs[directory_id]->rkey;
    struct ibv_qp *qp = cb->conn_qp[directory_id];
    struct ibv_cq *cq = cb->conn_cq[directory_id];

    memset(&sge, 0, sizeof(sge));
    sge.addr = reinterpret_cast<uint64_t>(cb->conn_buf);
    sge.length = cb->conn_buf_size;
    sge.lkey = cb->conn_buf_mr->lkey;

    memset(&send_wr, 0, sizeof(send_wr));
    send_wr.opcode = IBV_WR_RDMA_READ;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.num_sge = 1;
    send_wr.sg_list = &sge;
    send_wr.wr.rdma.remote_addr = remote_addr;
    send_wr.wr.rdma.rkey = rkey;
    send_wr.wr_id = wr_id++;

    int ret = ibv_post_send(qp, &send_wr, &bad_wr);
    dir_dassert(ret == 0);
    hrd_poll_cq(cq, 1, &wc);

    if (is_owned(const_cast<directory_entry_t *>(entry_)) &&
        !is_owner(const_cast<directory_entry_t *>(entry_),
                  mappings_->machine_id)) {
      return Result::kOwned;
    }

    do {
      // Copy current `entry`, which contains the result of remote read or the
      // previous value when doing a compare-and-swap. Then, add this machine as
      // an accessor.
      *entry_out = *(const_cast<directory_entry_t *>(entry_));
      add_accessor(entry_out, mappings_->machine_id);

      send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
      send_wr.wr.atomic.remote_addr = remote_addr;
      send_wr.wr.atomic.rkey = rkey;
      send_wr.wr.atomic.compare_add = entry_->owner_accessors;
      send_wr.wr.atomic.swap = entry_out->owner_accessors;

      ret = ibv_post_send(qp, &send_wr, &bad_wr);
      dir_dassert(ret == 0);
      hrd_poll_cq(cq, 1, &wc);
      dir_dassert(wc.status == IBV_WC_SUCCESS);

      // Repeat the loop until the CAS is successful, or the entry is owned.
    } while (entry_->owner_accessors != send_wr.wr.atomic.compare_add &&
             !is_owned(const_cast<directory_entry_t *>(entry_)));

    if (!is_owner(const_cast<directory_entry_t *>(entry_),
                  mappings_->machine_id)) {
      return Result::kOwned;
    }

    // Current machine must be in accessor set because while loop ended and
    // entry is not owned by anyone, or is the owner which means another thread
    // is updating the value.
    return Result::kSuccess;
  }

  Result acquire(hots_key_t key, directory_entry_t *entry_out) {
    uint64_t keyhash = dir_keyhash(key);
    uint32_t directory_id = mappings_->get_directory_mn(keyhash);
    uint64_t remote_addr = qp_attrs[directory_id]->buf_addr +
                           mappings_->get_directory_offset(keyhash);
    uint32_t rkey = qp_attrs[directory_id]->rkey;

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = reinterpret_cast<uint64_t>(cb->conn_buf);
    sge.length = cb->conn_buf_size;
    sge.lkey = cb->conn_buf_mr->lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(send_wr));
    send_wr.opcode = IBV_WR_RDMA_READ;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.num_sge = 1;
    send_wr.sg_list = &sge;
    send_wr.wr.rdma.remote_addr = remote_addr;
    send_wr.wr.rdma.rkey = rkey;
    send_wr.wr_id = wr_id++;

    struct ibv_send_wr *bad_wr;
    struct ibv_qp *qp = cb->conn_qp[directory_id];
    int ret = ibv_post_send(qp, &send_wr, &bad_wr);
    dir_dassert(ret == 0);

    struct ibv_wc wc;
    struct ibv_cq *cq = cb->conn_cq[directory_id];
    hrd_poll_cq(cq, 1, &wc);

    if (is_owned(const_cast<directory_entry_t *>(entry_))) {
      if (is_owner(const_cast<directory_entry_t *>(entry_),
                   mappings_->machine_id))
        return Result::kSuccess;
      else
        return Result::kOwned;
    }

    // `entry` contains the result of the remote read
    do {
      *entry_out = *(const_cast<directory_entry_t *>(entry_));
      set_owned(entry_out, mappings_->machine_id);

      send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
      send_wr.wr.atomic.remote_addr = remote_addr;
      send_wr.wr.atomic.rkey = rkey;
      send_wr.wr.atomic.compare_add = entry_->owner_accessors;
      send_wr.wr.atomic.swap = entry_out->owner_accessors;

      ret = ibv_post_send(qp, &send_wr, &bad_wr);
      dir_dassert(ret == 0);
      hrd_poll_cq(cq, 1, &wc);
      dir_dassert(wc.status == IBV_WC_SUCCESS);
    } while (entry_->owner_accessors != send_wr.wr.atomic.compare_add &&
             !is_owned(const_cast<directory_entry_t *>(entry_)));

    if (!is_owner(const_cast<directory_entry_t *>(entry_),
                  mappings_->machine_id)) {
      return Result::kOwned;  // Owned, but not by us.
    }
    return Result::kSuccess;
  }

  //! Must only be called after a successful acquire() call.
  Result release(hots_key_t key, directory_entry_t new_entry) {
    uint64_t keyhash = dir_keyhash(key);
    uint32_t directory_id = mappings_->get_directory_mn(keyhash);
    uint64_t remote_addr = qp_attrs[directory_id]->buf_addr +
                           mappings_->get_directory_offset(keyhash);
    uint32_t rkey = qp_attrs[directory_id]->rkey;

    entry_->owner_accessors = new_entry.owner_accessors;
    entry_->primary = new_entry.primary;

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = reinterpret_cast<uint64_t>(cb->conn_buf);
    sge.length = cb->conn_buf_size;
    sge.lkey = cb->conn_buf_mr->lkey;

    struct ibv_send_wr send_wr;
    memset(&send_wr, 0, sizeof(send_wr));
    send_wr.opcode = IBV_WR_RDMA_WRITE;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.num_sge = 1;
    send_wr.sg_list = &sge;
    send_wr.wr.rdma.remote_addr = remote_addr;
    send_wr.wr.rdma.rkey = rkey;
    send_wr.wr_id = wr_id++;

    struct ibv_send_wr *bad_wr;
    struct ibv_qp *qp = cb->conn_qp[directory_id];
    int ret = ibv_post_send(qp, &send_wr, &bad_wr);
    dir_dassert(ret == 0);

    struct ibv_wc wc;
    struct ibv_cq *cq = cb->conn_cq[directory_id];
    hrd_poll_cq(cq, 1, &wc);

    return Result::kSuccess;
  }
};

#endif
