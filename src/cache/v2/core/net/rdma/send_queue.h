/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_SEND_QUEUE_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_SEND_QUEUE_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <vector>

#include "cache/v2/core/net/rdma/op_awaiter.h"
#include "cache/v2/core/net/rdma/verbs/buffer.h"
#include "cache/v2/core/net/rdma/wr_id.h"
#include "cache/v2/utils/containers/park_queue.h"

namespace dingofs {
namespace cache {
namespace v2 {

class SendQueue;

// Queues holding an unposted chain; drained once per poll, one doorbell.
struct DoorbellList {
  SendQueue* head = nullptr;
  void FlushAll();
};

// Send side of one QP: bounded depth, separate READ window, per-op signal.
class SendQueue {
 public:
  // Applied once the handshake settles on an rd_atomic value.
  // Window = a multiple of rd_atomic: gating exactly at it starves the queue.
  static constexpr uint32_t kReadOversubscribe = 8;

  SendQueue(ibv_qp* qp, DoorbellList* dirty, uint32_t limit,
            uint32_t max_inline);
  ~SendQueue();

  SendQueue(const SendQueue&) = delete;
  SendQueue& operator=(const SendQueue&) = delete;

  // Submit now, or park until a slot frees.
  void Admit(OpAwaiter* op);
  // Called by the CQ poller when `op`'s completion arrives.
  void OnOpWc(OpAwaiter* op, int32_t wc_status);

  // Appends an unsignalled request; false when full -- submit, start anew.
  bool AddUnsignaled(ibv_wr_opcode opcode, verbs::LocalBuf local,
                     uint64_t remote_addr, uint32_t rkey,
                     ibv_send_wr** tail_out);
  // Turns `tail` into the batch's completion carrier.
  void SealBatch(BatchAwaiter* batch, ibv_send_wr* tail);
  void OnBatchWc(BatchAwaiter* batch, int32_t wc_status);

  // Posts an RPC frame as a SEND, always signalled; false when full.
  bool PostFrame(uint64_t wr_id, verbs::LocalBuf frame);
  void OnFrameWc();

  // Posts the accumulated chain, if any.
  void Flush();

  // Fails parked ops and reclaims never-posted slots so drains terminate.
  void SetBroken();

  // Never below one: a zero window parks every read with nothing to free it.
  void set_read_limit(uint32_t rd_atomic) {
    const uint32_t window = rd_atomic * kReadOversubscribe;
    const uint32_t capped = window > limit_ ? limit_ : window;
    read_limit_ = capped == 0 ? 1 : capped;
  }
  uint32_t inflight() const { return inflight_; }
  uint32_t reads_inflight() const { return reads_inflight_; }
  uint32_t read_limit() const { return read_limit_; }
  uint32_t parked() const { return parked_.size() + read_parked_.size(); }

 private:
  friend struct DoorbellList;

  ibv_send_wr* BuildWr(ibv_wr_opcode opcode, const verbs::LocalBuf& local,
                       uint64_t remote_addr, uint32_t rkey, uint64_t wr_id,
                       unsigned flags);
  void Dispatch(OpAwaiter* op);
  void Append(ibv_send_wr* wr);
  void PostNow(ibv_send_wr* head, ibv_send_wr* tail);
  void MarkDirty();
  void ReleaseSlots(uint32_t n, uint32_t reads);
  // Requests that never reached the device: complete their owners and give
  // the slots back, since no work completion will ever arrive for them.
  void FailUnposted(ibv_send_wr* wr, int32_t wc_status);
  void FailParked(int32_t wc_status);

  ibv_qp* qp_;
  DoorbellList* dirty_;
  uint32_t limit_;
  uint32_t max_inline_;

  // Scratch: ibv_post_send copies before returning; reusable once posted.
  std::vector<ibv_send_wr> wrs_;
  std::vector<ibv_sge> sges_;
  uint32_t ring_pos_ = 0;

  ibv_send_wr* chain_head_ = nullptr;
  ibv_send_wr* chain_tail_ = nullptr;

  // Reads park separately: gated by both depth and the rd_atomic window.
  ParkQueue<OpAwaiter> parked_;
  ParkQueue<OpAwaiter> read_parked_;

  uint32_t inflight_ = 0;
  uint32_t reads_inflight_ = 0;
  uint32_t read_limit_ = 1;  // verbs default until the handshake raises it
  bool broken_ = false;

  bool dirty_linked_ = false;
  SendQueue* dirty_next_ = nullptr;
};

// Builds a chain sharing one completion; Add* false = submit and start anew.
class OpBatch {
 public:
  explicit OpBatch(SendQueue* queue) : queue_(queue) {}

  bool AddWrite(verbs::LocalBuf local, const verbs::RemoteBuf& remote) {
    return Add(IBV_WR_RDMA_WRITE, local, remote);
  }
  bool AddRead(verbs::LocalBuf local, const verbs::RemoteBuf& remote) {
    return Add(IBV_WR_RDMA_READ, local, remote);
  }

  BatchAwaiter Submit() { return BatchAwaiter(queue_, tail_, count_, reads_); }

  uint32_t count() const { return count_; }

 private:
  bool Add(ibv_wr_opcode opcode, const verbs::LocalBuf& local,
           const verbs::RemoteBuf& remote) {
    ibv_send_wr* tail = nullptr;
    if (!queue_->AddUnsignaled(opcode, local, remote.addr, remote.rkey,
                               &tail)) {
      return false;
    }
    tail_ = tail;
    ++count_;
    if (opcode == IBV_WR_RDMA_READ) {
      ++reads_;
    }
    return true;
  }

  SendQueue* queue_;
  ibv_send_wr* tail_ = nullptr;
  uint32_t count_ = 0;
  uint32_t reads_ = 0;
};

inline void OpAwaiter::Arm() { queue_->Admit(this); }

inline void BatchAwaiter::Arm() { queue_->SealBatch(this, tail_); }

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_SEND_QUEUE_H_
