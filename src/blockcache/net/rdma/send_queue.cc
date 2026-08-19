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

#include "blockcache/net/rdma/send_queue.h"

#include <glog/logging.h>

#include <cstring>

#include "blockcache/common/status.h"
#include "blockcache/net/infiniband/wc_status.h"

namespace dingofs {
namespace blockcache {

static_assert(alignof(OpAwaiter) > kWrTagMask,
              "wr_id steals the low bits of the awaiter address");
static_assert(alignof(BatchAwaiter) > kWrTagMask,
              "wr_id steals the low bits of the awaiter address");

Status OpAwaiter::await_resume() const {
  switch (result_) {
    case kNoSuchRegion:
      return ToStatus(EINVAL,
                      "post a one-sided operation: the peer advertised no "
                      "such region");
    case kRegionTooSmall:
      return ToStatus(EINVAL,
                      "post a one-sided operation: it exceeds the region "
                      "the peer advertised");
    default:
      break;
  }
  return verbs::WcStatus(static_cast<ibv_wc_status>(result_),
                         "complete rdma operation");
}

Status BatchAwaiter::await_resume() const {
  if (covered_ == 0) {
    return Status::OK();
  }
  return verbs::WcStatus(static_cast<ibv_wc_status>(result_),
                         "complete rdma batch");
}

void DoorbellList::FlushAll() {
  SendQueue* queue = head;
  head = nullptr;
  while (queue != nullptr) {
    SendQueue* next = queue->dirty_next_;
    queue->dirty_next_ = nullptr;
    queue->dirty_linked_ = false;
    queue->Flush();
    queue = next;
  }
}

SendQueue::SendQueue(ibv_qp* qp, DoorbellList* dirty, uint32_t limit,
                     uint32_t max_inline)
    : qp_(qp),
      dirty_(dirty),
      limit_(limit),
      max_inline_(max_inline),
      wrs_(limit),
      sges_(limit) {}

SendQueue::~SendQueue() {
  DCHECK(parked_.empty());
  DCHECK(read_parked_.empty());
}

void SendQueue::OnOpWc(OpAwaiter* op, int32_t wc_status) {
  ReleaseSlots(1, op->IsRead() ? 1 : 0);
  op->Complete(wc_status);
}

bool SendQueue::AddUnsignaled(ibv_wr_opcode opcode, verbs::LocalBuf local,
                              uint64_t remote_addr, uint32_t rkey,
                              ibv_send_wr** tail_out) {
  if (broken_ || inflight_ >= limit_) {
    return false;
  }
  const bool is_read = opcode == IBV_WR_RDMA_READ;
  if (is_read && reads_inflight_ >= read_limit_) {
    return false;
  }

  // wr_id 0 = no completion object; the poller must not dereference it.
  ibv_send_wr* wr =
      BuildWr(opcode, local, remote_addr, rkey, /*wr_id=*/0, /*flags=*/0);
  ++inflight_;
  if (is_read) {
    ++reads_inflight_;
  }
  Append(wr);
  *tail_out = wr;
  return true;
}

void SendQueue::SealBatch(BatchAwaiter* batch, ibv_send_wr* tail) {
  if (broken_ || tail == nullptr) {
    batch->Complete(IBV_WC_WR_FLUSH_ERR);
    return;
  }
  tail->send_flags |= IBV_SEND_SIGNALED;
  tail->wr_id = MakeWrId(batch, kTagBatchEnd);
  MarkDirty();
}

void SendQueue::OnBatchWc(BatchAwaiter* batch, int32_t wc_status) {
  ReleaseSlots(batch->covered_, batch->reads_covered_);
  batch->Complete(wc_status);
}

bool SendQueue::PostFrame(uint64_t wr_id, verbs::LocalBuf frame) {
  if (broken_ || inflight_ >= limit_) {
    return false;
  }
  ++inflight_;
  ibv_send_wr* wr = BuildWr(IBV_WR_SEND, frame, /*remote_addr=*/0, /*rkey=*/0,
                            wr_id, IBV_SEND_SIGNALED);
  // Latency sensitive: post now unless a chain is pending (program order).
  if (chain_head_ == nullptr) {
    PostNow(wr, wr);
  } else {
    Append(wr);
  }
  return true;
}

void SendQueue::OnFrameWc() { ReleaseSlots(1, 0); }

void SendQueue::Admit(OpAwaiter* op) {
  if (broken_) {
    op->Complete(IBV_WC_WR_FLUSH_ERR);
    return;
  }

  const bool is_read = op->IsRead();
  if (is_read) {
    if (reads_inflight_ >= read_limit_ || inflight_ >= limit_) {
      read_parked_.Push(op);
      return;
    }
    ++reads_inflight_;
  } else if (inflight_ >= limit_) {
    parked_.Push(op);
    return;
  }

  ++inflight_;
  Dispatch(op);
}

void SendQueue::Flush() {
  if (chain_head_ == nullptr) {
    return;
  }
  ibv_send_wr* head = chain_head_;
  ibv_send_wr* tail = chain_tail_;
  chain_head_ = nullptr;
  chain_tail_ = nullptr;
  PostNow(head, tail);
}

void SendQueue::SetBroken() {
  broken_ = true;

  // Never-posted requests get no flush completion; fail them so drains end.
  ibv_send_wr* pending = chain_head_;
  chain_head_ = nullptr;
  chain_tail_ = nullptr;
  if (pending != nullptr) {
    FailUnposted(pending, IBV_WC_WR_FLUSH_ERR);
  }
  FailParked(IBV_WC_WR_FLUSH_ERR);
}

ibv_send_wr* SendQueue::BuildWr(ibv_wr_opcode opcode,
                                const verbs::LocalBuf& local,
                                uint64_t remote_addr, uint32_t rkey,
                                uint64_t wr_id, unsigned flags) {
  const uint32_t wr_index = ring_pos_;
  ring_pos_ = ring_pos_ + 1 == limit_ ? 0 : ring_pos_ + 1;

  ibv_sge* sge = &sges_[wr_index];
  sge->addr = reinterpret_cast<uint64_t>(local.addr);
  sge->length = local.len;
  sge->lkey = local.lkey;

  ibv_send_wr* wr = &wrs_[wr_index];
  std::memset(wr, 0, sizeof(*wr));
  wr->wr_id = wr_id;
  wr->sg_list = sge;
  wr->num_sge = local.len == 0 ? 0 : 1;
  wr->opcode = opcode;
  wr->send_flags = flags;
  // Inline spares the HCA a DMA fetch; SEND/WRITE only, below the threshold.
  if ((opcode == IBV_WR_SEND || opcode == IBV_WR_RDMA_WRITE) &&
      local.len <= max_inline_) {
    wr->send_flags |= IBV_SEND_INLINE;
  }
  if (opcode == IBV_WR_RDMA_WRITE || opcode == IBV_WR_RDMA_READ) {
    wr->wr.rdma.remote_addr = remote_addr;
    wr->wr.rdma.rkey = rkey;
  }
  return wr;
}

void SendQueue::Dispatch(OpAwaiter* op) {
  ibv_send_wr* wr = BuildWr(op->opcode_, op->local_, op->remote_addr_,
                            op->rkey_, MakeWrId(op, kTagOp), IBV_SEND_SIGNALED);

  // No chain pending: post immediately, a depth-1 request must not wait.
  if (chain_head_ == nullptr) {
    PostNow(wr, wr);
    return;
  }
  Append(wr);
}

void SendQueue::Append(ibv_send_wr* wr) {
  wr->next = nullptr;
  if (chain_tail_ != nullptr) {
    chain_tail_->next = wr;
  } else {
    chain_head_ = wr;
  }
  chain_tail_ = wr;
  MarkDirty();
}

void SendQueue::PostNow(ibv_send_wr* head, ibv_send_wr* tail) {
  tail->next = nullptr;
  ibv_send_wr* bad = nullptr;
  int rc = ibv_post_send(qp_, head, &bad);
  if (rc == 0) {
    return;
  }

  // Not back-pressure: nothing here reached the device; fail + reclaim now.
  LOG(ERROR) << "Fail to post send work request: " << std::strerror(rc);
  broken_ = true;
  FailUnposted(head, IBV_WC_GENERAL_ERR);
  FailParked(IBV_WC_WR_FLUSH_ERR);
}

void SendQueue::MarkDirty() {
  if (dirty_linked_ || dirty_ == nullptr) {
    return;
  }
  dirty_linked_ = true;
  dirty_next_ = dirty_->head;
  dirty_->head = this;
}

void SendQueue::ReleaseSlots(uint32_t n, uint32_t reads) {
  if (n > inflight_) {
    LOG(DFATAL) << "Release more send slots than are in flight: n=" << n
                << " inflight=" << inflight_ << " limit=" << limit_
                << "; clamping, but the accounting is already wrong";
    n = inflight_;
  }
  inflight_ -= n;
  reads_inflight_ -= reads < reads_inflight_ ? reads : reads_inflight_;

  // A broken queue must not re-dispatch; FailParked handles parked ones.
  if (broken_) {
    return;
  }

  // Reads first: their window is tighter, a freed slot is worth more.
  while (!read_parked_.empty() && reads_inflight_ < read_limit_ &&
         inflight_ < limit_) {
    ++inflight_;
    ++reads_inflight_;
    Dispatch(read_parked_.Pop());
  }
  while (!parked_.empty() && inflight_ < limit_) {
    ++inflight_;
    Dispatch(parked_.Pop());
  }

  // Nothing in flight means nothing will ever call this again, so a parked op
  // here waits forever. It is the shape every silent hang has taken.
  LOG_IF(DFATAL, inflight_ == 0 && parked() != 0)
      << "Send queue stalled with nothing in flight: parked=" << parked()
      << " read_parked=" << read_parked_.size() << " limit=" << limit_
      << " read_limit=" << read_limit_;
}

void SendQueue::FailUnposted(ibv_send_wr* wr, int32_t wc_status) {
  uint32_t reclaimed = 0;
  uint32_t reads = 0;
  while (wr != nullptr) {
    ibv_send_wr* next = wr->next;
    ++reclaimed;
    if (wr->opcode == IBV_WR_RDMA_READ) {
      ++reads;
    }
    if (wr->wr_id != 0) {
      void* owner = WrIdPtr(wr->wr_id);
      if (WrIdTag(wr->wr_id) == kTagOp) {
        static_cast<OpAwaiter*>(owner)->Complete(wc_status);
      } else {
        static_cast<BatchAwaiter*>(owner)->Complete(wc_status);
      }
    }
    wr = next;
  }
  ReleaseSlots(reclaimed, reads);
}

void SendQueue::FailParked(int32_t wc_status) {
  const auto fail = [wc_status](OpAwaiter* op) { op->Complete(wc_status); };
  read_parked_.TakeAllAnd(fail);
  parked_.TakeAllAnd(fail);
}

}  // namespace blockcache
}  // namespace dingofs
