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

#include "blockcache/infiniband/connection/send_queue.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>

#include "blockcache/infiniband/base/queue_pair.h"
#include "blockcache/infiniband/common/wr_id.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

SendQueue::SendQueue(QueuePair* queue_pair, uint32_t depth)
    : qp_(queue_pair->get()),
      max_inline_data_(queue_pair->max_inline_data()),
      max_total_inflights_(depth) {
  const uint32_t max_read_inflights =
      QueuePair::kMaxRdAtomic * kReadOversubscriptionFactor;
  max_read_inflights_ = std::min(max_read_inflights, max_total_inflights_);
}

SendQueue::~SendQueue() {
  DCHECK(!running_);
  DCHECK_EQ(total_inflights_, 0);
  DCHECK_EQ(pending_wr_head_, nullptr);
  DCHECK(!has_waiters());
}

void SendQueue::Start() {
  LOG(INFO) << "SendQueue{qpn=" << qp_->qp_num << "} is starting...";

  running_ = true;

  LOG(INFO) << "Successfully start SendQueue{qpn=" << qp_->qp_num
            << " max_total_inflights=" << max_total_inflights_
            << " max_read_inflights=" << max_read_inflights_ << "}";
}

void SendQueue::Shutdown() {
  if (!running_) {
    return;
  }

  LOG(INFO) << "SendQueue{qpn=" << qp_->qp_num << "} is shutting down...";

  running_ = false;
  HandleAllFailure(TakeAll(), IBV_WC_WR_FLUSH_ERR);
  FailWaiters(IBV_WC_WR_FLUSH_ERR);

  LOG(INFO) << "Successfully shutdown SendQueue{qpn=" << qp_->qp_num << " "
            << "total_inflights=" << total_inflights_ << "/"
            << max_total_inflights_ << " read_inflights=" << read_inflights_
            << "/" << max_read_inflights_ << "}";
}

void SendQueue::Submit(OpAwaiter* op) {
  if (!running_) {
    op->Abort(IBV_WC_WR_FLUSH_ERR);
    return;
  }

  if (!TryAcquire(op->is_read())) {
    (op->is_read() ? read_waiters_ : waiters_).Push(op);
    return;
  }

  ibv_send_wr* wr = op->wr();
  ibv_send_wr* first_unposted_wr = PostOrEnqueue(wr);
  if (first_unposted_wr != nullptr) {
    HandlePostFailure(wr, first_unposted_wr);
  }
}

bool SendQueue::TrySubmit(ibv_send_wr* wr) {
  if (!running_ || !TryAcquire(/*is_read=*/false)) {
    return false;
  }

  // Never park a message WR: MsgSender::Send builds it on its own stack, so
  // the list node would dangle the moment Send returns.
  CHECK(pending_wr_head_ == nullptr)
      << "TrySubmit on a queue that is already batching";

  ibv_send_wr* first_unposted_wr = PostOrEnqueue(wr);
  if (first_unposted_wr != nullptr) {
    Countdown(1, 0);
    Shutdown();
    return false;
  }
  return true;
}

void SendQueue::SubmitBatch(BatchAwaiter* batch) {
  if (!running_) {
    batch->Abort(IBV_WC_WR_FLUSH_ERR);
    return;
  }

  Acquire(batch->num_wrs(), batch->num_read_wrs());
  Enqueue(batch->first_wr(), batch->last_wr());
}

uint32_t SendQueue::AvailableWrs(bool is_read) const {
  if (!running_ || total_inflights_ >= max_total_inflights_) {
    return 0;
  }

  const uint32_t available = max_total_inflights_ - total_inflights_;
  if (!is_read) {
    return available;
  }

  if (read_inflights_ >= max_read_inflights_) {
    return 0;
  }
  return std::min(max_read_inflights_ - read_inflights_, available);
}

void SendQueue::Release(uint32_t completed_wrs, uint32_t completed_read_wrs) {
  Countdown(completed_wrs, completed_read_wrs);
  if (!running_) {
    return;
  }

  while (OpAwaiter* op = TryAdmitWaiter()) {
    ibv_send_wr* wr = op->wr();
    if (ibv_send_wr* first_unposted_wr = PostOrEnqueue(wr)) {
      HandlePostFailure(wr, first_unposted_wr);
      return;
    }
  }

  LOG_IF(FATAL, total_inflights_ == 0 && has_waiters())
      << "Send queue stalled with nothing inflight: qpn=" << qp_->qp_num
      << " waiters=" << waiters_.size() << "/" << read_waiters_.size();
}

bool SendQueue::TryAcquire(bool is_read) {
  if (AvailableWrs(is_read) == 0) {
    return false;
  }

  Acquire(1, is_read ? 1 : 0);
  return true;
}

void SendQueue::Acquire(uint32_t num_wrs, uint32_t num_read_wrs) {
  DCHECK_LE(total_inflights_ + num_wrs, max_total_inflights_);
  DCHECK_LE(read_inflights_ + num_read_wrs, max_read_inflights_);
  total_inflights_ += num_wrs;
  read_inflights_ += num_read_wrs;
}

void SendQueue::Countdown(uint32_t num_wrs, uint32_t num_read_wrs) {
  CHECK_LE(num_wrs, total_inflights_);
  CHECK_LE(num_read_wrs, read_inflights_);

  total_inflights_ -= num_wrs;
  read_inflights_ -= num_read_wrs;
}

OpAwaiter* SendQueue::TryAdmitWaiter() {
  if (!read_waiters_.empty() && TryAcquire(/*is_read=*/true)) {
    return read_waiters_.Pop();
  }

  if (!waiters_.empty() && TryAcquire(/*is_read=*/false)) {
    return waiters_.Pop();
  }

  return nullptr;
}

ibv_send_wr* SendQueue::PostOrEnqueue(ibv_send_wr* wr) {
  if (pending_wr_head_ == nullptr) {
    wr->next = nullptr;
    return PostWorkRequests(wr);
  }

  Enqueue(wr, wr);
  return nullptr;
}

void SendQueue::Enqueue(ibv_send_wr* first_wr, ibv_send_wr* last_wr) {
  last_wr->next = nullptr;
  if (pending_wr_tail_ != nullptr) {
    pending_wr_tail_->next = first_wr;
  } else {
    pending_wr_head_ = first_wr;
  }
  pending_wr_tail_ = last_wr;
  Plug();
}

ibv_send_wr* SendQueue::PostWorkRequests(ibv_send_wr* first_wr) {
  ibv_send_wr* bad_wr = nullptr;
  int rc = ibv_post_send(qp_, first_wr, &bad_wr);
  if (rc != 0) {
    LOG(ERROR) << "Fail to post send work requests: " << std::strerror(rc);
    CHECK(bad_wr != nullptr) << "ibv_post_send failed without returning bad_wr";
    return bad_wr;
  }
  return nullptr;
}

void SendQueue::Unplug() { Flush(); }

void SendQueue::Flush() {
  ibv_send_wr* first_wr = TakeAll();
  if (first_wr == nullptr) {
    return;
  }
  if (ibv_send_wr* first_unposted_wr = PostWorkRequests(first_wr)) {
    HandlePostFailure(first_wr, first_unposted_wr);
  }
}

ibv_send_wr* SendQueue::TakeAll() {
  ibv_send_wr* first_wr = pending_wr_head_;
  pending_wr_head_ = nullptr;
  pending_wr_tail_ = nullptr;
  return first_wr;
}

void SendQueue::HandlePostFailure(ibv_send_wr* first_wr,
                                  ibv_send_wr* first_unposted_wr) {
  LOG_IF(FATAL, first_unposted_wr != first_wr)
      << "ibv_post_send partially posted a WR list";

  Shutdown();
  HandleAllFailure(first_unposted_wr, IBV_WC_GENERAL_ERR);
}

void SendQueue::HandleAllFailure(ibv_send_wr* wr, int32_t wc_status) {
  while (wr != nullptr) {
    ibv_send_wr* next = wr->next;
    HandleOne(wr, wc_status);
    wr = next;
  }
}

void SendQueue::HandleOne(ibv_send_wr* wr, int32_t wc_status) {
  if (wr->wr_id == 0) {
    return;  // an unsignalled body WR: its batch tail answers for it
  }

  void* owner = WrIdPtr(wr->wr_id);
  switch (WrIdTag(wr->wr_id)) {
    case kTagOp:
      static_cast<OpAwaiter*>(owner)->OnComplete(wc_status);
      break;
    case kTagBatchEnd:
      static_cast<BatchAwaiter*>(owner)->OnComplete(wc_status);
      break;
    default:
      // A message WR cannot reach here: TrySubmit posts it directly and never
      // parks it, so it is never on the list this walks.
      LOG(FATAL) << "Fail to reclaim an unposted work request: tag="
                 << WrIdTag(wr->wr_id);
      break;
  }
}

void SendQueue::FailWaiters(int32_t wc_status) {
  const auto fail = [wc_status](OpAwaiter* op) { op->Abort(wc_status); };
  read_waiters_.TakeAllAnd(fail);
  waiters_.TakeAllAnd(fail);
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
