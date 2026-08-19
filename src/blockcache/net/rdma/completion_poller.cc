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

#include "blockcache/net/rdma/completion_poller.h"

#include <glog/logging.h>
#include <poll.h>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/net/infiniband/wc_status.h"
#include "blockcache/net/rdma/connection.h"
#include "blockcache/net/rdma/domain.h"
#include "blockcache/net/rdma/recv_ring.h"
#include "blockcache/net/rdma/rpc/frame_pool.h"
#include "blockcache/net/rdma/send_queue.h"
#include "blockcache/net/rdma/wr_id.h"

namespace dingofs {
namespace blockcache {

CompletionPoller::CompletionPoller(RdmaDomain* domain)
    : domain_(domain), channel_poll_(this) {
  ThisReactor().RegisterPoller(this);
  registered_ = true;
}

CompletionPoller::~CompletionPoller() {
  if (registered_) {
    ThisReactor().UnregisterPoller(this);
    registered_ = false;
  }
}

void CompletionPoller::FlushPending() {
  dirty_sends_.FlushAll();
  if (!dirty_recvs_.empty()) {
    for (RdmaConnection* conn : dirty_recvs_) {
      conn->set_recv_dirty(false);
      conn->messenger().recv().FlushReplenish();
    }
    dirty_recvs_.clear();
  }
}

void CompletionPoller::MarkDirtyRecv(RdmaConnection* conn) {
  if (conn->recv_dirty()) {
    return;
  }
  conn->set_recv_dirty(true);
  dirty_recvs_.push_back(conn);
}

// One-shot: the registration is over, so the next TryEnterInterruptMode() adds
// it again.
Future<> CompletionPoller::Disarm() {
  Promise<> promise;
  Future<> done = promise.GetFuture();
  disarm_promise_ = &promise;
  // Cancel() answers whether an OnCancelled() is actually coming, so the
  // registered/not-registered case needs no flag of its own here.
  if (!ThisDispatcher().DeleteEvent(&channel_poll_)) {
    disarm_promise_ = nullptr;
    co_return;
  }
  co_await std::move(done);
}

bool CompletionPoller::Poll() {
  int done = 0;
  if (DispatchStaged()) {
    done = 1;
  }

  for (;;) {
    int n = domain_->cq().Poll(kWcBatch, staged_);
    if (n < 0) {
      LOG(FATAL) << "Fail to poll completion queue";
    }
    if (n == 0) {
      break;
    }
    staged_count_ = n;
    staged_pos_ = 0;
    DispatchStaged();
    done += n;
    if (n < kWcBatch || done >= kPollBudget) {
      break;
    }
  }

  // Post whatever the dispatched completions queued up: one doorbell per
  // send queue, not one per operation.
  FlushPending();
  return done > 0;
}

bool CompletionPoller::PurePoll() {
  if (staged_pos_ < staged_count_) {
    return true;
  }
  // verbs cannot peek without consuming, so a "check only" poll has to reap
  // into the staging area and let the next Poll drain it.
  int n = domain_->cq().Poll(kWcBatch, staged_);
  if (n <= 0) {
    return false;
  }
  staged_count_ = n;
  staged_pos_ = 0;
  return true;
}

bool CompletionPoller::TryEnterInterruptMode() {
  if (domain_->cq().ReqNotify() != 0) {
    return false;  // veto sleeping rather than risk a lost wakeup
  }
  // Arming and then finding work is the classic one-shot race: re-check
  // after arming, and stay awake if anything showed up.
  if (PurePoll()) {
    return false;
  }
  if (!channel_poll_pending_) {
    ThisDispatcher().AddEvent(domain_->comp_channel_fd(), &channel_poll_,
                              EventMode::kPollOnce);
    channel_poll_pending_ = true;
  }
  return true;
}

void CompletionPoller::ExitInterruptMode() {
  // Acknowledging takes a lock inside the provider, so events are drained
  // and acked in batches (see verbs::CompletionQueue::DrainEvents).
  (void)domain_->cq().DrainEvents();
}

void CompletionPoller::ChannelPoll::OnReady() noexcept {
  owner_->channel_poll_pending_ = false;
}

void CompletionPoller::ChannelPoll::OnCancelled() noexcept {
  owner_->channel_poll_pending_ = false;
  if (owner_->disarm_promise_ != nullptr) {
    Promise<>* promise = owner_->disarm_promise_;
    owner_->disarm_promise_ = nullptr;
    promise->SetValue();
  }
}

bool CompletionPoller::DispatchStaged() {
  const bool had_work = staged_pos_ < staged_count_;
  while (staged_pos_ < staged_count_) {
    const ibv_wc& wc = staged_[staged_pos_++];
    // Prefetch the next completion object: the dispatch below is a data
    // dependent load away from a cold cache line otherwise.
    if (staged_pos_ < staged_count_) {
      __builtin_prefetch(WrIdPtr(staged_[staged_pos_].wr_id));
    }
    Dispatch(wc);
  }
  staged_count_ = 0;
  staged_pos_ = 0;
  return had_work;
}

void CompletionPoller::Dispatch(const ibv_wc& wc) {
  // Body requests of a batch are unsignalled and carry id 0. They never
  // complete on success; on failure the error completion still arrives, and
  // it owns no awaiter -- decoding it as one would dereference null.
  if (wc.wr_id == 0) {
    if (wc.status != IBV_WC_SUCCESS && error_wc_logged_++ < 8) {
      LOG(ERROR)
          << "Fail to complete rdma batch body: "
          << verbs::WcStatus(wc.status, "complete work request").ToString();
    }
    return;
  }

  // NOTE: wc.opcode is only meaningful when status is SUCCESS, so routing
  // uses the work request id alone. (The predecessor switched on wc.opcode
  // unconditionally.)
  void* owner = WrIdPtr(wc.wr_id);
  switch (WrIdTag(wc.wr_id)) {
    case kTagOp: {
      auto* op = static_cast<OpAwaiter*>(owner);
      op->queue()->OnOpWc(op, wc.status);
      break;
    }
    case kTagBatchEnd: {
      auto* batch = static_cast<BatchAwaiter*>(owner);
      batch->queue()->OnBatchWc(batch, wc.status);
      break;
    }
    case kTagRecv: {
      auto* slot = static_cast<RecvSlot*>(owner);
      slot->conn->OnRecvWc(wc, slot);
      break;
    }
    case kTagFrame: {
      auto* buffer = static_cast<FrameBuf*>(owner);
      buffer->conn->OnFrameWc(buffer, wc.status);
      break;
    }
    default:
      LOG(ERROR) << "Fail to dispatch completion: unknown tag="
                 << WrIdTag(wc.wr_id);
      break;
  }
}

}  // namespace blockcache
}  // namespace dingofs
