/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http:
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "blockcache/infiniband/connection/poller.h"

#include <glog/logging.h>

#include <memory>
#include <utility>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/infiniband/base/completion_queue.h"
#include "blockcache/infiniband/common/wr_id.h"
#include "blockcache/infiniband/connection/connection.h"
#include "blockcache/infiniband/connection/plug.h"
#include "blockcache/infiniband/connection/receive_buffer.h"
#include "blockcache/infiniband/connection/send_buffer.h"
#include "blockcache/infiniband/connection/send_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

void CompletionChannelEvent::OnReady() noexcept {
  poller_->channel_event_registered_ = false;
}

void CompletionChannelEvent::OnCancelled() noexcept {
  poller_->channel_event_registered_ = false;
  if (poller_->disarm_promise_ != nullptr) {
    Promise<>* promise = poller_->disarm_promise_;
    poller_->disarm_promise_ = nullptr;
    promise->SetValue();
  }
}

InfinibandPoller::InfinibandPoller(CompletionQueue* completion_queue,
                                   int channel_fd)
    : completion_queue_(completion_queue),
      completion_channel_fd_(channel_fd),
      completion_channel_event_(
          std::make_unique<CompletionChannelEvent>(this)) {
  ThisReactor().RegisterPoller(this);
}

InfinibandPoller::~InfinibandPoller() { ThisReactor().UnregisterPoller(this); }

bool InfinibandPoller::Poll() {
  int done = 0;
  if (DrainPendingCompletions()) {
    done = 1;
  }

  // completion queue
  for (;;) {
    int n = completion_queue_->Poll(kCompletionBatchSize, pending_completions_);
    if (n < 0) {
      LOG(FATAL) << "Fail to poll completion queue";
    } else if (n == 0) {
      break;
    }

    pending_completion_count_ = n;
    next_completion_index_ = 0;
    DrainPendingCompletions();
    done += n;
    if (n < kCompletionBatchSize || done >= kCompletionBudget) {
      break;
    }
  }

  // submit (send queue + receive queue)
  ThisPlugs().UnplugAll();
  return done > 0;
}

bool InfinibandPoller::PurePoll() {
  if (next_completion_index_ < pending_completion_count_) {
    return true;
  }

  int n = completion_queue_->Poll(kCompletionBatchSize, pending_completions_);
  if (n <= 0) {
    return false;
  }

  pending_completion_count_ = n;
  next_completion_index_ = 0;
  return true;
}

bool InfinibandPoller::TryEnterInterruptMode() {
  if (completion_queue_->ReqNotify() != 0) {
    return false;
  }

  if (PurePoll()) {
    return false;
  }

  if (!channel_event_registered_) {
    ThisDispatcher().AddEvent(completion_channel_fd_,
                              completion_channel_event_.get(),
                              EventMode::kPollOnce);
    channel_event_registered_ = true;
  }
  return true;
}

void InfinibandPoller::ExitInterruptMode() { completion_queue_->DrainEvents(); }

Future<> InfinibandPoller::Disarm() {
  Promise<> promise;
  Future<> done = promise.GetFuture();
  disarm_promise_ = &promise;
  if (!ThisDispatcher().DeleteEvent(completion_channel_event_.get())) {
    disarm_promise_ = nullptr;
    co_return;
  }
  co_await std::move(done);
}

bool InfinibandPoller::DrainPendingCompletions() {
  const bool had_work = next_completion_index_ < pending_completion_count_;
  while (next_completion_index_ < pending_completion_count_) {
    const ibv_wc& wc = pending_completions_[next_completion_index_++];
    if (next_completion_index_ < pending_completion_count_) {
      __builtin_prefetch(
          WrIdPtr(pending_completions_[next_completion_index_].wr_id));
    }
    DispatchCompletion(wc);
  }
  pending_completion_count_ = 0;
  next_completion_index_ = 0;
  return had_work;
}

void InfinibandPoller::DispatchCompletion(const ibv_wc& wc) {
  if (wc.wr_id == 0) {
    if (wc.status != IBV_WC_SUCCESS && error_wc_count_++ < 8) {
      LOG(ERROR) << "Fail to complete rdma batch attachment: "
                 << ToStatus(wc.status, "complete work request").ToString();
    }
    return;
  }

  void* owner = WrIdPtr(wc.wr_id);
  switch (WrIdTag(wc.wr_id)) {
    case kTagOp:
      static_cast<OpAwaiter*>(owner)->OnComplete(wc.status);
      break;
    case kTagBatchEnd:
      static_cast<BatchAwaiter*>(owner)->OnComplete(wc.status);
      break;
    case kTagReceiveBuffer: {
      auto* buffer = static_cast<ReceiveBuffer*>(owner);
      static_cast<Connection*>(buffer->conn)->OnMessageReceived(buffer, wc);
      break;
    }
    case kTagSendBuffer: {
      auto* buffer = static_cast<SendBuffer*>(owner);
      static_cast<Connection*>(buffer->conn)->OnMessageSent(buffer, wc.status);
      break;
    }
    default:
      LOG(ERROR) << "Fail to dispatch completion: unknown tag="
                 << WrIdTag(wc.wr_id);
      break;
  }
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
