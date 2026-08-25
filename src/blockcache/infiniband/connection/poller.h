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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_POLLER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_POLLER_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <memory>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/dispatcher.h"
#include "blockcache/core/reactor/poller.h"
#include "blockcache/infiniband/base/completion_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class InfinibandPoller;

class CompletionChannelEvent final : public Event {
 public:
  explicit CompletionChannelEvent(InfinibandPoller* poller) : poller_(poller) {}

  void OnReady() noexcept override;
  void OnCancelled() noexcept override;

 private:
  InfinibandPoller* poller_;
};

using CompletionChannelEventUPtr = std::unique_ptr<CompletionChannelEvent>;

class InfinibandPoller final : public Poller {
 public:
  InfinibandPoller(CompletionQueue* completion_queue, int channel_fd);
  ~InfinibandPoller() override;

  InfinibandPoller(const InfinibandPoller&) = delete;
  InfinibandPoller& operator=(const InfinibandPoller&) = delete;

  bool Poll() override;
  bool PurePoll() override;
  bool TryEnterInterruptMode() override;
  void ExitInterruptMode() override;

  Future<> Disarm();

 private:
  friend class CompletionChannelEvent;

  static constexpr int kCompletionBatchSize = 64;
  static constexpr int kCompletionBudget = 256;

  bool DrainPendingCompletions();
  void DispatchCompletion(const ibv_wc& wc);

  CompletionQueue* completion_queue_;
  int completion_channel_fd_;
  CompletionChannelEventUPtr completion_channel_event_;
  bool channel_event_registered_ = false;
  uint64_t error_wc_count_ = 0;
  ibv_wc pending_completions_[kCompletionBatchSize];
  int pending_completion_count_ = 0;
  int next_completion_index_ = 0;
  Promise<>* disarm_promise_ = nullptr;
};

using InfinibandPollerUPtr = std::unique_ptr<InfinibandPoller>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_POLLER_H_
