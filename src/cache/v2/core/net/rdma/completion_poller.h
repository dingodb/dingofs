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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_COMPLETION_POLLER_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_COMPLETION_POLLER_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <vector>

#include "cache/v2/core/reactor/coroutine.h"
#include "cache/v2/core/reactor/dispatcher.h"
#include "cache/v2/core/reactor/poller.h"

// Dispatch keys off the wr_id tag: kTagOp/kTagBatchEnd -> SendQueue,
// kTagRecv/kTagFrame -> RdmaConnection.

#include "cache/v2/core/net/rdma/send_queue.h"

namespace dingofs {
namespace cache {
namespace v2 {

class RdmaConnection;
class RdmaDomain;

// Drives the shard's completion queue from the reactor loop: busy-polls
// while there is work, arms the completion channel only before sleeping.
class CompletionPoller final : public Poller {
 public:
  explicit CompletionPoller(RdmaDomain* domain);
  ~CompletionPoller() override;

  CompletionPoller(const CompletionPoller&) = delete;
  CompletionPoller& operator=(const CompletionPoller&) = delete;

  // Waits out the channel event's cancellation; no dangling source after.
  Future<> Disarm();

  bool Poll() override;
  bool PurePoll() override;
  bool TryEnterInterruptMode() override;
  void ExitInterruptMode() override;

  // Called at the end of every poll.
  void FlushPending();
  void MarkDirtyRecv(RdmaConnection* conn);

  // Batched doorbells, flushed once per poll: one MMIO write per poll
  // instead of one per operation.
  DoorbellList& dirty_sends() { return dirty_sends_; }

 private:
  // 64 completions per ibv_poll_cq call; at most 256 per reactor pass so one
  // busy connection cannot starve the rest of the shard.
  static constexpr int kWcBatch = 64;
  static constexpr int kPollBudget = 256;

  class ChannelPoll final : public Event {
   public:
    explicit ChannelPoll(CompletionPoller* owner) : owner_(owner) {}
    void OnReady() noexcept override;
    void OnCancelled() noexcept override;

   private:
    CompletionPoller* owner_;
  };

  bool DispatchStaged();
  void Dispatch(const ibv_wc& wc);

  RdmaDomain* domain_;
  DoorbellList dirty_sends_;
  std::vector<RdmaConnection*> dirty_recvs_;
  ChannelPoll channel_poll_;
  bool channel_poll_pending_ = false;  // registered with the engine
  bool registered_ = false;
  uint64_t error_wc_logged_ = 0;

  // verbs cannot peek without consuming: PurePoll reaps here, Poll drains.
  ibv_wc staged_[kWcBatch];
  int staged_count_ = 0;
  int staged_pos_ = 0;

  Promise<>* disarm_promise_ = nullptr;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_COMPLETION_POLLER_H_
