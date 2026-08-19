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

#ifndef DINGOFS_BLOCKCACHE_CORE_RUNTIME_SHARD_INBOX_H_
#define DINGOFS_BLOCKCACHE_CORE_RUNTIME_SHARD_INBOX_H_

#include <atomic>

#include "blockcache/core/reactor/poller.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/utils/containers/mpsc_queue.h"

namespace dingofs {
namespace blockcache {

struct InboxWork : MpscNode {
  void (*run)(InboxWork*) = nullptr;
};

// Runs one queued item. The node IS the work, and `next` has already been
// read by the consumer, so the callback may free it.
inline void RunInboxWork(MpscNode* node) {
  auto* work = static_cast<InboxWork*>(node);
  work->run(work);
}

// The one way into a shard from outside the runtime.
class ShardInbox final : public Poller {
 public:
  ShardInbox() = default;

  ShardInbox(const ShardInbox&) = delete;
  ShardInbox& operator=(const ShardInbox&) = delete;

  void Open(Reactor* reactor) {
    reactor_.store(reactor, std::memory_order_release);
    closed_.store(false, std::memory_order_release);
  }

  void Close() { closed_.store(true, std::memory_order_release); }

  bool Post(InboxWork* work) {
    if (closed_.load(std::memory_order_acquire)) {
      return false;
    }

    queue_.Push(work);
    reactor_.load(std::memory_order_acquire)->Wakeup();
    return true;
  }

  bool Poll() override { return queue_.ConsumeAll(RunInboxWork) != 0; }

  bool PurePoll() override { return !empty(); }
  bool TryEnterInterruptMode() override { return empty(); }

  bool empty() const { return queue_.empty(); }

 private:
  std::atomic<Reactor*> reactor_{nullptr};
  std::atomic<bool> closed_{true};
  MpscQueue queue_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_RUNTIME_SHARD_INBOX_H_
