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

#ifndef DINGOFS_BLOCKCACHE_CORE_REACTOR_REACTOR_H_
#define DINGOFS_BLOCKCACHE_CORE_REACTOR_REACTOR_H_

#include <glog/logging.h>

#include <cstdint>
#include <vector>

#include "blockcache/core/reactor/dispatcher.h"
#include "blockcache/core/reactor/doorbell.h"
#include "blockcache/core/reactor/idle.h"
#include "blockcache/core/reactor/poller.h"
#include "blockcache/core/reactor/task.h"
#include "blockcache/core/reactor/timer.h"
#include "blockcache/utils/containers/circular_buffer.h"

namespace dingofs {
namespace blockcache {

class TaskQueue {
 public:
  void Push(Task* t) { queue_.PushBack(t); }

  template <typename Flush>
  void RunBatch(Flush flush, uint32_t flush_every) {
    uint32_t since_flush = 0;
    while (!queue_.empty()) {
      queue_.PopFront()->RunAndDispose();
      if (NeedPreempt()) {
        return;
      }

      if (++since_flush >= flush_every) {
        since_flush = 0;
        flush();
      }
    }
  }

  void Drain() {
    while (!queue_.empty()) {
      queue_.PopFront()->RunAndDispose();
    }
  }

  bool empty() const { return queue_.empty(); }

 private:
  CircularBuffer<Task*> queue_;
};

class PollerSet {
 public:
  PollerSet() = default;

  PollerSet(const PollerSet&) = delete;
  PollerSet& operator=(const PollerSet&) = delete;
  void Add(Poller* p) { pollers_.push_back(p); }
  void Remove(Poller* p) { std::erase(pollers_, p); }

  bool Poll() {
    bool work = false;
    for (size_t i = 0; i < pollers_.size(); ++i) {
      work |= pollers_[i]->Poll();
    }
    return work;
  }

  bool PurePoll() {
    for (size_t i = 0; i < pollers_.size(); ++i) {
      if (pollers_[i]->PurePoll()) {
        return true;
      }
    }
    return false;
  }

  bool TryEnterInterruptMode() {
    for (size_t armed = 0; armed < pollers_.size(); ++armed) {
      if (!pollers_[armed]->TryEnterInterruptMode()) {
        for (size_t i = armed; i > 0; --i) {
          pollers_[i - 1]->ExitInterruptMode();
        }
        return false;
      }
    }
    return true;
  }

  void ExitInterruptMode() {
    for (size_t i = pollers_.size(); i > 0; --i) {
      pollers_[i - 1]->ExitInterruptMode();
    }
  }

  void Flush() {
    for (size_t i = 0; i < pollers_.size(); ++i) {
      pollers_[i]->Flush();
    }
  }

 private:
  std::vector<Poller*> pollers_;
};

struct ReactorStats {
  uint64_t sleeps = 0;
  uint64_t idle_ns = 0;
};

class Reactor {
 public:
  explicit Reactor(unsigned shard_id);
  ~Reactor();

  Reactor(const Reactor&) = delete;
  Reactor& operator=(const Reactor&) = delete;

  void Run();  // blocks until Shutdown()
  void Shutdown();
  void Wakeup();

  void Schedule(Task* t) { tasks_.Push(t); }
  void RegisterPoller(Poller* p) { pollers_.Add(p); }
  void UnregisterPoller(Poller* p) { pollers_.Remove(p); }

  unsigned shard_id() const { return shard_id_; }
  const ReactorStats& stats() const { return stats_; }

 private:
  bool PollOnce();
  bool AnyWork();
  void TrySleep();

  unsigned shard_id_;
  ReactorStats stats_;
  TaskQueue tasks_;
  PollerSet pollers_;
  Dispatcher dispatcher_;
  TimerService timers_;
  IdleSpinner idle_;
  Doorbell bell_;
  bool stopped_ = false;  // owner thread only; Shutdown() runs on it
};

inline thread_local Reactor* tls_reactor = nullptr;

inline Reactor& ThisReactor() { return *tls_reactor; }
inline bool HasReactor() { return tls_reactor != nullptr; }

inline unsigned ThisShardId() {
  DCHECK(HasReactor()) << "ThisShardId off a shard thread";
  return tls_reactor->shard_id();
}

inline bool IsOnShard(unsigned shard) {
  return tls_reactor != nullptr && tls_reactor->shard_id() == shard;
}

[[gnu::always_inline]] inline void Schedule(Task* t) {
  tls_reactor->Schedule(t);
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_REACTOR_REACTOR_H_
