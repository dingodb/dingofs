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

// The sleep/wake handshake: a sleeper Arms, a waker Claims. Only the claimer
// pays for the wakeup syscall.

#ifndef DINGOFS_BLOCKCACHE_CORE_REACTOR_DOORBELL_H_
#define DINGOFS_BLOCKCACHE_CORE_REACTOR_DOORBELL_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>

#include "blockcache/utils/align.h"

namespace dingofs {
namespace blockcache {

class Doorbell {
 public:
  void Arm() { armed_.store(true, std::memory_order_seq_cst); }
  void Disarm() { armed_.store(false, std::memory_order_relaxed); }

  bool ClaimWakeup() {
    return armed_.exchange(false, std::memory_order_seq_cst);
  }

 private:
  alignas(kCacheLineSize) std::atomic<bool> armed_{false};
};

// A Doorbell with somewhere to sleep. The waiter re-checks its own condition
// after arming and before blocking, so work that lands in that window is not
// lost; the waker only touches the mutex when it claims an armed bell, which
// is what keeps a busy consumer syscall-free.
//
// One waiter. Wake() is safe from any thread.
class Parker {
 public:
  // Relative on purpose: an absolute deadline would have to agree with the
  // caller's clock domain, and TimestampNs() is steady rather than epoch.
  // Returns false when the timeout ran out with no wakeup. `ready` is checked
  // under the same lock the waker takes, so it must not block.
  template <typename Ready>
  bool WaitFor(Ready ready, uint64_t timeout_ns) {
    bell_.Arm();
    if (ready()) {
      bell_.Disarm();
      return true;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    const bool woken =
        cv_.wait_for(lock, std::chrono::nanoseconds(timeout_ns),
                     [this, &ready] { return woken_ || ready(); });
    woken_ = false;
    bell_.Disarm();
    return woken;
  }

  void Wake() {
    if (!bell_.ClaimWakeup()) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      woken_ = true;
    }
    cv_.notify_one();
  }

 private:
  Doorbell bell_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool woken_ = false;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_REACTOR_DOORBELL_H_
