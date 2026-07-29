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

#ifndef DINGOFS_CACHE_CORE_REACTOR_IDLE_H_
#define DINGOFS_CACHE_CORE_REACTOR_IDLE_H_

#include <atomic>
#include <cstdint>

#include "cache/v1/core/utils/align.h"
#include "cache/v1/core/utils/cpu.h"
#include "cache/v1/core/utils/time.h"

namespace dingofs {
namespace cache {

class IdleSpinner {
 public:
  explicit IdleSpinner(bool never_sleep) : never_sleep_(never_sleep) {}

  // return true if should sleep
  bool Spin() {
    if (!idle_) {
      idle_ = true;
      since_ns_ = NowNs();
      spins_ = 0;
    }

    CpuRelax();

    if (never_sleep_) {
      return false;
    }

    if ((++spins_ & 63) != 0) {
      return false;
    }

    return NowNs() - since_ns_ > kBudgetNs;
  }

  // return the time spent idle
  uint64_t Reset() {
    if (!idle_) {
      return 0;
    }

    idle_ = false;
    return NowNs() - since_ns_;
  }

 private:
  static constexpr uint64_t kBudgetNs = 200'000;

  uint64_t since_ns_ = 0;
  uint64_t spins_ = 0;
  bool idle_ = false;
  const bool never_sleep_;
};

class Doorbell {
 public:
  void Arm() { armed_.store(true, std::memory_order_seq_cst); }
  void Disarm() { armed_.store(false, std::memory_order_relaxed); }

  bool ClaimRing() {
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (!armed_.load(std::memory_order_relaxed)) {
      return false;
    }

    armed_.store(false, std::memory_order_relaxed);
    return true;
  }

 private:
  alignas(kCacheLineSize) std::atomic<bool> armed_{false};
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_CORE_REACTOR_IDLE_H_
