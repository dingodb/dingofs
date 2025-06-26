/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2025-05-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_UTILS_INFLIGHT_THROTTLE_H_
#define DINGOFS_SRC_CACHE_UTILS_INFLIGHT_THROTTLE_H_

#include <absl/strings/str_format.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <cstring>
#include <memory>

#include "cache/common/type.h"

namespace dingofs {
namespace cache {

class InflightThrottle {
 public:
  explicit InflightThrottle(uint64_t max_inflights)
      : max_inflights_(max_inflights) {}

  void Increment(uint64_t n) {
    std::unique_lock<BthreadMutex> lock(mutex_);
    while (inflights_ + n > max_inflights_) {
      cond_.wait(lock);
    }

    inflights_ += n;
  }

  void Decrement(uint64_t n) {
    std::unique_lock<BthreadMutex> lock(mutex_);
    CHECK_GE(inflights_, n);
    inflights_ -= n;
    cond_.notify_all();
  }

  uint64_t GetInflights() {
    std::unique_lock<BthreadMutex> lock(mutex_);
    return inflights_;
  }

 private:
  uint64_t inflights_{0};
  const uint64_t max_inflights_;
  BthreadMutex mutex_;
  BthreadConditionVariable cond_;
};

using InflightThrottleSPtr = std::shared_ptr<InflightThrottle>;
using InflightThrottleUPtr = std::unique_ptr<InflightThrottle>;

class InflightThrottleGuard {
 public:
  InflightThrottleGuard(InflightThrottleSPtr throttle, uint64_t inflights)
      : throttle_(throttle), inflights_(inflights) {
    if (throttle_) {
      throttle_->Increment(1);
    }
  }

  ~InflightThrottleGuard() {
    if (throttle_) {
      throttle_->Decrement(inflights_);
    }
  }

 private:
  InflightThrottleSPtr throttle_;
  const uint64_t inflights_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_INFLIGHT_THROTTLE_H_
