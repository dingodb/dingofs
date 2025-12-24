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
 * Created Date: 2025-07-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_IUTIL_INFLIGHT_TRACKER_H_
#define DINGOFS_SRC_CACHE_IUTIL_INFLIGHT_TRACKER_H_

#include <bthread/condition_variable.h>

#include <memory>
#include <unordered_set>

#include "common/status.h"

namespace dingofs {
namespace cache {
namespace iutil {

class InflightTracker {
 public:
  explicit InflightTracker(uint64_t max_inflights)
      : max_inflights_(max_inflights) {}

  Status Add(const std::string& key) {
    std::unique_lock<bthread::Mutex> lock(mutex_);
    if (busy_.count(key)) {
      return Status::Exist("task already running");
    }

    while (inflights_ + 1 > max_inflights_) {
      cond_.wait(lock);
    }

    busy_.emplace(key);
    inflights_ += 1;
    return Status::OK();
  }

  void Remove(const std::string& key) {
    std::lock_guard<bthread::Mutex> lock(mutex_);
    CHECK_GT(inflights_, 0);
    inflights_ -= 1;
    busy_.erase(key);
    cond_.notify_all();
  }

 private:
  uint64_t inflights_{0};
  uint64_t max_inflights_;
  std::unordered_set<std::string> busy_;
  bthread::Mutex mutex_;
  bthread::ConditionVariable cond_;
};

using InflightTrackerUPtr = std::unique_ptr<InflightTracker>;
using InflightTrackerSPtr = std::shared_ptr<InflightTracker>;

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_IUTIL_INFLIGHT_TRACKER_H_
