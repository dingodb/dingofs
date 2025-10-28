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

#include "cache/utils/inflight_tracker.h"

namespace dingofs {
namespace cache {

InflightTracker::InflightTracker(uint64_t max_inflights)
    : inflights_(0), max_inflights_(max_inflights) {}

Status InflightTracker::Add(const std::string& key) {
  std::unique_lock<BthreadMutex> lock(mutex_);
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

void InflightTracker::Remove(const std::string& key) {
  std::lock_guard<BthreadMutex> lk(mutex_);
  CHECK_GT(inflights_, 0);
  inflights_ -= 1;
  busy_.erase(key);
  cond_.notify_all();
}

}  // namespace cache
}  // namespace dingofs
