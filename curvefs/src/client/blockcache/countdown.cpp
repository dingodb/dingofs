/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-05
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/blockcache/countdown.h"

#include <glog/logging.h>

#include <cassert>

#include "curvefs/src/client/blockcache/error.h"

namespace curvefs {
namespace client {
namespace blockcache {

void Countdown::Add(uint64_t key, int64_t n, bool has_error) {
  std::unique_lock<std::mutex> lk(mutex_);
  auto it = counters_.find(key);
  if (it == counters_.end()) {
    counters_.emplace(key, Counter());
  }

  auto& counter = counters_[key];
  counter.count += n;
  if (has_error) {
    has_error_[key] = true;
  }
  CHECK(counter.count >= 0);
  if (counter.count == 0) {
    auto cond = counter.cond;
    counters_.erase(key);
    cond->notify_all();
  }
}

BCACHE_ERROR Countdown::Wait(uint64_t key) {
  std::unique_lock<std::mutex> lk(mutex_);
  while (true) {
    auto it = counters_.find(key);
    if (it == counters_.end()) {
      break;
    }

    // The cond will released after wait() return
    auto cond = it->second.cond;
    cond->wait(lk);
  }

  auto it = has_error_.find(key);
  if (it == has_error_.end()) {
    return BCACHE_ERROR::OK;
  }
  has_error_.erase(it);
  return BCACHE_ERROR::NOT_FOUND;
}

size_t Countdown::Size() {
  std::unique_lock<std::mutex> lk(mutex_);
  return counters_.size();
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
