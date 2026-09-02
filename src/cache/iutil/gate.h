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

/*
 * Project: DingoFS
 * Created Date: 2026-09-02
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_IUTIL_GATE_H_
#define DINGOFS_SRC_CACHE_IUTIL_GATE_H_

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <glog/logging.h>

#include <cstdint>
#include <mutex>

namespace dingofs {
namespace cache {
namespace iutil {

class Gate {
 public:
  bool Enter() {
    std::lock_guard<bthread::Mutex> lock(mutex_);
    if (closing_) {
      return false;
    }
    inflights_++;
    return true;
  }

  void Leave() {
    std::lock_guard<bthread::Mutex> lock(mutex_);
    DCHECK_GT(inflights_, 0);
    if (--inflights_ == 0 && closing_) {
      cond_.notify_all();
    }
  }

  void Close() {
    std::unique_lock<bthread::Mutex> lock(mutex_);
    closing_ = true;
    while (inflights_ > 0) {
      cond_.wait(lock);
    }
  }

 private:
  bthread::Mutex mutex_;
  bthread::ConditionVariable cond_;
  bool closing_{false};
  uint64_t inflights_{0};
};

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_IUTIL_GATE_H_
