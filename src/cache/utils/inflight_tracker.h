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

#ifndef DINGOFS_SRC_CACHE_UTILS_INFLIGHT_TRACKER_H_
#define DINGOFS_SRC_CACHE_UTILS_INFLIGHT_TRACKER_H_

#include <memory>
#include <unordered_set>

#include "cache/common/type.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class InflightTracker {
 public:
  InflightTracker(uint64_t max_inflights);

  Status Add(const std::string& key);
  void Remove(const std::string& key);

 private:
  uint64_t inflights_;
  uint64_t max_inflights_;
  std::unordered_set<std::string> busy_;
  BthreadMutex mutex_;
  BthreadConditionVariable cond_;
};

using InflightTrackerUPtr = std::unique_ptr<InflightTracker>;
using InflightTrackerSPtr = std::shared_ptr<InflightTracker>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_INFLIGHT_TRACKER_H_
