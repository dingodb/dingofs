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
 * Created Date: 2024-09-26
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_THROTTLE_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_THROTTLE_H_

#include <atomic>
#include <memory>

#include "cache/common/type.h"
#include "utils/executor/executor.h"
#include "utils/throttle.h"

namespace dingofs {
namespace cache {

class UploadStageThrottle {
 public:
  UploadStageThrottle();
  virtual ~UploadStageThrottle() = default;

  void Start();
  void Shutdown();

  void Add(uint64_t upload_bytes);

 private:
  void UpdateThrottleParam();

  std::atomic<bool> running_;
  BthreadMutex mutex_;
  uint64_t current_throttle_bandwidth_mb_;
  uint64_t current_throttle_iops_;
  utils::ThrottleUPtr throttle_;
  std::unique_ptr<Executor> executor_;
};

using UploadStageThrottleUPtr = std::unique_ptr<UploadStageThrottle>;
using UploadStageThrottleSPtr = std::shared_ptr<UploadStageThrottle>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_THROTTLE_H_
