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

#ifndef DINGOFS_CACHE_V2_API_THROTTLE_H_
#define DINGOFS_CACHE_V2_API_THROTTLE_H_

#include <atomic>
#include <cstdint>
#include <memory>

#include "cache/v2/utils/align.h"

namespace dingofs {
namespace cache {
namespace v2 {

class InflightThrottle {
 public:
  InflightThrottle(unsigned shards, uint32_t max_inflight);

  bool Acquire(unsigned shard);
  void Release(unsigned shard);
  uint32_t Inflights() const;

 private:
  struct alignas(kCacheLineSize) Inflight {
    std::atomic<uint32_t> count{0};
  };

  const unsigned shards_;
  const uint32_t max_inflight_;
  std::unique_ptr<Inflight[]> inflights_;
};

using InflightThrottleUPtr = std::unique_ptr<InflightThrottle>;

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_API_THROTTLE_H_
