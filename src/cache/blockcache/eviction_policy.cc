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
 * Created Date: 2026-07-22
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/eviction_policy.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "cache/blockcache/lru_cache.h"
#include "cache/blockcache/s3fifo_policy.h"
#include "cache/blockcache/two_random_policy.h"

namespace dingofs {
namespace cache {

DEFINE_string(cache_eviction_policy, "lru",
              "cache eviction policy: lru, s3fifo or 2random "
              "(restart required to switch)");

DEFINE_double(cache_s3fifo_small_ratio, 0.1,
              "ratio of cache capacity used by the s3fifo probationary "
              "(small) queue");

EvictionPolicyUPtr NewEvictionPolicy(std::string_view name,
                                     uint64_t capacity_bytes) {
  if (name == "s3fifo") {
    return std::make_unique<S3FIFOPolicy>(capacity_bytes,
                                          FLAGS_cache_s3fifo_small_ratio);
  } else if (name == "2random") {
    return std::make_unique<TwoRandomPolicy>();
  } else if (name == "lru") {
    return std::make_unique<LRUCache>();
  }
  LOG(FATAL) << "Unknown cache eviction policy: " << name;
  return nullptr;  // unreachable
}

}  // namespace cache
}  // namespace dingofs
