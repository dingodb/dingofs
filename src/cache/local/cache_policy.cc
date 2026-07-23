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
 * Created Date: 2026-07-23
 * Author: Jingli Chen (Wine93)
 */

#include "cache/local/cache_policy.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>
#include <string>

#include "cache/local/lru_policy.h"
#include "cache/local/none_policy.h"
#include "cache/local/s3fifo_policy.h"
#include "cache/local/sieve_policy.h"
#include "cache/local/two_random_policy.h"

namespace dingofs {
namespace cache {

DEFINE_string(cache_eviction_policy, "lru",
              "cache eviction policy: sieve | s3fifo | 2random | lru | none. "
              "Read once at cache startup; changing it requires a restart.");

static bool ValidateEvictionPolicy(const char* /*name*/,
                                   const std::string& value) {
  return value == "sieve" || value == "s3fifo" || value == "2random" ||
         value == "lru" || value == "none";
}
DEFINE_validator(cache_eviction_policy, &ValidateEvictionPolicy);

EvictionPolicyUPtr NewEvictionPolicy(const std::string& name) {
  const std::string& policy = name.empty() ? FLAGS_cache_eviction_policy : name;
  if (policy == "sieve") {
    return std::make_unique<SievePolicy>();
  }
  if (policy == "s3fifo") {
    return std::make_unique<S3FifoPolicy>();
  }
  if (policy == "2random") {
    return std::make_unique<TwoRandomPolicy>();
  }
  if (policy == "none") {
    return std::make_unique<NonePolicy>();
  }
  if (policy != "lru") {
    LOG(WARNING) << "Unknown cache eviction policy '" << policy
                 << "', falling back to lru.";
  }
  return std::make_unique<LruPolicy>();
}

}  // namespace cache
}  // namespace dingofs
