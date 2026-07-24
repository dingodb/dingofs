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

#include "common/writemempool/cpu_local.h"

#include <glog/logging.h>
#include <sched.h>

#include <functional>
#include <limits>
#include <thread>

namespace dingofs {
namespace {

constexpr uint32_t kMinCpuLocalCaches = 8;

bool IsPowerOfTwo(uint32_t value) {
  return value != 0 && (value & (value - 1)) == 0;
}

}  // namespace

uint32_t CpuLocalCacheCount() {
  const uint32_t cpus = std::thread::hardware_concurrency();
  uint32_t count = kMinCpuLocalCaches;
  while (count < cpus) {
    CHECK_LE(count, std::numeric_limits<uint32_t>::max() / 2);
    count <<= 1;
  }
  return count;
}

uint32_t CurrentCpuLocalCache(uint32_t cache_count) {
  DCHECK(IsPowerOfTwo(cache_count));
  const int cpu = sched_getcpu();
  if (cpu >= 0) {
    return static_cast<uint32_t>(cpu) & (cache_count - 1);
  }

  static thread_local const uint32_t fallback = static_cast<uint32_t>(
      std::hash<std::thread::id>{}(std::this_thread::get_id()));
  return fallback & (cache_count - 1);
}

}  // namespace dingofs
