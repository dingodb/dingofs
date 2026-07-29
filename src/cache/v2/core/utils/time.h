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

#ifndef DINGOFS_CACHE_CORE_UTILS_TIME_H_
#define DINGOFS_CACHE_CORE_UTILS_TIME_H_

#include <chrono>
#include <cstdint>

namespace dingofs {
namespace cache {

// Monotonic nanoseconds (CLOCK_MONOTONIC: immune to clock adjustments). The one
// time base for program logic -- timer deadlines, timeouts and the reactor's
// idle budget all compare against it. (Benchmarks use the cheaper TSC helpers
// in bench/harness/tsc.h for measurement only.)
inline uint64_t NowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_CORE_UTILS_TIME_H_
