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

#ifndef DINGOFS_BLOCKCACHE_UTILS_TIME_H_
#define DINGOFS_BLOCKCACHE_UTILS_TIME_H_

#include <chrono>
#include <cstdint>

namespace dingofs {
namespace blockcache {

inline constexpr uint64_t kNsPerUs = 1000;
inline constexpr uint64_t kNsPerMs = 1000 * kNsPerUs;
inline constexpr uint64_t kNsPerSec = 1000 * kNsPerMs;
inline constexpr uint64_t kUsPerSec = 1000 * 1000;

inline uint64_t TimestampNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

inline uint32_t TimestampSec() {
  return static_cast<uint32_t>(TimestampNs() / kNsPerSec);
}

inline thread_local uint64_t tls_cached_timestamp = 0;

inline void RefreshCachedTimestamp() { tls_cached_timestamp = TimestampNs(); }

inline uint64_t CachedTimestampNs() {
  return tls_cached_timestamp != 0 ? tls_cached_timestamp : TimestampNs();
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_UTILS_TIME_H_
