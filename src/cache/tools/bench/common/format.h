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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_FORMAT_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_FORMAT_H_

#include <cstdint>
#include <string>

namespace dingofs {
namespace cache {
namespace bench {

std::string FormatBytes(uint64_t bytes);          // 4.00 MiB
std::string FormatRate(double bytes_per_s);       // 1.23 GiB/s
std::string FormatCount(double value);            // 1.90 M / 170.00 k
std::string FormatLatencyUs(uint64_t latency_us); // 640 us / 1.20 ms
std::string FormatNanos(uint64_t ns);             // 850 ns / 4.20 us / 1.50 ms
std::string FormatDuration(double seconds);       // 1.5s / 1m30s

}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_FORMAT_H_
