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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_FSOP_RUNNER_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_FSOP_RUNNER_H_

#include <cstdint>

#include "cache/tools/bench/common/histogram.h"
#include "cache/tools/bench/fsop/options.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace fsop {

// The syscalls of one block write+read cycle, in execution order. Mirrors
// LocalFileSystem::WriteFile (+ DiskCache::Stage's hardlink) then a read-back.
enum Op : uint8_t {
  kMkdir = 0,
  kOpenW,
  kFallocate,
  kWrite,
  kFsync,
  kCloseW,
  kRename,
  kLink,
  kOpenR,
  kRead,
  kCloseR,
  kUnlink,
  kNumOps,
};

extern const char* const kOpNames[kNumOps];

// Per-op latency histograms (nanoseconds), aggregated across threads.
struct OpStats {
  LatencyHistogram hist[kNumOps];
  uint64_t errors{0};

  void Merge(const OpStats& other);
};

OpStats RunCell(const Options& options, uint64_t block_size, uint32_t threads);
void PrintCell(const Options& options, uint64_t block_size, uint32_t threads,
               const OpStats& stats);

}  // namespace fsop
}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_FSOP_RUNNER_H_
