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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_FREE_SPACE_MONITOR_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_FREE_SPACE_MONITOR_H_

#include <cstdint>

namespace dingofs {
namespace cache {

// Three-band free-space ladder (cachefilesd-style run/cull/stop), judged on
// the worse of free-bytes ratio and free-inodes ratio:
//
//   free >= run   : kOk    - no eviction, admit everything
//   [cull, run)   : kOk    - hysteresis: an in-progress cull keeps running
//                            until free climbs back above run
//   [stop, cull)  : kCull  - background eviction, still admit everything
//   free < stop   : kStop  - reject caching new blocks (last-resort backstop)
//
// Pure logic only: no threads, no IO, no flags — callers feed in a statfs
// snapshot plus usage numbers and get back the band and the eviction target.
class FreeSpaceMonitor {
 public:
  enum class Band : uint8_t { kOk = 0, kCull = 1, kStop = 2 };

  // Eviction goal ratios for the capacity dimension, shared with the inline
  // trigger in DiskCacheManager::Add().
  static constexpr double kCapacityEvictGoalRatio = 0.95;
  static constexpr double kCapacityEvictFilesRatio = 0.05;

  struct Thresholds {
    double run;   // eviction goal: free space restored above it
    double cull;  // background-eviction trigger
    double stop;  // hard-reject backstop

    // Flags are hot-reloadable, so enforce stop < cull <= run at use time.
    Thresholds Sanitized() const;
  };

  struct StatFS {
    double free_bytes_ratio;
    double free_files_ratio;
    uint64_t total_bytes;
    uint64_t total_files;
  };

  struct Usage {
    uint64_t used_bytes;
    uint64_t capacity_bytes;
    uint64_t num_blocks;
    uint64_t inflight_free_bytes;  // enqueued for unlink, not yet in statfs
    uint64_t inflight_free_files;
  };

  struct Target {
    uint64_t want_free_bytes{0};
    uint64_t want_free_files{0};

    bool NeedEvict() const {
      return want_free_bytes > 0 || want_free_files > 0;
    }
  };

  static Band Judge(const Thresholds& thresholds, const StatFS& stat,
                    Band prev);

  // Merges the free-space deficit (restore to run, minus in-flight deletions)
  // with the capacity overshoot (same goal as the trigger in Add()) into one
  // eviction target.
  static Target CalcTarget(const Thresholds& thresholds, const StatFS& stat,
                           const Usage& usage);
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_FREE_SPACE_MONITOR_H_
