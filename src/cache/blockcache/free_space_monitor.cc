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

#include "cache/blockcache/free_space_monitor.h"

#include <algorithm>

namespace dingofs {
namespace cache {

FreeSpaceMonitor::Thresholds FreeSpaceMonitor::Thresholds::Sanitized() const {
  Thresholds t;
  t.cull = cull;
  t.run = std::max(run, cull);
  t.stop = std::min(stop, cull);
  return t;
}

FreeSpaceMonitor::Band FreeSpaceMonitor::Judge(const Thresholds& thresholds,
                                               const StatFS& stat, Band prev) {
  double free_ratio = std::min(stat.free_bytes_ratio, stat.free_files_ratio);
  if (free_ratio < thresholds.stop) {
    return Band::kStop;
  } else if (free_ratio < thresholds.cull) {
    return Band::kCull;
  } else if (free_ratio < thresholds.run) {
    return (prev == Band::kOk) ? Band::kOk : Band::kCull;
  }
  return Band::kOk;
}

FreeSpaceMonitor::Target FreeSpaceMonitor::CalcTarget(
    const Thresholds& thresholds, const StatFS& stat, const Usage& usage) {
  // free-space dimension: restore to the run watermark; deduct deletions
  // already enqueued but not yet reflected by statfs
  uint64_t fs_bytes = 0, fs_files = 0;
  if (stat.free_bytes_ratio < thresholds.run) {
    fs_bytes = stat.total_bytes * (thresholds.run - stat.free_bytes_ratio);
  }
  if (stat.free_files_ratio < thresholds.run) {
    fs_files = stat.total_files * (thresholds.run - stat.free_files_ratio);
  }
  fs_bytes -= std::min(fs_bytes, usage.inflight_free_bytes);
  fs_files -= std::min(fs_files, usage.inflight_free_files);

  // capacity dimension: used_bytes already excludes enqueued deletions, so no
  // in-flight deduction here
  uint64_t cap_bytes = 0, cap_files = 0;
  if (usage.used_bytes >= usage.capacity_bytes) {
    cap_bytes =
        usage.used_bytes - usage.capacity_bytes * kCapacityEvictGoalRatio;
    cap_files = usage.num_blocks * kCapacityEvictFilesRatio;
  }

  Target target;
  target.want_free_bytes = std::max(fs_bytes, cap_bytes);
  target.want_free_files = std::max(fs_files, cap_files);
  return target;
}

}  // namespace cache
}  // namespace dingofs
