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

/*
 * Project: DingoFS
 * Created Date: 2025-06-08
 * Author: Jingli Chen (Wine93)
 */

#include "metrics/cache/disk_cache_metric.h"

#include "cache/common/const.h"

namespace dingofs {
namespace cache {

DiskCacheMetric::DiskCacheMetric(cache::DiskCacheOption option)
    : option_(option) {
  Expose(absl::StrFormat("dingofs_disk_cache_%d_", option.cache_index));
  Reset();
}

void DiskCacheMetric::Expose(const std::string& prefix) {
  uuid.expose_as(prefix, "uuid");
  dir.expose_as(prefix, "dir");
  used_bytes.expose_as(prefix, "used_bytes");
  capacity.expose_as(prefix, "capacity");
  free_space_ratio.expose_as(prefix, "free_space_ratio");
  load_status.expose_as(prefix, "load_status");
  running_status.expose_as(prefix, "running_status");
  healthy_status.expose_as(prefix, "healthy_status");
  stage_skips.expose_as(prefix, "stage_skips");  // stage
  stage_blocks.expose_as(prefix, "stage_blocks");
  stage_full.expose_as(prefix, "stage_full");
  cache_hits.expose_as(prefix, "cache_hits");  // cache
  cache_misses.expose_as(prefix, "cache_misses");
  cache_blocks.expose_as(prefix, "cache_blocks");
  cache_bytes.expose_as(prefix, "cache_bytes");
  cache_full.expose_as(prefix, "cache_full");
}

void DiskCacheMetric::Reset() {
  uuid.set_value("-");
  dir.set_value(option_.cache_dir);
  used_bytes.set_value(0);
  capacity.set_value(option_.cache_size_mb * kMiB);
  free_space_ratio.set_value(cache::FLAGS_free_space_ratio);
  load_status.set_value("STOP");
  running_status.set_value("DOWN");
  healthy_status.set_value("unknown");
  stage_skips.reset();
  stage_blocks.reset();
  stage_full.set_value(false);
  cache_hits.reset();
  cache_misses.reset();
  cache_blocks.reset();
  cache_bytes.reset();
  cache_full.set_value(false);
}

}  // namespace cache
}  // namespace dingofs
