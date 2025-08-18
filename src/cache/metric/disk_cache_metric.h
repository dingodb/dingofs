/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_METRICS_DISK_CACHE_METRIC_H_
#define DINGOFS_SRC_CACHE_METRICS_DISK_CACHE_METRIC_H_

#include <absl/strings/str_format.h>
#include <bvar/reducer.h>
#include <bvar/status.h>
#include <glog/logging.h>

#include <string>

#include "cache/common/const.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

struct DiskCacheMetric {
  DiskCacheMetric(uint64_t cache_index, const std::string& cfg_cache_dir,
                  uint64_t cfg_cache_size_mb, double cfg_free_space_ratio)
      : cache_index(cache_index),
        prefix(absl::StrFormat("dingofs_disk_cache_%d", cache_index)),
        uuid(Name("uuid"), "-"),
        dir(Name("dir"), cfg_cache_dir.c_str()),
        used_bytes(Name("used_bytes"), 0),
        capacity(Name("capacity"), cfg_cache_size_mb * kMiB),
        free_space_ratio(Name("free_space_ratio"), cfg_free_space_ratio),
        load_status(Name("load_status"), "stop"),
        running_status(Name("running_status"), "down"),
        healthy_status(Name("healthy_status"), "unknown"),
        stage_skips(Name("stage_skips")),  // stage
        stage_blocks(Name("stage_blocks")),
        stage_full(Name("stage_full"), false),
        cache_hits(Name("cache_hits")),  // cache
        cache_misses(Name("cache_misses")),
        cache_blocks(Name("cache_blocks")),
        cache_bytes(Name("cache_bytes")),
        cache_full(Name("cache_full"), false) {}

  std::string Name(const std::string& name) const {
    CHECK_GT(prefix.length(), 0);
    return absl::StrFormat("%s_%s", prefix, name);
  }

  void Reset() {
    used_bytes.set_value(0);
    load_status.set_value("stoped");
    running_status.set_value("down");
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

  uint64_t cache_index;
  std::string prefix;
  bvar::Status<std::string> uuid;
  bvar::Status<std::string> dir;
  bvar::Status<int64_t> used_bytes;
  bvar::Status<int64_t> capacity;
  bvar::Status<double> free_space_ratio;
  bvar::Status<std::string> load_status;
  bvar::Status<std::string> running_status;
  bvar::Status<std::string> healthy_status;
  bvar::Adder<int64_t> stage_skips;  // stage
  bvar::Adder<int64_t> stage_blocks;
  bvar::Status<bool> stage_full;
  bvar::Adder<int64_t> cache_hits;  // cache
  bvar::Adder<int64_t> cache_misses;
  bvar::Adder<int64_t> cache_blocks;
  bvar::Adder<int64_t> cache_bytes;
  bvar::Status<bool> cache_full;
};

using DiskCacheMetricSPtr = std::shared_ptr<DiskCacheMetric>;

struct DiskCacheMetricGuard {
  DiskCacheMetricGuard(const std::string& op_name, Status& status,
                       DiskCacheMetricSPtr metric)
      : status(status), op_name(op_name), metric(metric) {}

  ~DiskCacheMetricGuard() {
    if (op_name == "Load") {
      if (status.ok()) {
        metric->cache_hits << 1;
      } else {
        metric->cache_misses << 1;
      }
    } else if (op_name == "Stage") {
      if (!status.ok()) {
        metric->stage_skips << 1;
      }
    }
  }

  std::string op_name;
  Status& status;
  DiskCacheMetricSPtr metric;
};
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_METRICS_DISK_CACHE_METRIC_H_
