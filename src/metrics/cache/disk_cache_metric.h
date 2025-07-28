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

#ifndef DINGOFS_SRC_METRICS_CACHE_DISK_CACHE_METRIC_H_
#define DINGOFS_SRC_METRICS_CACHE_DISK_CACHE_METRIC_H_

#include <absl/strings/str_format.h>
#include <bvar/reducer.h>
#include <bvar/status.h>
#include <glog/logging.h>

#include <string>

#include "common/status.h"
#include "options/cache/blockcache.h"

namespace dingofs {
namespace cache {

struct DiskCacheMetric {
  DiskCacheMetric(cache::DiskCacheOption option);

  void Expose(const std::string& prefix);
  void Init();
  void Reset();

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

  cache::DiskCacheOption option;
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

#endif  // DINGOFS_SRC_METRICS_CACHE_DISK_CACHE_METRIC_H_
