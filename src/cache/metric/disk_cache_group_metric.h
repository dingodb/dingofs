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
 * Created Date: 2025-07-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_METRIC_DISK_CACHE_GROUP_METRIC_H_
#define DINGOFS_SRC_CACHE_METRIC_DISK_CACHE_GROUP_METRIC_H_

#include <absl/strings/str_format.h>
#include <bvar/latency_recorder.h>

#include "cache/metric/common.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

struct DiskCacheGroupMetric {
  DiskCacheGroupMetric() = default;

  inline static const std::string prefix = "dingofs_disk_cache_group";

  OpMetric op_stage{absl::StrFormat("%s_%s", prefix, "stage")};
  OpMetric op_cache{absl::StrFormat("%s_%s", prefix, "cache")};
  OpMetric op_load{absl::StrFormat("%s_%s", prefix, "load")};
};

using DiskCacheGroupMetricSPtr = std::shared_ptr<DiskCacheGroupMetric>;

struct DiskCacheGroupMetricGuard {
  DiskCacheGroupMetricGuard(const std::string& op_name, size_t bytes,
                            Status& status, DiskCacheGroupMetricSPtr metric)
      : op_name(op_name), bytes(bytes), status(status), metric(metric) {
    CHECK(op_name == "Stage" || op_name == "Cache" || op_name == "Load")
        << "Invalid operation name: " << op_name;
    timer.start();
  }

  ~DiskCacheGroupMetricGuard() {
    timer.stop();

    OpMetric* op;
    if (op_name == "Stage") {
      op = &metric->op_stage;
    } else if (op_name == "Cache") {
      op = &metric->op_cache;
    } else if (op_name == "Load") {
      op = &metric->op_load;
    }

    if (status.ok()) {
      op->op_per_second.count << 1;
      op->bandwidth_per_second.count << bytes;
      op->latency << timer.u_elapsed();
      op->total_letency << timer.u_elapsed();
    } else {
      op->error_per_second.count << 1;
    }
  }

  std::string op_name;
  size_t bytes;
  Status& status;
  butil::Timer timer;
  DiskCacheGroupMetricSPtr metric;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_METRIC_DISK_CACHE_GROUP_METRIC_H_
