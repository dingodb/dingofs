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
 * Created Date: 2025-07-20
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_METRICS_CACHE_REMOTE_CACHE_NODE_GROUP_METRIC_H_
#define DINGOFS_SRC_METRICS_CACHE_REMOTE_CACHE_NODE_GROUP_METRIC_H_

#include <absl/strings/str_format.h>
#include <bvar/latency_recorder.h>

#include "common/status.h"
#include "metrics/cache/common/common.h"

namespace dingofs {
namespace cache {

struct RemoteCacheCacheNodeGroupMetric {
  RemoteCacheCacheNodeGroupMetric() = default;

  inline static const std::string prefix = "dingofs_remote_node_group";

  OpMetric op_put{absl::StrFormat("%s_%s", prefix, "put")};
  OpMetric op_range{absl::StrFormat("%s_%s", prefix, "range")};
  OpMetric op_cache{absl::StrFormat("%s_%s", prefix, "cache")};
  OpMetric op_prefetch{absl::StrFormat("%s_%s", prefix, "prefetch")};
};

using RemoteCacheCacheNodeGroupMetricSPtr =
    std::shared_ptr<RemoteCacheCacheNodeGroupMetric>;

struct RemoteCacheCacheNodeGroupMetricGuard {
  RemoteCacheCacheNodeGroupMetricGuard(
      const std::string& op_name, size_t bytes, Status& status,
      RemoteCacheCacheNodeGroupMetricSPtr metric)
      : op_name(op_name), bytes(bytes), status(status), metric(metric) {
    CHECK(op_name == "Put" || op_name == "Range" || op_name == "Cache" ||
          op_name == "Prefetch")
        << "Invalid operation name: " << op_name;
    timer.start();
  }

  ~RemoteCacheCacheNodeGroupMetricGuard() {
    timer.stop();

    OpMetric* op;
    if (op_name == "Put") {
      op = &metric->op_put;
    } else if (op_name == "Range") {
      op = &metric->op_range;
    } else if (op_name == "Cache") {
      op = &metric->op_cache;
    } else if (op_name == "Prefetch") {
      op = &metric->op_prefetch;
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
  RemoteCacheCacheNodeGroupMetricSPtr metric;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_CACHE_REMOTE_CACHE_NODE_GROUP_METRIC_H_
