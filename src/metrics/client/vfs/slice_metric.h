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

#ifndef DINGOFS_METRICS_CLIENT_SLICE_METRIC_H_
#define DINGOFS_METRICS_CLIENT_SLICE_METRIC_H_

#include <bvar/bvar.h>

#include <string>

#include "common/status.h"
#include "metrics/metric.h"

namespace dingofs {
namespace metrics {
namespace client {

struct SliceMetric {
  inline static const std::string prefix = "dingofs_vfs";

  OpMetric new_sliceid;
  OpMetric read_slice;
  OpMetric write_slice;

  SliceMetric()
      : new_sliceid(prefix, "new_sliceid"),
        read_slice(prefix, "read_slice"),
        write_slice(prefix, "write_slice") {}
};

struct SliceMetricGuard {
  explicit SliceMetricGuard(const Status* status, OpMetric* metric,
                            const uint64_t start)
      : status_(status), metric_(metric), start_(start) {
    metric_->inflightOpNum << 1;
  }

  ~SliceMetricGuard() {
    metric_->inflightOpNum << -1;
    if (status_->ok()) {
      metric_->qpsTotal << 1;
      auto duration = butil::cpuwide_time_us() - start_;
      metric_->latency << duration;
      metric_->latTotal << duration;
    } else {
      metric_->ecount << 1;
    }
  }

  const Status* status_;
  OpMetric* metric_;
  const uint64_t start_;
};

}  // namespace client
}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_METRICS_CLIENT_SLICE_METRIC_H_