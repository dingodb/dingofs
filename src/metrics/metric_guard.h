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

#ifndef DINGOFS_SRC_METRICS_METRIC_GUARD_H_
#define DINGOFS_SRC_METRICS_METRIC_GUARD_H_

#include <bvar/bvar.h>

#include <cstdint>

#include "metrics/metric.h"
#include "utils/timeutility.h"

namespace dingofs {
namespace metrics {

using ::dingofs::utils::TimeUtility;

struct MetricGuard {
  explicit MetricGuard(int* rc, InterfaceMetric* metric, size_t count,
                       uint64_t start)
      : rc_(rc), metric_(metric), count_(count), start_(start) {}
  ~MetricGuard() {
    if (*rc_ == 0) {
      metric_->bps.count << count_;
      metric_->qps.count << 1;
      auto duration = butil::cpuwide_time_us() - start_;
      metric_->latency << duration;
      metric_->latTotal << duration;
    } else {
      metric_->eps.count << 1;
    }
  }
  int* rc_;
  InterfaceMetric* metric_;
  size_t count_;
  uint64_t start_;
};

// metric guard for one or more metrics collection
struct MetricListGuard {
  explicit MetricListGuard(bool* rc, std::list<InterfaceMetric*> metricList,
                           uint64_t start)
      : rc_(rc), metricList_(metricList), start_(start) {}
  ~MetricListGuard() {
    for (auto& metric_ : metricList_) {
      auto duration = butil::cpuwide_time_us() - start_;
      if (*rc_) {
        metric_->qps.count << 1;
        metric_->latency << duration;
        metric_->latTotal << duration;
      } else {
        metric_->eps.count << 1;
      }
    }
  }
  bool* rc_;
  std::list<InterfaceMetric*> metricList_;
  uint64_t start_;
};

struct FsMetricGuard {
  explicit FsMetricGuard(InterfaceMetric* metric, size_t* count)
      : metric(metric), count(count), start(butil::cpuwide_time_us()) {}

  ~FsMetricGuard() {
    if (!fail) {
      metric->bps.count << *count;
      metric->qps.count << 1;
      auto duration = butil::cpuwide_time_us() - start;
      metric->latency << duration;
      metric->latTotal << duration;
    } else {
      metric->eps.count << 1;
    }
  }

  void Fail() { fail = true; }

  bool fail{false};
  InterfaceMetric* metric;
  size_t* count;
  uint64_t start;
};

struct ClientOpMetricGuard {
  explicit ClientOpMetricGuard(std::list<metrics::OpMetric*> p_metric_list)
      : metric_list(p_metric_list), start(butil::cpuwide_time_us()) {
    for (auto& metric : metric_list) {
      metric->inflightOpNum << 1;
    }
  }

  ~ClientOpMetricGuard() {
    for (auto& metric : metric_list) {
      metric->inflightOpNum << -1;
      if (op_ok) {
        metric->qpsTotal << 1;
        auto duration = butil::cpuwide_time_us() - start;
        metric->latency << duration;
        metric->latTotal << duration;
      } else {
        metric->ecount << 1;
      }
    }
  }

  void FailOp() { op_ok = false; }

  bool op_ok{true};
  std::list<metrics::OpMetric*> metric_list;
  uint64_t start;
};

struct LatencyGuard {
  bvar::LatencyRecorder* latencyRec;
  uint64_t startTimeUs;

  explicit LatencyGuard(bvar::LatencyRecorder* latency) {
    latencyRec = latency;
    startTimeUs = TimeUtility::GetTimeofDayUs();
  }

  ~LatencyGuard() {
    *latencyRec << (TimeUtility::GetTimeofDayUs() - startTimeUs);
  }
};

}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_METRIC_GUARD_H_
