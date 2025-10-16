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

#include "common/status.h"
#include "metrics/metric.h"
#include "utils/timeutility.h"

namespace dingofs {
namespace metrics {

using ::dingofs::utils::TimeUtility;

template <typename T>
struct MetricGuard {
  explicit MetricGuard(T* rc, InterfaceMetric* metric, size_t count,
                       uint64_t start)
      : rc_(rc), metric_(metric), count_(count), start_(start) {}
  ~MetricGuard() {
    if (IsOk()) {
      metric_->bps.count << count_;
      metric_->qps.count << 1;
      auto duration = butil::cpuwide_time_us() - start_;
      metric_->latency << duration;
      metric_->latTotal << duration;
    } else {
      metric_->eps.count << 1;
    }
  }

 private:
  bool IsOk() const {
    if constexpr (std::is_same_v<T, Status>) {
      return rc_->ok();
    } else if constexpr (std::is_same_v<T, int>) {
      return *rc_ == 0;
    }
    return false;
  }
  T* rc_;
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

struct VFSRWMetricGuard {
  explicit VFSRWMetricGuard(Status* s, InterfaceMetric* metric, size_t* count,
                            bool enable = true)
      : s_(s),
        metric_(metric),
        count_(count),
        enable_(enable),
        start_(butil::cpuwide_time_us()) {}

  ~VFSRWMetricGuard() {
    if (!enable_) {
      return;
    }
    if (s_->ok()) {
      metric_->bps.count << *count_;
      metric_->qps.count << 1;
      auto duration = butil::cpuwide_time_us() - start_;
      metric_->latency << duration;
      metric_->latTotal << duration;
    } else {
      metric_->eps.count << 1;
    }
  }

  Status* s_;
  InterfaceMetric* metric_;
  size_t* count_;
  uint64_t start_;
  bool enable_;
};

struct ClientOpMetricGuard {
  explicit ClientOpMetricGuard(std::list<metrics::OpMetric*> metric_list,
                               bool enable = true)
      : metric_list_(metric_list),
        enable_(enable),
        start_(butil::cpuwide_time_us()) {
    if (!enable_) {
      return;
    }
    for (auto& metric : metric_list) {
      metric->inflightOpNum << 1;
    }
  }

  ~ClientOpMetricGuard() {
    if (!enable_) {
      return;
    }
    for (auto& metric : metric_list_) {
      metric->inflightOpNum << -1;
      if (op_ok_) {
        metric->qpsTotal << 1;
        auto duration = butil::cpuwide_time_us() - start_;
        metric->latency << duration;
        metric->latTotal << duration;
      } else {
        metric->ecount << 1;
      }
    }
  }

  void FailOp() { op_ok_ = false; }

  bool op_ok_{true};
  std::list<metrics::OpMetric*> metric_list_;
  uint64_t start_;
  bool enable_;
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
