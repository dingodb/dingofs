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

#ifndef DINGOFS_SRC_METRICS_METRIC_H_
#define DINGOFS_SRC_METRICS_METRIC_H_

#include <bvar/bvar.h>

#include <cstdint>
#include <string>

namespace dingofs {
namespace metrics {

// metric stats per second
struct PerSecondMetric {
  bvar::Adder<uint64_t> count;                   // total count
  bvar::PerSecond<bvar::Adder<uint64_t>> value;  // average count persecond

  PerSecondMetric(const std::string& prefix, const std::string& name)
      : count(prefix, name + "_total_count"), value(prefix, name, &count, 1) {}
};

// interface metric statistics
struct InterfaceMetric {
  PerSecondMetric qps;             // processed per second
  PerSecondMetric eps;             // error request per second
  PerSecondMetric bps;             // throughput with byte per second
  bvar::LatencyRecorder latency;   // latency
  bvar::Adder<uint64_t> latTotal;  // latency total value

  InterfaceMetric(const std::string& prefix, const std::string& name)
      : qps(prefix, name + "_qps"),
        eps(prefix, name + "_eps"),
        bps(prefix, name + "_bps"),
        latency(prefix, name + "_lat", 1),
        latTotal(prefix, name + "_lat_total_value") {}
};

struct OpMetric {
  bvar::LatencyRecorder latency;
  bvar::Adder<int64_t> inflightOpNum;
  bvar::Adder<uint64_t> ecount;
  bvar::Adder<uint64_t> qpsTotal;  // qps total count
  bvar::Adder<uint64_t> latTotal;  // latency total count

  explicit OpMetric(const std::string& prefix, const std::string& name)
      : latency(prefix, name + "_lat", 1),
        inflightOpNum(prefix, name + "_inflight_num"),
        ecount(prefix, name + "_error_num"),
        qpsTotal(prefix, name + "_qps_total_count"),
        latTotal(prefix, name + "_lat_total_value") {}
};

}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_METRIC_H_
