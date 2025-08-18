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

#ifndef DINGOFS_SRC_CACHE_METRIC_COMMON_H_
#define DINGOFS_SRC_CACHE_METRIC_COMMON_H_

#include <bvar/latency_recorder.h>
#include <bvar/reducer.h>

namespace dingofs {
namespace cache {

struct PerSecondMetric {
  bvar::Adder<uint64_t> count;                   // total count
  bvar::PerSecond<bvar::Adder<uint64_t>> value;  // average count persecond

  PerSecondMetric(const std::string& prefix, const std::string& name)
      : count(prefix, "total_" + name),
        value(prefix, name + "_per_second", &count, 1) {}
};

// e.g.
//   dingofs_disk_cache_op_stage_total_op
//   dingofs_disk_cache_op_stage_op_per_second
//   dingofs_disk_cache_op_stage_total_bytes
//   dingofs_disk_cache_op_stage_bytes_per_second
//   dingofs_disk_cache_op_stage_total_error
//   dingofs_disk_cache_op_stage_error_per_second
struct OpMetric {
  OpMetric(const std::string& prefix)
      : op_per_second(prefix, "op"),
        bandwidth_per_second(prefix, "bytes"),
        error_per_second(prefix, "error"),
        latency(prefix, "lat"),
        total_letency(prefix, "total_lat") {}

  PerSecondMetric op_per_second;
  PerSecondMetric bandwidth_per_second;
  PerSecondMetric error_per_second;
  bvar::LatencyRecorder latency;
  bvar::Adder<uint64_t> total_letency;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_METRIC_COMMON_H_
