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

#ifndef DINGOFS_SRC_CACHE_COMMON_VARS_H_
#define DINGOFS_SRC_CACHE_COMMON_VARS_H_

#include <bvar/latency_recorder.h>
#include <bvar/reducer.h>

namespace dingofs {
namespace cache {

// e.g.
// PerSecondVar(dingofs_disk_cache_op_stage, error) will create below vars:
//   dingofs_disk_cache_op_stage_total_error
//   dingofs_disk_cache_op_stage_error_per_second
struct PerSecondVar {
  PerSecondVar(const std::string& opname, const std::string& itemname)
      : total_count(opname, "total_" + itemname),
        per_second(opname, itemname + "_per_second", &total_count, 1) {}

  bvar::Adder<uint64_t> total_count;                  // total count
  bvar::PerSecond<bvar::Adder<uint64_t>> per_second;  // average count persecond
};

// e.g.
// OpVar(dingofs_disk_cache_op_stage) will create below vars:
//   dingofs_disk_cache_op_stage_total_op
//   dingofs_disk_cache_op_stage_op_per_second
//   dingofs_disk_cache_op_stage_total_bytes
//   dingofs_disk_cache_op_stage_bytes_per_second
//   dingofs_disk_cache_op_stage_total_error
//   dingofs_disk_cache_op_stage_error_per_second
//   dingofs_disk_cache_op_stage_lat
//   dingofs_disk_cache_op_stage_total_lat
struct OpVar {
  OpVar(const std::string& opname)
      : op_per_second(opname, "op"),
        bandwidth_per_second(opname, "bytes"),
        error_per_second(opname, "error"),
        latency(opname, "lat"),
        total_latency(opname, "total_lat") {}

  PerSecondVar op_per_second;
  PerSecondVar bandwidth_per_second;
  PerSecondVar error_per_second;
  bvar::LatencyRecorder latency;
  bvar::Adder<uint64_t> total_latency;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_VARS_H_
