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

#ifndef DINGOFS_METRICS_CLIENT_WARMUP_METRIC_H_
#define DINGOFS_METRICS_CLIENT_WARMUP_METRIC_H_

#include <bvar/bvar.h>

#include <cstdint>
#include <string>

namespace dingofs {
namespace metrics {
namespace client {

struct WarmupMetric {
  inline static const std::string prefix = "dingofs_vfs";

  bvar::Adder<uint64_t> inflight_warmup_tasks;
  bvar::Adder<uint64_t> inflight_warmup_files;
  bvar::Adder<uint64_t> inflight_warmup_blocks;

  WarmupMetric()
      : inflight_warmup_tasks(prefix, "inflight_warmup_tasks"),
        inflight_warmup_files(prefix, "inflight_warmup_files"),
        inflight_warmup_blocks(prefix, "inflight_warmup_blocks") {}
};

}  // namespace client
}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_METRICS_CLIENT_WARMUP_METRIC_H_