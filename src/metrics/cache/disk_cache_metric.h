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

#ifndef DINGOFS_SRC_METRICS_CACHE_DISK_CACHE_METRICS_H_
#define DINGOFS_SRC_METRICS_CACHE_DISK_CACHE_METRICS_H_

#include <absl/strings/str_format.h>
#include <bvar/reducer.h>
#include <bvar/status.h>

#include <string>

#include "options/cache/blockcache.h"

namespace dingofs {
namespace metrics {

class DiskCacheMetric {
 public:
  DiskCacheMetric(cache::DiskCacheOption option);

  void Expose(const std::string& prefix);

  void Reset();

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
  bvar::Status<bool> use_direct_write;

 private:
  cache::DiskCacheOption option_;
};

using DiskCacheMetricSPtr = std::shared_ptr<DiskCacheMetric>;

}  // namespace metrics
}  // namespace dingofs

/*
struct DiskCacheMetricGuard {
      explicit DiskCacheMetricGuard(Status* status,
                                    stub::metric::InterfaceMetric* metric,
                                    size_t count)
          : status(status), metric(metric), count(count) {
        start = butil::cpuwide_time_us();
      }

      ~DiskCacheMetricGuard() {
        if (status->ok()) {
          metric->bps.count << count;
          metric->qps.count << 1;
          auto duration = butil::cpuwide_time_us() - start;
          metric->latency << duration;
          metric->latTotal << duration;
        } else {
          metric->eps.count << 1;
        }
      }

      Status* status;
      stub::metric::InterfaceMetric* metric;
      size_t count;
      uint64_t start;
    };
*/

#endif  // DINGOFS_SRC_METRICS_CACHE_DISK_CACHE_METRICS_H_