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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_METRIC_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_METRIC_H_

#include <bvar/bvar.h>

#include <memory>
#include <string>
#include <type_traits>

#include "curvefs/src/base/string/string.h"
#include "curvefs/src/client/blockcache/error.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/common/dynamic_config.h"

namespace curvefs {
namespace client {
namespace blockcache {

USING_FLAG(disk_cache_free_space_ratio);

using ::curvefs::base::string::StrFormat;
using ::curvefs::client::common::DiskCacheOption;

constexpr const char* kStopped = "stopped";  // load status
constexpr const char* kOnLoading = "loading";
constexpr const char* kLoadFinised = "finished";
constexpr const char* kCacheUp = "UP";  // cache status
constexpr const char* kCacheDown = "DOWN";

class DiskCacheMetric {
 public:
  explicit DiskCacheMetric(DiskCacheOption option)
      : metric_(StrFormat("block_cache.disk_caches_%d", option.index)) {
    metric_.dir.set_value(option.cache_dir);
    metric_.capacity.set_value(option.cache_size);
    metric_.free_space_ratio.set_value(FLAGS_disk_cache_free_space_ratio);
    metric_.load_status.set_value(kStopped);
    metric_.cache_status.set_value(kCacheDown);
  }

  virtual ~DiskCacheMetric() = default;

  void SetUuid(const std::string& value) { metric_.uuid.set_value(value); }

  void SetLoadStatus(const std::string& value) {
    metric_.load_status.set_value(value);
  }

  std::string GetLoadStatus() const { return metric_.load_status.get_value(); }

  void SetCacheStatus(const std::string& value) {
    metric_.cache_status.set_value(value);
  }

  std::string GetCacheStatus() const {
    return metric_.cache_status.get_value();
  }

  void SetUsedBytes(int64_t used_bytes) {
    metric_.used_bytes.set_value(used_bytes);
  }

  void AddStageBlock(int64_t n) { metric_.stage_blocks << n; }

  void AddStageSkip() { metric_.stage_skips << 1; }

  void AddCacheHit() { metric_.cache_hits << 1; }

  void AddCacheMiss() { metric_.cache_misses << 1; }

  void AddCacheBlock(int64_t n, int64_t bytes) {
    metric_.cache_blocks << n;
    metric_.cache_bytes << bytes;
  }

 private:
  struct Metric {
    Metric(const std::string& prefix)
        : used_bytes(prefix, "used_bytes", 0),
          capacity(prefix, "capacity", 0),
          free_space_ratio(prefix, "free_space_ratio", 0),
          stage_skips(prefix, "stage_skips"),  // stage
          stage_blocks(prefix, "stage_blocks"),
          cache_hits(prefix, "cache_hits"),  // cache
          cache_misses(prefix, "cache_misses"),
          cache_blocks(prefix, "cache_blocks"),
          cache_bytes(prefix, "cache_bytes") {
      uuid.expose_as(prefix, "uuid");
      dir.expose_as(prefix, "dir");
      load_status.expose_as(prefix, "load_status");
      cache_status.expose_as(prefix, "status");
    }

    bvar::Status<std::string> uuid;
    bvar::Status<std::string> dir;
    bvar::Status<int64_t> used_bytes;
    bvar::Status<int64_t> capacity;
    bvar::Status<double> free_space_ratio;
    bvar::Status<std::string> load_status;   // loading/finished
    bvar::Status<std::string> cache_status;  // UP/DOWN
    bvar::Adder<int64_t> stage_skips;        // stage
    bvar::Adder<int64_t> stage_blocks;
    bvar::Adder<int64_t> cache_hits;  // cache
    bvar::Adder<int64_t> cache_misses;
    bvar::Adder<int64_t> cache_blocks;
    bvar::Adder<int64_t> cache_bytes;
  };

  Metric metric_;
};

using MetricHandler = std::function<void(DiskCacheMetric* metric)>;

struct MetricGuard {
  MetricGuard(std::shared_ptr<DiskCacheMetric> metric, MetricHandler handler)
      : metric(metric), handler(handler){};

  ~MetricGuard() { handler(metric.get()); }

  std::shared_ptr<DiskCacheMetric> metric;
  MetricHandler handler;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_METRIC_H_
