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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_METRIC_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_METRIC_H_

#include <bvar/bvar.h>

#include <memory>
#include <string>
#include <type_traits>

#include "curvefs/src/base/string/string.h"
#include "curvefs/src/client/blockcache/error.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/common/dynamic_config.h"
#include "curvefs/src/client/metric/client_metric.h"

namespace curvefs {
namespace client {
namespace blockcache {

USING_FLAG(disk_cache_free_space_ratio);

using ::curvefs::base::string::StrFormat;
using ::curvefs::client::common::DiskCacheOption;
using ::curvefs::client::metric::InterfaceMetric;

constexpr const char* kLoadStopped = "STOP";  // load status
constexpr const char* kOnLoading = "LOADING";
constexpr const char* kLoadFinised = "FINISH";
constexpr const char* kCacheUp = "UP";  // cache status
constexpr const char* kCacheDown = "DOWN";

class DiskCacheMetric {
 public:
  explicit DiskCacheMetric(DiskCacheOption option)
      : option_(option),
        metric_(StrFormat("dingofs_block_cache.disk_caches_%d", option.index)) {
  }

  virtual ~DiskCacheMetric() = default;

  void Init() {
    metric_.dir.set_value(option_.cache_dir);
    metric_.used_bytes.set_value(0);
    metric_.capacity.set_value(option_.cache_size);
    metric_.free_space_ratio.set_value(FLAGS_disk_cache_free_space_ratio);
    metric_.load_status.set_value(kLoadStopped);
    metric_.cache_status.set_value(kCacheDown);
    metric_.stage_skips.reset();
    metric_.stage_blocks.reset();
    metric_.stage_full.set_value(false);
    metric_.cache_hits.reset();
    metric_.cache_misses.reset();
    metric_.cache_blocks.reset();
    metric_.cache_bytes.reset();
    metric_.cache_full.set_value(false);
  }

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

  // stage
  void AddStageBlock(int64_t n) { metric_.stage_blocks << n; }

  void AddStageSkip() { metric_.stage_skips << 1; }

  void SetStageFull(bool is_full) { metric_.stage_full.set_value(is_full); }

  // cache
  void AddCacheHit() { metric_.cache_hits << 1; }

  void AddCacheMiss() { metric_.cache_misses << 1; }

  void AddCacheBlock(int64_t n, int64_t bytes) {
    metric_.cache_blocks << n;
    metric_.cache_bytes << bytes;
  }

  void SetCacheFull(bool is_full) { metric_.cache_full.set_value(is_full); }

 private:
  struct Metric {
    Metric(const std::string& prefix) {
      uuid.expose_as(prefix, "uuid");
      dir.expose_as(prefix, "dir");
      used_bytes.expose_as(prefix, "used_bytes");
      capacity.expose_as(prefix, "capacity");
      free_space_ratio.expose_as(prefix, "free_space_ratio");
      load_status.expose_as(prefix, "load_status");
      cache_status.expose_as(prefix, "status");
      stage_skips.expose_as(prefix, "stage_skips");  // stage
      stage_blocks.expose_as(prefix, "stage_blocks");
      stage_full.expose_as(prefix, "stage_full");
      cache_hits.expose_as(prefix, "cache_hits");  // cache
      cache_misses.expose_as(prefix, "cache_misses");
      cache_blocks.expose_as(prefix, "cache_blocks");
      cache_bytes.expose_as(prefix, "cache_bytes");
      cache_full.expose_as(prefix, "cache_full");
    }

    bvar::Status<std::string> uuid;
    bvar::Status<std::string> dir;
    bvar::Status<int64_t> used_bytes;
    bvar::Status<int64_t> capacity;
    bvar::Status<double> free_space_ratio;
    bvar::Status<std::string> load_status;
    bvar::Status<std::string> cache_status;
    bvar::Adder<int64_t> stage_skips;  // stage
    bvar::Adder<int64_t> stage_blocks;
    bvar::Status<bool> stage_full;
    bvar::Adder<int64_t> cache_hits;  // cache
    bvar::Adder<int64_t> cache_misses;
    bvar::Adder<int64_t> cache_blocks;
    bvar::Adder<int64_t> cache_bytes;
    bvar::Status<bool> cache_full;
  };

  DiskCacheOption option_;
  Metric metric_;
};

struct DiskCacheMetricGuard {
  explicit DiskCacheMetricGuard(BCACHE_ERROR* rc, InterfaceMetric* metric,
                                size_t count)
      : rc_(rc), metric_(metric), count_(count) {
    start_ = butil::cpuwide_time_us();
  }

  ~DiskCacheMetricGuard() {
    if (*rc_ == BCACHE_ERROR::OK) {
      metric_->bps.count << count_;
      metric_->qps.count << 1;
      auto duration = butil::cpuwide_time_us() - start_;
      metric_->latency << duration;
      metric_->latTotal << duration;
    } else {
      metric_->eps.count << 1;
    }
  }

  BCACHE_ERROR* rc_;
  InterfaceMetric* metric_;
  size_t count_;
  uint64_t start_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_METRIC_H_
