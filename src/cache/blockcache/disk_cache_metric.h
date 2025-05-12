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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_METRIC_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_METRIC_H_

#include <bvar/bvar.h>

#include <string>

#include "base/math/math.h"
#include "base/string/string.h"
#include "cache/common/common.h"
#include "options/cache/block_cache.h"
#include "stub/metric/metric.h"
#include "utils/dingo_define.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using dingofs::base::math::kMiB;
using dingofs::base::string::StrFormat;
using dingofs::options::cache::DiskCacheOption;
using dingofs::stub::metric::InterfaceMetric;

constexpr const char* kLoadStopped = "STOP";  // load status
constexpr const char* kOnLoading = "LOADING";
constexpr const char* kLoadFinised = "FINISH";
constexpr const char* kCacheUp = "UP";  // cache status
constexpr const char* kCacheDown = "DOWN";

class DiskCacheMetric {
 public:
  explicit DiskCacheMetric(DiskCacheOption option)
      : option_(option),
        metric_(StrFormat("dingofs_block_cache.disk_caches_%d",
                          option.cache_index())) {}

  virtual ~DiskCacheMetric() = default;

  void Init() {
    metric_.dir.set_value(option_.cache_dir());
    metric_.used_bytes.set_value(0);
    metric_.capacity.set_value(option_.cache_size_mb() * kMiB);
    metric_.free_space_ratio.set_value(option_.free_space_ratio());
    metric_.load_status.set_value(kLoadStopped);
    metric_.running_status.set_value(kCacheDown);
    metric_.healthy_status.set_value("unknown");
    metric_.stage_skips.reset();
    metric_.stage_blocks.reset();
    metric_.stage_full.set_value(false);
    metric_.cache_hits.reset();
    metric_.cache_misses.reset();
    metric_.cache_blocks.reset();
    metric_.cache_bytes.reset();
    metric_.cache_full.set_value(false);
    metric_.use_direct_write.set_value(false);
  }

  void SetUuid(const std::string& value) { metric_.uuid.set_value(value); }

  void SetLoadStatus(const std::string& value) {
    metric_.load_status.set_value(value);
  }

  std::string GetLoadStatus() const { return metric_.load_status.get_value(); }

  void SetRunningStatus(const std::string& value) {
    metric_.running_status.set_value(value);
  }

  std::string GetRunningStatus() const {
    return metric_.running_status.get_value();
  }

  void SetHealthyStatus(const std::string& value) {
    metric_.healthy_status.set_value(value);
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

  void SetUseDirectWrite(bool use_direct_write) {
    metric_.use_direct_write.set_value(use_direct_write);
  }

 private:
  struct Metric {
    Metric(const std::string& prefix) {
      uuid.expose_as(prefix, "uuid");
      dir.expose_as(prefix, "dir");
      used_bytes.expose_as(prefix, "used_bytes");
      capacity.expose_as(prefix, "capacity");
      free_space_ratio.expose_as(prefix, "free_space_ratio");
      load_status.expose_as(prefix, "load_status");
      running_status.expose_as(prefix, "running_status");
      healthy_status.expose_as(prefix, "healthy_status");
      stage_skips.expose_as(prefix, "stage_skips");  // stage
      stage_blocks.expose_as(prefix, "stage_blocks");
      stage_full.expose_as(prefix, "stage_full");
      cache_hits.expose_as(prefix, "cache_hits");  // cache
      cache_misses.expose_as(prefix, "cache_misses");
      cache_blocks.expose_as(prefix, "cache_blocks");
      cache_bytes.expose_as(prefix, "cache_bytes");
      cache_full.expose_as(prefix, "cache_full");
      use_direct_write.expose_as(prefix, "use_direct_write");
    }

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
  };

  DiskCacheOption option_;
  Metric metric_;
};

struct DiskCacheMetricGuard {
  explicit DiskCacheMetricGuard(Status* status, InterfaceMetric* metric,
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
  InterfaceMetric* metric;
  size_t count;
  uint64_t start;
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_METRIC_H_
