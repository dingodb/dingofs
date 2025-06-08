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

#ifndef DINGOFS_SRC_CACHE_METRICS_DISK_CACHE_METRICS_H_
#define DINGOFS_SRC_CACHE_METRICS_DISK_CACHE_METRICS_H_

namespace dingofs {
namespace cache {

/*
constexpr const char* kLoadStopped = "STOP";  // load status
constexpr const char* kOnLoading = "LOADING";
constexpr const char* kLoadFinised = "FINISH";
constexpr const char* kCacheUp = "UP";  // cache status
constexpr const char* kCacheDown = "DOWN";

class DiskCacheMetric {
 public:
  explicit DiskCacheMetric(DiskCacheOption option) {}
: option_(option),
  metric_(base::string::StrFormat("dingofs_block_cache.disk_caches_%d",
                                  option.cache_index())) {}

virtual ~DiskCacheMetric() = default;

void Reset() {
  dir_.set_value(option_.cache_dir());
  used_bytes_.set_value(0);
  capacity_.set_value(option_.cache_size_mb() * base::math::kMiB);
  free_space_ratio_.set_value(option_.free_space_ratio());
  load_status_.set_value(kLoadStopped);
  running_status_.set_value(kCacheDown);
  healthy_status_.set_value("unknown");
  stage_skips_.reset();
  stage_blocks_.reset();
  stage_full_.set_value(false);
  cache_hits_.reset();
  cache_misses_.reset();
  cache_blocks_.reset();
  cache_bytes_.reset();
  cache_full_.set_value(false);
  use_direct_write_.set_value(false);
}

void SetUuid(const std::string& value) { uuid_.set_value(value); }

void SetLoadStatus(const std::string& value) { load_status_.set_value(value); }

std::string GetLoadStatus() const { return load_status_.get_value(); }

void SetRunningStatus(const std::string& value) {
  running_status_.set_value(value);
}

std::string GetRunningStatus() const { return running_status_.get_value(); }

void SetHealthyStatus(const std::string& value) {
  healthy_status_.set_value(value);
}

void SetUsedBytes(int64_t used_bytes) { used_bytes_.set_value(used_bytes); }

// stage
void AddStageBlock(int64_t n) { stage_blocks_ << n; }

void AddStageSkip() { stage_skips_ << 1; }

void SetStageFull(bool is_full) { stage_full_.set_value(is_full); }

// cache
void AddCacheHit() { cache_hits_ << 1; }

void AddCacheMiss() { cache_misses_ << 1; }

void AddCacheBlock(int64_t n, int64_t bytes) {
  cache_blocks_ << n;
  cache_bytes_ << bytes;
}

void SetCacheFull(bool is_full) { cache_full_.set_value(is_full); }

void SetUseDirectWrite(bool use_direct_write) {
  use_direct_write_.set_value(use_direct_write);
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

bvar::Status<std::string> uuid_;
bvar::Status<std::string> dir_;
bvar::Status<int64_t> used_bytes_;
bvar::Status<int64_t> capacity_;
bvar::Status<double> free_space_ratio_;
bvar::Status<std::string> load_status_;
bvar::Status<std::string> running_status_;
bvar::Status<std::string> healthy_status_;
bvar::Adder<int64_t> stage_skips_;  // stage
bvar::Adder<int64_t> stage_blocks_;
bvar::Status<bool> stage_full_;
bvar::Adder<int64_t> cache_hits_;  // cache
bvar::Adder<int64_t> cache_misses_;
bvar::Adder<int64_t> cache_blocks_;
bvar::Adder<int64_t> cache_bytes_;
bvar::Status<bool> cache_full_;
bvar::Status<bool> use_direct_write_;
};

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

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_METRICS_DISK_CACHE_METRICS_H_