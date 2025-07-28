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
 * Created Date: 2025-07-28
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_METRICS_CACHE_REMOTE_BLOCK_CACHE_METRIC_H_
#define DINGOFS_SRC_METRICS_CACHE_REMOTE_BLOCK_CACHE_METRIC_H_

#include <bvar/passive_status.h>
#include <bvar/reducer.h>

namespace dingofs {
namespace cache {

class RemoteBlockCacheMetric {
 public:
  static RemoteBlockCacheMetric& Instance() {
    static RemoteBlockCacheMetric instance;
    return instance;
  }

  static void Init() { Instance(); }

  static void AddCacheHit(uint64_t n) {
    Instance().metric_.cache_hit_count << n;
  }

  static void AddCacheMiss(uint64_t n) {
    Instance().metric_.cache_miss_count << n;
  }

 private:
  static double GetCacheHitRatio(void* arg) {
    auto* metric = reinterpret_cast<Metric*>(arg);
    const double hit = metric->cache_hit_count_per_second.get_value();
    const double miss = metric->cache_miss_count_per_second.get_value();
    const double total = hit + miss;
    return total > 0 ? (hit * 100.0 / total) : 0.0;
  }

  struct Metric {
    Metric()
        : cache_hit_count(prefix, "hit_count"),
          cache_miss_count(prefix, "miss_count"),
          cache_hit_count_per_second(prefix, "hit_count_per_second",
                                     &cache_hit_count, 1),
          cache_miss_count_per_second(prefix, "miss_count_per_second",
                                      &cache_miss_count, 1),
          cache_hit_ratio(prefix, "hit_ratio", GetCacheHitRatio, this) {}

    inline static const std::string prefix = "dingofs_remote_cache_";

    bvar::Adder<uint64_t> cache_hit_count;
    bvar::Adder<uint64_t> cache_miss_count;
    bvar::PerSecond<bvar::Adder<uint64_t>> cache_hit_count_per_second;
    bvar::PerSecond<bvar::Adder<uint64_t>> cache_miss_count_per_second;
    bvar::PassiveStatus<double> cache_hit_ratio;
  };

  Metric metric_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_CACHE_REMOTE_BLOCK_CACHE_METRIC_H_
