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
 * Created Date: 2025-02-17
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_METRICS_CACHE_GROUP_NODE_METRIC_H_
#define DINGOFS_SRC_CACHE_METRICS_CACHE_GROUP_NODE_METRIC_H_

#include <bvar/bvar.h>

namespace dingofs {
namespace cache {

struct CacheGroupNodeMetric {
  static CacheGroupNodeMetric& GetInstance() {
    static CacheGroupNodeMetric instance;
    return instance;
  }

  void AddCacheHit() { cache_hit_count << 1; }
  void AddCacheMiss() { cache_miss_count << 1; }
  int64_t GetCacheHit() { return cache_hit_count.get_value(); }
  int64_t GetCacheMiss() { return cache_miss_count.get_value(); }

  bvar::Adder<int64_t> cache_hit_count{"cache_hit_count"};
  bvar::Adder<int64_t> cache_miss_count{"cache_miss_count"};
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_METRICS_CACHE_GROUP_NODE_METRIC_H_
