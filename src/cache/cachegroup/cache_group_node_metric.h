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

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_METRIC_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_METRIC_H_

#include <bvar/bvar.h>

namespace dingofs {
namespace cache {
namespace cachegroup {

class CacheGroupNodeMetric {
 public:
  CacheGroupNodeMetric() : metric_("dingofs_cache_group_node") {}

  void AddCacheHit() { metric_.cache_hits << 1; }
  void AddCacheMiss() { metric_.cache_misses << 1; }
  uint64_t GetCacheHit() { return metric_.cache_hits.get_value(); }
  uint64_t GetCacheMiss() { return metric_.cache_misses.get_value(); }

 private:
  struct Metric {
    Metric(const std::string& prefix) {
      cache_hits.expose_as(prefix, "cache_hits");
      cache_misses.expose_as(prefix, "cache_misses");
    }

    bvar::Adder<int64_t> cache_hits;
    bvar::Adder<int64_t> cache_misses;
  };

  Metric metric_;
};

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_METRIC_H_
