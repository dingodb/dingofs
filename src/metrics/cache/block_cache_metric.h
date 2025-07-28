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
 * Created Date: 2024-09-23
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_METRICS_CACHE_BLOCK_CACHE_METRIC_H_
#define DINGOFS_SRC_METRICS_CACHE_BLOCK_CACHE_METRIC_H_

#include <bvar/passive_status.h>

namespace dingofs {
namespace cache {

struct BlockCacheMetric {
  static BlockCacheMetric& GetInstance() {
    static BlockCacheMetric instance;
    return instance;
  }
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_CACHE_BLOCK_CACHE_METRIC_H_
