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

#ifndef DINGOFS_SRC_METRICS_CLIENT_VFS_LEGACY_WARMUP_H_
#define DINGOFS_SRC_METRICS_CLIENT_VFS_LEGACY_WARMUP_H_

#include <bvar/bvar.h>

#include <string>

#include "metrics/metric.h"

namespace dingofs {
namespace metrics {
namespace client {
namespace vfs_legacy {

struct WarmupManagerBlockMetric {
  inline static const std::string prefix = "dingofs_warmup";

  InterfaceMetric warmupBlockCached;
  bvar::Adder<uint64_t> warmupBlockCacheSize;

  WarmupManagerBlockMetric()
      : warmupBlockCached(prefix, "block_cached"),
        warmupBlockCacheSize(prefix, "block_cache_size") {}
};

}  // namespace vfs_legacy
}  // namespace client
}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_CLIENT_VFS_LEGACY_WARMUP_H_
