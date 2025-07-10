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

#ifndef DINGOFS_SRC_METRICS_CLIENT_VFS_LEGACY_DIR_CACHE_H_
#define DINGOFS_SRC_METRICS_CLIENT_VFS_LEGACY_DIR_CACHE_H_

#include <bvar/bvar.h>

namespace dingofs {
namespace metrics {
namespace client {
namespace vfs_legacy {

class DirCacheMetric {
 public:
  DirCacheMetric() = default;

  void AddEntries(int64_t n) { metric_.nentries << n; }

 private:
  struct Metric {
    Metric() : nentries("filesystem_dircache", "nentries") {}
    bvar::Adder<int64_t> nentries;
  };

  Metric metric_;
};

}  // namespace vfs_legacy
}  // namespace client
}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_CLIENT_VFS_LEGACY_DIR_CACHE_H_
