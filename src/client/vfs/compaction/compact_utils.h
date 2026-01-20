/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGODB_CLIENT_VFS_COMPACTION_COMPACT_UTILS_H_
#define DINGODB_CLIENT_VFS_COMPACTION_COMPACT_UTILS_H_

#include <absl/types/span.h>
#include <glog/logging.h>

#include <cstdint>

#include "client/vfs/data/common/common.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace compaction {

FileRange GetSlicesFileRange(absl::Span<const Slice> slices);

int64_t SliceReadReqsLength(const std::vector<SliceReadReq>& reqs);

int32_t Skip(const std::vector<Slice>& slices);

}  // namespace compaction
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_COMPACTION_COMPACT_UTILS_H_
