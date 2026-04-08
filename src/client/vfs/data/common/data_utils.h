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

#ifndef DINGODB_CLIENT_VFS_DATA_UITLS_H_
#define DINGODB_CLIENT_VFS_DATA_UITLS_H_

#include <absl/types/span.h>
#include <glog/logging.h>

#include <boost/range/algorithm/sort.hpp>
#include <cstdint>
#include <vector>

#include "client/vfs/data/common/common.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

void DumpSliceReadReqs(const std::vector<SliceReadReq>& results);

std::vector<SliceReadReq> Convert2SliceReadReq(
    absl::Span<const Slice> slices, const FileRange& file_range_req,
    int64_t chunk_start);

std::vector<SliceReadReq> ProcessReadRequest(
    const std::vector<Slice>& slices, const FileRange& file_range_req,
    int64_t chunk_start);

std::vector<BlockReadReq> ConvertSliceReadReqToBlockReadReqs(
    const SliceReadReq& slice_req, uint32_t fs_id, uint64_t ino,
    int32_t chunk_size, int32_t block_size, int64_t chunk_start);

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_DATA_UITLS_H_