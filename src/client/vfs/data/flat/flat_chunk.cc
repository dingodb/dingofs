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

#include "client/vfs/data/flat/flat_chunk.h"

#include "client/vfs/data/common/data_utils.h"

namespace dingofs {
namespace client {
namespace vfs {

std::vector<BlockReadReq> FlatFileChunk::GenBlockReadReqs() const {
  std::vector<BlockReadReq> block_reqs;

  FileRange file_range = {.offset = (int64_t)(index_ * chunk_size_),
                          .len = chunk_size_};

  std::vector<SliceReadReq> slice_reqs =
      ProcessReadRequest(chunk_slices_, file_range);

  for (const auto& slice_req : slice_reqs) {
    if (slice_req.slice.has_value() && !slice_req.slice->is_zero) {
      std::vector<BlockReadReq> reqs = ConvertSliceReadReqToBlockReadReqs(
          slice_req, fs_id_, ino_, chunk_size_, block_size_);

      block_reqs.insert(block_reqs.end(), std::make_move_iterator(reqs.begin()),
                        std::make_move_iterator(reqs.end()));
    } else {
      VLOG(1) << "Slice is zero or not available, skipping convert, slice_req: "
              << slice_req.ToString();
    }
  }

  return block_reqs;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs