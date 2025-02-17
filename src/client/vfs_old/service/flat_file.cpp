
// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/vfs_old/service/flat_file.h"

namespace dingofs {
namespace client {

using pb::metaserver::S3ChunkInfo;

void FlatFileChunk::InsertChunkInfo(const FlatFileSlice& new_slice) {
  VLOG(6) << "Will insert new slice, " << new_slice.ToString();

  std::vector<FlatFileSlice> overlap_slices;
  auto start_iter = file_offset_slice_.lower_bound(new_slice.file_offset);

  // find overlap slices which slice end > new_slice file_offset
  if (start_iter != file_offset_slice_.begin()) {
    start_iter--;
    CHECK(start_iter->second.file_offset < new_slice.file_offset)
        << "expect " << start_iter->second.file_offset << " < "
        << new_slice.file_offset;

    if (start_iter->second.file_offset + start_iter->second.len >
        new_slice.file_offset) {
      overlap_slices.push_back(start_iter->second);
    }

    start_iter++;
  }

  // find slices which slice start < new_slice end (file_offset + len)
  while (start_iter != file_offset_slice_.end() &&
         start_iter->second.file_offset <
             (new_slice.file_offset + new_slice.len)) {
    overlap_slices.push_back(start_iter->second);
    start_iter++;
  }

  // remove overlap slices
  for (const auto& overlap_slice : overlap_slices) {
    VLOG(6) << "Remove overlap slice, " << overlap_slice.ToString();
    file_offset_slice_.erase(overlap_slice.file_offset);
  }

  if (!overlap_slices.empty()) {
    {
      // process first overlap slice
      FlatFileSlice overlap_slice = overlap_slices[0];

      if (overlap_slice.file_offset < new_slice.file_offset) {
        // insert left slice
        uint64_t file_offset = overlap_slice.file_offset;
        uint64_t len = new_slice.file_offset - overlap_slice.file_offset;

        FlatFileSlice left_slice = {file_offset, len, overlap_slice.chunk_id};
        VLOG(6) << "Insert left slice, " << left_slice.ToString();
        CHECK(file_offset_slice_.insert(std::make_pair(file_offset, left_slice))
                  .second);
      }
    }

    {
      // process last overlap slice
      FlatFileSlice overlap_slice = overlap_slices.back();

      if (overlap_slice.file_offset + overlap_slice.len >
          new_slice.file_offset + new_slice.len) {
        // insert right slice
        uint64_t file_offset = new_slice.file_offset + new_slice.len;
        uint64_t len = overlap_slice.file_offset + overlap_slice.len -
                       (new_slice.file_offset + new_slice.len);

        FlatFileSlice right_slice = {file_offset, len, overlap_slice.chunk_id};
        VLOG(6) << "Insert right slice, " << right_slice.ToString();
        CHECK(
            file_offset_slice_.insert(std::make_pair(file_offset, right_slice))
                .second);
      }
    }
  }

  // insert new slice
  VLOG(6) << "Insert new slice, " << new_slice.ToString();
  CHECK(file_offset_slice_
            .insert(std::make_pair(new_slice.file_offset, new_slice))
            .second);
}

}  // namespace client
}  // namespace dingofs
