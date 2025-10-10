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

#include <glog/logging.h>

#include <algorithm>
#include <boost/range/algorithm/sort.hpp>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "client/vfs/common/helper.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

static void DumpSliceReadReqs(const std::vector<SliceReadReq>& results) {
  for (const auto& req : results) {
    LOG(INFO) << "SliceReadReq:  " << req.ToString();
  }
}

static inline std::vector<SliceReadReq> ProcessReadRequest(
    const std::vector<Slice>& slices, const FileRange& file_range_req) {
  VLOG(9) << "ProcessReadRequest: input_file_range_req: "
          << file_range_req.ToString();
  std::vector<SliceReadReq> results;
  std::vector<FileRange> unmatched_ranges = {file_range_req};

  // TODO: maybe sort slices by id in descending order
  // then iter from newest to oldest
  //   auto sorted_slices = slices;
  //   std::sort(sorted_slices.begin(), sorted_slices.end(),
  //             [](const Slice& a, const Slice& b) { return a.id > b.id; });

  // loop from newest slice to oldest slice
  for (auto it = slices.rbegin(); it != slices.rend(); ++it) {
    const auto& slice = *it;
    VLOG(9) << "ProcessReadRequest: slice: " << Slice2Str(slice);

    std::vector<FileRange> new_unmatched_ranges;

    for (const auto& file_range : unmatched_ranges) {
      VLOG(9) << "ProcessReadRequest: file_range: " << file_range.ToString();

      // check if the current range overlaps with the slice
      if (slice.End() <= file_range.offset ||
          slice.offset >= file_range.End()) {
        new_unmatched_ranges.push_back(file_range);
        VLOG(9)
            << "ProcessReadRequest: file_range_req no overlap with slice_id: "
            << slice.id;
        continue;
      }

      // calculate the overlapping part
      uint64_t overlap_start = std::max(file_range.offset, slice.offset);
      uint64_t overlap_end = std::min(file_range.End(), slice.End());
      uint64_t overlap_len = overlap_end - overlap_start;

      SliceReadReq slice_read_req{
          .file_offset = overlap_start,
          .len = overlap_len,
          .slice = slice,
      };

      VLOG(9) << "ProcessReadRequest: slice_read_req: "
              << slice_read_req.ToString();

      results.push_back(slice_read_req);

      // process the left part of the range that is not covered by the slice
      if (file_range.offset < overlap_start) {
        FileRange left_uncovered_file_range{
            .offset = file_range.offset,
            .len = (overlap_start - file_range.offset),
        };
        VLOG(9) << "ProcessReadRequest: left_uncovered_file_range: "
                << left_uncovered_file_range.ToString();
        new_unmatched_ranges.push_back(left_uncovered_file_range);
      }

      // process the right part of the range that is not covered by the slice
      if (file_range.End() > overlap_end) {
        FileRange right_uncovered_file_range{
            .offset = overlap_end,
            .len = (file_range.End() - overlap_end),
        };
        VLOG(9) << "ProcessReadRequest: right_uncovered_file_range: "
                << right_uncovered_file_range.ToString();

        new_unmatched_ranges.push_back(right_uncovered_file_range);
      }
    }
    // End of for loop for unmatched_ranges

    unmatched_ranges = std::move(new_unmatched_ranges);

    if (unmatched_ranges.empty()) {
      VLOG(9) << "ProcessReadRequest: unmatched_ranges is empty, break";
      break;
    }
  }
  // End of for reverse loop for slices

  // add the parts that are not covered by any slice
  for (const auto& range : unmatched_ranges) {
    SliceReadReq uncovered_slice_read_req{
        range.offset,
        range.len,
        std::nullopt,
    };
    VLOG(9) << "ProcessReadRequest: uncovered_slice_read_req: "
            << uncovered_slice_read_req.ToString();
    results.push_back(uncovered_slice_read_req);
  }

  // sort the results by file offset
  boost::range::sort(results, [](const SliceReadReq& a, const SliceReadReq& b) {
    return a.file_offset < b.file_offset;
  });

  return std::move(results);
}

static inline std::vector<BlockReadReq> ConvertSliceReadReqToBlockReadReqs(
    const SliceReadReq& slice_req, uint64_t fs_id, uint64_t ino,
    uint64_t chunk_size, uint64_t block_size) {
  VLOG(9) << "ConvertSliceReadReqToBlockReadReqs: slice_req: "
          << slice_req.ToString() << " fs_id=" << fs_id << ", ino=" << ino
          << ", chunk_size=" << chunk_size << ", block_size=" << block_size;

  CHECK(slice_req.slice.has_value())
      << "Illegal slice_req: " << slice_req.ToString() << ", fs_id=" << fs_id
      << ", ino=" << ino << ", chunk_size=" << chunk_size
      << ", block_size=" << block_size;

  CHECK_GE(slice_req.file_offset, slice_req.slice->offset)
      << "Illegal slice_req_offset: " << slice_req.file_offset
      << " slice_offset: " << slice_req.slice->offset;

  std::vector<BlockReadReq> block_read_reqs;

  const auto& slice = slice_req.slice.value();
  uint64_t slice_offset = slice.offset;
  uint64_t slice_len = slice.length;
  uint64_t slice_id = slice.id;

  uint64_t read_offset = slice_req.file_offset;
  uint64_t remain_len = slice_req.len;

  VLOG(9) << "ConvertSliceReadReqToBlockReadReqs: read from file_offset: "
          << read_offset << ", remain_len: " << remain_len;

  while (remain_len > 0) {
    // calculate the current block range
    uint64_t cur_block_start = (read_offset / block_size) * block_size;
    uint64_t cur_block_end = cur_block_start + block_size;
    cur_block_start = std::max(cur_block_start, slice_offset);
    cur_block_end = std::min(cur_block_end, slice_offset + slice_len);
    CHECK_LT(cur_block_start, cur_block_end)
        << "Illegal cur_block_start: " << cur_block_start
        << ", cur_block_end: " << cur_block_end
        << ", slice_req: " << slice_req.ToString();

    uint64_t offset_in_chunk = cur_block_start % chunk_size;
    uint64_t block_index_in_chunk = offset_in_chunk / block_size;

    BlockDesc block{
        .file_offset = cur_block_start,
        .block_len = cur_block_end - cur_block_start,
        .zero = slice.is_zero,
        .version = slice.compaction,
        .slice_id = slice_id,
        .index = block_index_in_chunk,
    };

    VLOG(9) << "ConvertSliceReadReqToBlockReadReqs: block_obj: "
            << block.ToString() << " read file_offset:" << read_offset;

    CHECK_GE(read_offset, cur_block_start)
        << "Illegal read_offset: " << read_offset
        << " cur_block_start: " << cur_block_start;

    uint64_t read_block_offst = read_offset - cur_block_start;
    uint64_t read_block_len =
        std::min(remain_len, (block.block_len - read_block_offst));

    BlockReadReq block_read_req{
        .block_offset = read_block_offst,
        .len = read_block_len,
        .block = block,
    };

    VLOG(9) << "ConvertSliceReadReqToBlockReadReqs: block_read_req: "
            << block_read_req.ToString();

    block_read_reqs.push_back(block_read_req);

    remain_len -= read_block_len;
    read_offset += read_block_len;

    VLOG(9) << "ConvertSliceReadReqToBlockReadReqs: read_offset: "
            << read_offset << ", remain_len: " << remain_len;
  }

  return std::move(block_read_reqs);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_DATA_UITLS_H_