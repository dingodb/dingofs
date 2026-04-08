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

#include "client/vfs/data/common/data_utils.h"

#include <absl/types/span.h>
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

void DumpSliceReadReqs(const std::vector<SliceReadReq>& results) {
  for (const auto& req : results) {
    LOG(INFO) << "SliceReadReq:  " << req.ToString();
  }
}

std::vector<SliceReadReq> Convert2SliceReadReq(absl::Span<const Slice> slices,
                                               const FileRange& file_range_req,
                                               int64_t chunk_start) {
  VLOG(9) << "Convert2SliceReadReq: input_file_range_req: "
          << file_range_req.ToString();
  std::vector<SliceReadReq> results;
  std::vector<FileRange> unmatched_ranges = {file_range_req};

  // loop from newest slice to oldest slice
  for (auto it = slices.rbegin(); it != slices.rend(); ++it) {
    const auto& slice = *it;
    VLOG(9) << "Convert2SliceReadReq: slice: " << Slice2Str(slice);

    std::vector<FileRange> new_unmatched_ranges;

    for (const auto& file_range : unmatched_ranges) {
      VLOG(9) << "Convert2SliceReadReq: file_range: " << file_range.ToString();

      // check if the current range overlaps with the slice
      int64_t slice_file_offset = chunk_start + slice.pos;
      int64_t slice_file_end = slice_file_offset + slice.len;
      if (slice_file_end <= file_range.offset ||
          slice_file_offset >= file_range.End()) {
        new_unmatched_ranges.push_back(file_range);
        VLOG(9)
            << "Convert2SliceReadReq: file_range_req no overlap with slice_id: "
            << slice.id;
        continue;
      }

      // calculate the overlapping part
      int64_t overlap_start = std::max(file_range.offset, slice_file_offset);
      int64_t overlap_end = std::min(file_range.End(), slice_file_end);
      int64_t overlap_len = overlap_end - overlap_start;

      SliceReadReq slice_read_req{
          .file_offset = overlap_start,
          .len = overlap_len,
          .slice = slice,
      };

      VLOG(9) << "Convert2SliceReadReq: slice_read_req: "
              << slice_read_req.ToString();

      results.push_back(slice_read_req);

      // process the left part of the range that is not covered by the slice
      if (file_range.offset < overlap_start) {
        FileRange left_uncovered_file_range{
            .offset = file_range.offset,
            .len = (overlap_start - file_range.offset),
        };
        VLOG(9) << "Convert2SliceReadReq: left_uncovered_file_range: "
                << left_uncovered_file_range.ToString();
        new_unmatched_ranges.push_back(left_uncovered_file_range);
      }

      // process the right part of the range that is not covered by the slice
      if (file_range.End() > overlap_end) {
        FileRange right_uncovered_file_range{
            .offset = overlap_end,
            .len = (file_range.End() - overlap_end),
        };
        VLOG(9) << "Convert2SliceReadReq: right_uncovered_file_range: "
                << right_uncovered_file_range.ToString();

        new_unmatched_ranges.push_back(right_uncovered_file_range);
      }
    }
    // End of for loop for unmatched_ranges

    unmatched_ranges = std::move(new_unmatched_ranges);

    if (unmatched_ranges.empty()) {
      VLOG(9) << "Convert2SliceReadReq: unmatched_ranges is empty, break";
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
    VLOG(9) << "Convert2SliceReadReq: uncovered_slice_read_req: "
            << uncovered_slice_read_req.ToString();
    results.push_back(uncovered_slice_read_req);
  }

  // sort the results by file offset
  boost::range::sort(results, [](const SliceReadReq& a, const SliceReadReq& b) {
    return a.file_offset < b.file_offset;
  });

  return results;
}

std::vector<SliceReadReq> ProcessReadRequest(const std::vector<Slice>& slices,
                                             const FileRange& file_range_req,
                                             int64_t chunk_start) {
  VLOG(9) << "ProcessReadRequest: file_range_req: "
          << file_range_req.ToString();
  absl::Span<const Slice> slice_span(slices);
  return Convert2SliceReadReq(slice_span, file_range_req, chunk_start);
}

std::vector<BlockReadReq> ConvertSliceReadReqToBlockReadReqs(
    const SliceReadReq& slice_req, uint32_t fs_id, uint64_t ino,
    int32_t chunk_size, int32_t block_size, int64_t chunk_start) {
  VLOG(9) << "ConvertSliceReadReqToBlockReadReqs: slice_req: "
          << slice_req.ToString() << " fs_id=" << fs_id << ", ino=" << ino
          << ", chunk_size=" << chunk_size << ", block_size=" << block_size;

  CHECK(slice_req.slice.has_value())
      << "Illegal slice_req: " << slice_req.ToString() << ", fs_id=" << fs_id
      << ", ino=" << ino << ", chunk_size=" << chunk_size
      << ", block_size=" << block_size;

  int64_t slice_file_offset = chunk_start + slice_req.slice->pos;
  CHECK_GE(slice_req.file_offset, slice_file_offset)
      << "Illegal slice_req_offset: " << slice_req.file_offset
      << " slice_file_offset: " << slice_file_offset;

  std::vector<BlockReadReq> block_read_reqs;

  const auto& slice = slice_req.slice.value();
  int64_t slice_offset = chunk_start + slice.pos;
  uint64_t slice_id = slice.id;

  int64_t read_offset = slice_req.file_offset;
  int64_t remain_len = slice_req.len;

  VLOG(9) << "ConvertSliceReadReqToBlockReadReqs: read from file_offset: "
          << read_offset << ", remain_len: " << remain_len;

  int32_t slice_off = slice.off;
  int32_t slice_size = slice.size;

  int64_t slice_logical_end = slice_offset + slice.len;

  while (remain_len > 0) {
    // 1. Calculate the physical offset within the slice's data
    int32_t physical_offset =
        static_cast<int32_t>(slice_off + (read_offset - slice_offset));

    // 2. Block boundaries based on slice physical offset
    int32_t block_index = physical_offset / block_size;
    int32_t block_start = block_index * block_size;
    int32_t block_end = std::min(block_start + block_size, slice_size);
    int32_t actual_block_size = block_end - block_start;

    // 3. Reverse-map to file range covered by this block, clamped to slice
    int64_t file_block_start =
        std::max(slice_offset + (block_start - slice_off), slice_offset);
    int64_t file_block_end =
        std::min(slice_offset + (block_end - slice_off), slice_logical_end);

    // 4. Offset within the block and read length
    int32_t offset_in_block = physical_offset % block_size;
    int32_t read_len = static_cast<int32_t>(
        std::min(remain_len, file_block_end - read_offset));

    CHECK_GT(read_len, 0) << "Illegal read_len: " << read_len
                          << ", slice_req: " << slice_req.ToString()
                          << ", read_offset: " << read_offset
                          << ", file_block_end: " << file_block_end;

    // 5. Construct BlockReadReq
    BlockKey key(slice_id, block_index, actual_block_size);
    BlockReadReq block_read_req{
        .file_offset = read_offset,
        .offset_in_block = offset_in_block,
        .len = read_len,
        .key = key,
    };

    VLOG(9) << "ConvertSliceReadReqToBlockReadReqs: block_read_req: "
            << block_read_req.ToString();

    block_read_reqs.push_back(block_read_req);

    remain_len -= read_len;
    read_offset += read_len;

    VLOG(9) << "ConvertSliceReadReqToBlockReadReqs: read_offset: "
            << read_offset << ", remain_len: " << remain_len;
  }

  return block_read_reqs;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
