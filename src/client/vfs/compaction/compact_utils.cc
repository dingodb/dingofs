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

#include "client/vfs/compaction/compact_utils.h"

#include <absl/types/span.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdint>

#include "client/vfs/common/helper.h"
#include "client/vfs/data/common/data_utils.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace compaction {

FileRange GetSlicesFileRange(int64_t chunk_start,
                             absl::Span<const Slice> slices) {
  CHECK(!slices.empty()) << "invalid compact, no slices to compact";

  int64_t min_offset = INT64_MAX;
  int64_t max_end = 0;

  for (const auto& slice : slices) {
    int64_t file_offset = chunk_start + slice.pos;
    min_offset = std::min(file_offset, min_offset);
    int64_t end = file_offset + slice.len;
    max_end = std::max(end, max_end);
  }

  CHECK_GT(max_end, min_offset);

  return FileRange{.offset = min_offset,
                   .len = (max_end - min_offset)};
}

int64_t SliceReadReqsLength(const std::vector<SliceReadReq>& reqs) {
  int64_t total_len = 0;
  for (const auto& req : reqs) {
    total_len += req.len;
  }
  return total_len;
}

int32_t Skip(int64_t chunk_start, const std::vector<Slice>& slices) {
  int32_t skipped = 0;
  int32_t total = slices.size();

  absl::Span<const Slice> span_slices(slices);

  while (skipped < total) {
    absl::Span<const Slice> ss = span_slices.subspan(skipped);
    const Slice& first = ss[0];

    FileRange range = GetSlicesFileRange(chunk_start, ss);
    auto slice_readreqs = Convert2SliceReadReq(ss, range, chunk_start);
    CHECK(!slice_readreqs.empty())
        << "invalid slice readreqs for compact, slices count: " << ss.size();

    int64_t readreqs_len = SliceReadReqsLength(slice_readreqs);
    CHECK_GT(readreqs_len, 0) << "invalid slice readreqs length for compact";

    if (first.len < (1 << 20) || first.len * 5 < readreqs_len) {
      VLOG(9) << "Can't skip first slice too small, first_slice: "
              << Slice2Str(first) << ", readreqs_len: " << readreqs_len
              << ", skip: " << skipped;
      break;
    }

    const auto& first_req = slice_readreqs[0];
    int64_t first_file_offset = chunk_start + first.pos;

    bool is_first =
        (first_req.file_offset == first_file_offset) &&
        (first_req.slice->id == first.id) && (first_req.len == first.len);

    if (!is_first) {
      VLOG(9) << "Can't skip not same as first, first_slice: "
              << Slice2Str(first) << ", first_req: " << first_req.ToString()
              << ", skip: " << skipped;
      break;
    }

    skipped++;
    VLOG(9) << "Skip slice for compact, slice: " << Slice2Str(first)
            << ", first_req: " << first_req.ToString()
            << ", readreqs_len: " << readreqs_len << ", skip: " << skipped;
  }

  return skipped;
}

}  // namespace compaction

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
