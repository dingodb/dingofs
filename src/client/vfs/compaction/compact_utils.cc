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
#include <limits>
#include <vector>

#include "client/vfs/common/helper.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace compaction {

namespace {

bool IsEquivalentHole(const Slice& lhs, const Slice& rhs) {
  return lhs.id == 0 && rhs.id == 0 && lhs.pos == rhs.pos && lhs.len == rhs.len;
}

}  // namespace

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

  return FileRange{.offset = min_offset, .len = (max_end - min_offset)};
}

int32_t Skip(int64_t chunk_start, const std::vector<Slice>& slices) {
  const int32_t total = static_cast<int32_t>(slices.size());
  if (total == 0) return 0;

  // Convert2SliceReadReq() partitions the complete suffix file range into
  // covered and uncovered requests, so the sum of its result lengths is
  // always exactly (suffix_max_end - suffix_min_offset). Precompute those
  // suffix bounds once instead of rebuilding the full read plan for every
  // candidate slice.
  std::vector<int64_t> suffix_min_offset(total + 1,
                                         std::numeric_limits<int64_t>::max());
  std::vector<int64_t> suffix_max_end(total + 1,
                                      std::numeric_limits<int64_t>::min());
  for (int32_t i = total - 1; i >= 0; --i) {
    const int64_t offset = chunk_start + slices[i].pos;
    suffix_min_offset[i] = std::min(offset, suffix_min_offset[i + 1]);
    suffix_max_end[i] = std::max(offset + slices[i].len, suffix_max_end[i + 1]);
  }

  int32_t skipped = 0;
  while (skipped < total) {
    const Slice& first = slices[skipped];
    const int64_t first_offset = chunk_start + first.pos;
    const int64_t first_end = first_offset + first.len;
    const int64_t readreqs_len =
        suffix_max_end[skipped] - suffix_min_offset[skipped];
    CHECK_GT(readreqs_len, 0) << "invalid slice range length for compact";

    if (first.len < (1 << 20) ||
        static_cast<int64_t>(first.len) * 5 < readreqs_len) {
      VLOG(12) << "Can't skip first slice too small, first_slice: "
               << Slice2Str(first) << ", readreqs_len: " << readreqs_len
               << ", skip: " << skipped;
      break;
    }

    // Convert2SliceReadReq() applies slices from newest to oldest. The first
    // slice of this suffix survives as the first complete read request iff it
    // starts at the suffix's minimum offset and no newer slice overlaps it.
    bool is_first = first_offset == suffix_min_offset[skipped];
    for (int32_t i = skipped + 1; is_first && i < total; ++i) {
      const int64_t newer_offset = chunk_start + slices[i].pos;
      const int64_t newer_end = newer_offset + slices[i].len;
      const bool overlaps =
          std::max(first_offset, newer_offset) < std::min(first_end, newer_end);
      // Zero slices are append-only hole markers and may legitimately be
      // duplicated by repeated PUNCH_HOLE/ZERO_RANGE operations. An identical
      // newer hole has the same visible value over the complete candidate
      // range, so it must not force the sparse range through data compaction.
      if (overlaps && !IsEquivalentHole(first, slices[i])) {
        is_first = false;
      }
    }

    if (!is_first) {
      VLOG(12) << "Can't skip overwritten or non-leading first slice: "
               << Slice2Str(first) << ", skip: " << skipped;
      break;
    }

    skipped++;
    VLOG(12) << "Skip slice for compact, slice: " << Slice2Str(first)
             << ", readreqs_len: " << readreqs_len << ", skip: " << skipped;
  }

  VLOG(9) << "Compact skip summary, slices: " << slices.size()
          << ", skipped: " << skipped;
  return skipped;
}

}  // namespace compaction

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
