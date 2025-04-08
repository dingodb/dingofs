// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mdsv2/background/compaction.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "dingofs/mdsv2.pb.h"

namespace dingofs {
namespace mdsv2 {

struct DeleteSlice {
  uint64_t slice_id;
  std::string reason;
};

struct OffsetRange {
  uint64_t start;
  uint64_t end;
  std::vector<pb::mdsv2::Slice> slices;
};

bool CompactChunkProcessor::Init() { return true; }

bool CompactChunkProcessor::Destroy() { return true; }

void CompactChunkProcessor::TriggerCompaction() {}

Status CompactChunkProcessor::ScanFile() { return Status::OK(); }

Status CompactChunkProcessor::CleanSlice(const std::vector<pb::mdsv2::Slice>& slices) { return Status::OK(); }

static std::map<uint64_t, pb::mdsv2::Slice> GenSliceMap(const std::map<uint64_t, pb::mdsv2::SliceList>& chunk_map) {
  std::map<uint64_t, pb::mdsv2::Slice> slice_map;
  for (const auto& [chunk_index, chunk] : chunk_map) {
    for (const auto& slice : chunk.slices()) {
      slice_map[slice.id()] = slice;
    }
  }

  return slice_map;
}

std::vector<pb::mdsv2::Slice> CompactChunkProcessor::CalculateInvalidSlices(
    std::map<uint64_t, pb::mdsv2::SliceList>& chunk_map, uint64_t file_length, uint64_t chunk_size) {
  std::vector<pb::mdsv2::Slice> delete_slices;

  // find out-of-file-length slices
  for (const auto& [chunk_index, chunk] : chunk_map) {
    if ((chunk_index + 1) * chunk_size < file_length) {
      continue;
    }

    for (const auto& slice : chunk.slices()) {
      if (slice.offset() >= file_length) {
        delete_slices.push_back(slice);
      }
    }
  }

  // get covered slices
  for (auto& [chunk_index, chunk] : chunk_map) {
    if ((chunk_index + 1) * chunk_size < file_length) {
      continue;
    }

    // sort by offset
    std::sort(chunk.mutable_slices()->begin(), chunk.mutable_slices()->end(),
              [](const pb::mdsv2::Slice& a, const pb::mdsv2::Slice& b) { return a.offset() < b.offset(); });

    // get offset ranges
    std::vector<uint64_t> offsets;
    for (const auto& slice : chunk.slices()) {
      offsets.push_back(slice.offset());
      offsets.push_back(slice.offset() + slice.len());
    }

    std::sort(offsets.begin(), offsets.end());

    std::vector<OffsetRange> offset_ranges;
    for (size_t i = 0; i < offsets.size() - 1; ++i) {
      offset_ranges.push_back({.start = offsets[i], .end = offsets[i + 1]});
    }

    for (auto& offset_range : offset_ranges) {
      for (const auto& slice : chunk.slices()) {
        uint64_t slice_start = slice.offset();
        uint64_t slice_end = slice.offset() + slice.len();
        if ((slice_start >= offset_range.start && slice_start < offset_range.end) ||
            (slice_end >= offset_range.start && slice_end < offset_range.end)) {
          offset_range.slices.push_back(slice);
        }
      }
    }

    std::set<uint64_t> reserve_slice_ids;
    for (auto& offset_range : offset_ranges) {
      std::sort(offset_range.slices.begin(), offset_range.slices.end(),
                [](const pb::mdsv2::Slice& a, const pb::mdsv2::Slice& b) { return a.id() > b.id(); });
      reserve_slice_ids.insert(offset_range.slices.front().id());
    }

    // delete slices
    for (const auto& slice : chunk.slices()) {
      if (reserve_slice_ids.count(slice.id()) == 0) {
        delete_slices.push_back(slice);
      }
    }
  }

  return delete_slices;
}

// delete invalid slice
// merge slices
void CompactChunkProcessor::Compact(InodePtr inode) {
  uint64_t chunk_size = 64 * 1024 * 1024;
  uint64_t file_length = inode->Length();
  auto chunk_map = inode->GetChunkMap();

  auto delete_slices = CalculateInvalidSlices(chunk_map, file_length, chunk_size);
  if (delete_slices.empty()) {
    return;
  }

  CleanSlice(delete_slices);
}

}  // namespace mdsv2
}  // namespace dingofs
