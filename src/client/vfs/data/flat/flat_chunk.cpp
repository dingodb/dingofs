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

#include <cstdint>
#include <memory>

#include "client/vfs/data/flat/common.h"
#include "client/vfs/data/flat/slice_holder.h"
#include "client/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

std::vector<SliceRange> FlatFileChunk::FindOverlappingSlices(
    const SliceRange& new_slice_range) {
  std::vector<SliceRange> overlap_slices;
  auto start_iter = file_offset_slice_.lower_bound(new_slice_range.file_offset);

  // Check for overlap with the previous slice
  if (start_iter != file_offset_slice_.begin()) {
    start_iter--;
    const SliceRange& old_slice = start_iter->second;

    CHECK(old_slice.file_offset < new_slice_range.file_offset)
        << "expect " << old_slice.file_offset << " < "
        << new_slice_range.file_offset;

    if (old_slice.file_offset + old_slice.len > new_slice_range.file_offset) {
      overlap_slices.push_back(old_slice);
    }
    start_iter++;
  }

  // Check for overlaps with subsequent slices
  while (start_iter != file_offset_slice_.end()) {
    const SliceRange& old_slice = start_iter->second;
    if (old_slice.file_offset >=
        new_slice_range.file_offset + new_slice_range.len) {
      break;
    }
    overlap_slices.push_back(old_slice);
    start_iter++;
  }

  return overlap_slices;
}

void FlatFileChunk::RemoveOverlappingSlices(
    const std::vector<SliceRange>& overlap_slices) {
  for (const auto& overlap_slice : overlap_slices) {
    VLOG(6) << "Remove overlap slice_range: " << overlap_slice.ToString();
    file_offset_slice_.erase(overlap_slice.file_offset);
  }
}

void FlatFileChunk::ProcessOverlapSlices(
    const std::vector<SliceRange>& overlap_slices,
    const SliceRange& new_slice_range) {
  // Process the first overlapping slice
  const SliceRange& first_overlap = overlap_slices.front();
  if (first_overlap.file_offset < new_slice_range.file_offset) {
    uint64_t file_offset = first_overlap.file_offset;
    uint64_t len = new_slice_range.file_offset - first_overlap.file_offset;

    SliceRange left_slice = {file_offset, len, first_overlap.slice_id};
    VLOG(6) << "Insert left slice_range: " << left_slice.ToString();
    CHECK(file_offset_slice_.insert(std::make_pair(file_offset, left_slice))
              .second);
  }

  // Process the last overlapping slice
  const SliceRange& last_overlap = overlap_slices.back();
  if (last_overlap.file_offset + last_overlap.len >
      new_slice_range.file_offset + new_slice_range.len) {
    uint64_t file_offset = new_slice_range.file_offset + new_slice_range.len;
    uint64_t len = last_overlap.file_offset + last_overlap.len -
                   (new_slice_range.file_offset + new_slice_range.len);

    SliceRange right_slice = {file_offset, len, last_overlap.slice_id};
    VLOG(6) << "Insert right slice_range: " << right_slice.ToString();
    CHECK(file_offset_slice_.insert(std::make_pair(file_offset, right_slice))
              .second);
  }
}

void FlatFileChunk::InsertNewSlice(const SliceRange& new_slice_range) {
  VLOG(6) << "Insert new slice_range: " << new_slice_range.ToString();
  CHECK(
      file_offset_slice_
          .insert(std::make_pair(new_slice_range.file_offset, new_slice_range))
          .second);
}

void FlatFileChunk::InsertFileOffsetSliceMap(
    const SliceRange& new_slice_range) {
  VLOG(6) << "Will insert new slice_range into file_offset_slice_: "
          << new_slice_range.ToString();

  // Find overlapping slices
  std::vector<SliceRange> overlap_slices =
      FindOverlappingSlices(new_slice_range);

  if (!overlap_slices.empty()) {
    // Remove overlapping slices
    RemoveOverlappingSlices(overlap_slices);

    // Process and insert left and right slices from overlaps
    ProcessOverlapSlices(overlap_slices, new_slice_range);
  }

  // Insert the new slice
  InsertNewSlice(new_slice_range);
}

void FlatFileChunk::InsertSlice(const Slice& new_slice) {
  VLOG(6) << "Will insert new slice into flat_flie_chunk, new_slice: "
          << Slice2Str(new_slice);
  auto holder =
      SliceHolder::Create(fs_id_, ino_, chunk_size_, block_size_, new_slice);
  CHECK(slice_id_to_slice_holder_.insert({new_slice.id, std::move(holder)})
            .second)
      << "slice id " << new_slice.id << " already exists";

  SliceRange new_slice_range{new_slice.length, new_slice.length, new_slice.id};

  InsertFileOffsetSliceMap(new_slice_range);
}

void FlatFileChunk::GenerateFileSlice() {
  for (const auto& [file_offset, slice_range] : file_offset_slice_) {
    auto slice_holder_iter =
        slice_id_to_slice_holder_.find(slice_range.slice_id);
    CHECK(slice_holder_iter != slice_id_to_slice_holder_.end())
        << "slice holder not found for id: " << slice_range.slice_id;

    const auto* slice_holder = slice_holder_iter->second.get();

    std::vector<FileSlice> file_slices;
    slice_holder->GetFileSlices(slice_range, file_slices);

    for (const auto& file_slice : file_slices) {
      CHECK(file_offset_file_slice_map_
                .insert({file_slice.file_offset, file_slice})
                .second);
      VLOG(6) << "Insert file_slice: " << file_slice.ToString();
    }
  }
}

std::shared_ptr<FlatFileChunk> FlatFileChunk::Create(
    uint64_t fs_id, uint64_t ino, uint64_t index, uint64_t chunk_size,
    uint64_t block_size, const std::vector<Slice>& slices) {
  std::shared_ptr<FlatFileChunk> chunk(
      new FlatFileChunk(fs_id, ino, index, chunk_size, block_size));

  for (const auto& slice : slices) {
    chunk->InsertSlice(slice);
  }

  chunk->GenerateFileSlice();

  return chunk;
}

void FlatFileChunk::PrepareReadReq(const FileRange& file_range,
                                   std::vector<BlockReadReq>& block_read_reqs) {

}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs