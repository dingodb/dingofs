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

#include "client/vfs/data/flat/slice_holder.h"

#include <glog/logging.h>

#include <cstdint>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "client/vfs/data/common.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

void SliceHolder::Init() {
  uint64_t offset_in_chunk = slice_.offset % chunk_size_;
  uint64_t block_index_within_chunk = offset_in_chunk / block_size_;

  uint64_t file_offset = slice_.offset;
  uint64_t chunk_end = slice_.length + slice_.offset;

  uint64_t block_id = file_offset / block_size_;
  while (file_offset < chunk_end) {
    uint64_t block_boundary = (block_id + 1) * block_size_;
    uint64_t block_len = std::min(block_boundary, chunk_end) - file_offset;

    BlockDesc block = {
        .file_offset = file_offset,
        .block_len = block_len,
        .zero = slice_.is_zero,
        .version = slice_.compaction,
        .slice_id = slice_.id,
        .index = block_index_within_chunk,
    };

    CHECK(file_offset_to_block_.insert({file_offset, block}).second);

    block_id++;
    file_offset += block_len;

    block_index_within_chunk++;
  }
}

std::unique_ptr<SliceHolder> SliceHolder::Create(uint64_t fs_id, uint64_t ino,
                                                 uint64_t chunk_size,
                                                 uint64_t block_size,
                                                 const Slice& slice) {
  std::unique_ptr<SliceHolder> holder(
      new SliceHolder(fs_id, ino, chunk_size, block_size, slice));
  holder->Init();
  return holder;
}

std::string SliceHolder::ToString() const {
  std::ostringstream os;
  os << "{ S3ChunkInfo :" << Slice2Str(slice_) << "\n";
  for (const auto& block : file_offset_to_block_) {
    os << block.second.ToString() << "\n";
  }
  os << "}";
  return os.str();
}

void SliceHolder::GetBlockDesc(const SliceRange& slice_range,
                               std::vector<BlockDesc> blocks) const {
  CHECK_EQ(slice_range.slice_id, slice_.id);
  CHECK_GE(slice_range.file_offset, slice_.offset);
  CHECK_LE(slice_range.End(), slice_.End());

  std::vector<BlockDesc> overlap_data_blocks;

  auto start_iter = file_offset_to_block_.lower_bound(slice_range.file_offset);
  if (start_iter != file_offset_to_block_.begin()) {
    start_iter--;
    const BlockDesc& block = start_iter->second;
    overlap_data_blocks.push_back(block);
    start_iter++;
  }

  while (start_iter != file_offset_to_block_.end() &&
         start_iter->first < slice_range.End()) {
    const BlockDesc& block = start_iter->second;
    overlap_data_blocks.push_back(block);
    start_iter++;
  }

  CHECK(!overlap_data_blocks.empty());

  blocks.insert(blocks.end(),
                std::make_move_iterator(overlap_data_blocks.begin()),
                std::make_move_iterator(overlap_data_blocks.end()));
}

void SliceHolder::GetFileSlices(const SliceRange& slice_range,
                                std::vector<FileSlice> file_slices) const {
  CHECK_EQ(slice_range.slice_id, slice_.id);
  CHECK_GE(slice_range.file_offset, slice_.offset);
  CHECK_LE(slice_range.len, slice_.length);

  std::vector<BlockDesc> overlap_data_blocks;
  GetBlockDesc(slice_range, overlap_data_blocks);

  for (const auto& block : overlap_data_blocks) {
    uint64_t start_offset =
        std::max(slice_range.file_offset, block.file_offset);
    uint64_t end_offset = std::min(slice_range.End(), block.End());
    uint64_t len = end_offset - start_offset;

    cache::blockcache::BlockKey key(fs_id_, ino_, block.slice_id, block.index,
                                    block.version);
    FileSlice file_slice = {
        .file_offset = start_offset,
        .len = len,
        .block_key = key,
        .block_len = block.block_len,
        .zero = block.zero,
    };

    file_slices.push_back(file_slice);
  }
}

}  // namespace vfs

}  // namespace client

}  // namespace dingofs