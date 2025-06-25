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

#ifndef DINGOFS_CLIENT_VFS_DATA_SLICE_BLOCK_DATA_H_
#define DINGOFS_CLIENT_VFS_DATA_SLICE_BLOCK_DATA_H_

#include <fmt/format.h>

#include <cstdint>
#include <map>
#include <memory>

#include "client/datastream/page_allocator.h"
#include "client/vfs/data/slice/common.h"
#include "client/vfs/data/slice/page_data.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

// protected by slice data
class BlockData {
 public:
  explicit BlockData(const SliceDataContext& context,
                     datastream::PageAllocator* allocator, uint64_t block_index,
                     uint64_t block_offset)
      : context_(context),
        page_allocator_(allocator),
        block_index_(block_index),
        block_offset_(block_offset),
        lower_bound_in_chunk_(block_index_ * context_.block_size),
        upper_bound_in_chunk_(lower_bound_in_chunk_ + context_.block_size) {}

  ~BlockData();

  Status Write(const char* buf, uint64_t size, uint64_t block_offset);

  IOBuffer ToIOBuffer() const;

  uint64_t BlockIndex() const { return block_index_; }

  uint64_t ChunkOffset() const {
    return (block_index_ * context_.block_size) + block_offset_;
  }

  uint64_t End() const { return ChunkOffset() + len_; }

  uint64_t Len() const { return len_; }

  std::string UUID() const {
    return fmt::format("block_data-{}-{}-{}-{}", context_.ino,
                       context_.chunk_index, context_.seq, block_index_);
  }

  // NOTE: should be called under slice lock
  std::string ToString() const {
    std::ostringstream ss;
    ss << "{ uuid: " << UUID() << ", block_range: [" << block_offset_ << "-"
       << (block_offset_ + len_) << "] "
       << ", chunk_range: [" << ChunkOffset() << "-" << End() << "] "
       << "len: " << len_
       << ", bound: [" << lower_bound_in_chunk_ << "-" << upper_bound_in_chunk_
       << "] "
       << "page_count: " << pages_.size() << " }";
    return ss.str();
  }

 private:
  char* AllocPage();

  PageData* FindOrCreatePageData(uint64_t page_index, uint64_t page_offset);

  const SliceDataContext context_;
  datastream::PageAllocator* page_allocator_{nullptr};
  const uint64_t block_index_;
  uint64_t block_offset_{0};
  uint64_t len_{0};
  const uint64_t lower_bound_in_chunk_{0};
  const uint64_t upper_bound_in_chunk_{0};
  std::map<uint64_t, PageDataUPtr> pages_;  // page_index -> PageData
};

using BlockDataUPtr = std::unique_ptr<BlockData>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_SLICE_BLOCK_DATA_H_