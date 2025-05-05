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

#include <cstdint>
#include <map>
#include <memory>

#include "client/datastream/page_allocator.h"
#include "client/vfs/data/slice/common.h"
#include "client/vfs/vfs_meta.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

struct PageData {
  uint64_t index;
  char* data{nullptr};  // not owned, mem managed by page allocator
};

using PageDataUPtr = std::unique_ptr<PageData>;

// protected by slice data
class BlockData {
 public:
  explicit BlockData(const SliceDataContext& context,
                     datastream::PageAllocator* allocator, uint64_t block_index,
                     uint64_t chunk_offset, uint64_t len)
      : context_(context),
        page_allocator_(allocator),
        block_index_(block_index),
        chunk_offset_(chunk_offset),
        len_(len),
        lower_bound_in_chunk_(block_index_ * context_.block_size),
        upper_bound_in_chunk_(lower_bound_in_chunk_ + context_.block_size) {}

  ~BlockData();

  Status Write(const char* buf, uint64_t size, uint64_t block_offset);

  IOBuffer ToIOBuffer() const;

  uint64_t BlockIndex() const { return block_index_; }

  uint64_t ChunkOffset() const { return chunk_offset_; }
  uint64_t End() const { return chunk_offset_ + len_; }
  uint64_t Len() const { return len_; }

  std::string ToString() const;

 private:
  char* AllocPage();

  PageData* FindOrCreatePageData(uint64_t page_index);

  const SliceDataContext context_;
  datastream::PageAllocator* page_allocator_{nullptr};
  const uint64_t block_index_;
  const uint64_t chunk_offset_{0};
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