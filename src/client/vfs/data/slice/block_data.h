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

#include "client/vfs/data/slice/common.h"
#include "client/vfs/data/slice/page_data.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/memory/write_buffer_manager.h"
#include "common/io_buffer.h"
#include "common/status.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

// protected by slice data
class BlockData {
 public:
  explicit BlockData(const SliceDataContext& context, VFSHub* vfs_hub,
                     WriteBufferManager* buffer_manager, uint32_t block_index,
                     int32_t block_offset)
      : context_(context),
        vfs_hub_(vfs_hub),
        write_buffer_manager_(buffer_manager),
        block_index_(block_index),
        block_offset_(block_offset) {}

  ~BlockData() { FreePageData(); }

  Status Write(ContextSPtr ctx, const char* buf, int32_t size,
               int32_t block_offset);

  IOBuffer ToIOBuffer() const;

  uint32_t BlockIndex() const { return block_index_; }

  int32_t SliceOffset() const {
    return (block_index_ * context_.block_size) + block_offset_;
  }

  int32_t End() const { return SliceOffset() + len_; }

  int32_t Len() const { return len_; }

  std::string UUID() const {
    return fmt::format("block_data-{}-{}", context_.UUID(), block_index_);
  }

  std::string ToString() const {
    return fmt::format(
        "(uuid: {}, block_range: [{}-{}], slice_range: [{}-{}], len: {}, "
        "page_count: {})",
        UUID(), block_offset_, block_offset_ + len_, SliceOffset(), End(), len_,
        pages_.size());
  }

 private:
  char* AllocPage();

  void FreePageData();

  PageData* FindOrCreatePageData(uint32_t page_index, int32_t page_offset);

  const SliceDataContext context_;
  VFSHub* vfs_hub_{nullptr};
  WriteBufferManager* write_buffer_manager_{nullptr};
  const uint32_t block_index_;
  int32_t block_offset_{0};
  int32_t len_{0};
  std::map<uint32_t, PageDataUPtr> pages_;  // page_index -> PageData
};

using BlockDataUPtr = std::unique_ptr<BlockData>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_SLICE_BLOCK_DATA_H_