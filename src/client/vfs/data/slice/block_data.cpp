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

#include "client/vfs/data/slice/block_data.h"

#include <butil/iobuf.h>
#include <butil/time.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <cstdint>

#include "client/common/utils.h"

namespace dingofs {
namespace client {
namespace vfs {

BlockData::~BlockData() {
  std::map<uint64_t, PageDataUPtr> pages_;  // page_index -> PageData
  for (const auto& [page_index, page_data_ptr] : pages_) {
    CHECK_NOTNULL(page_data_ptr->data);
    butil::Timer timer;
    timer.start();
    page_allocator_->DeAllocate(page_data_ptr->data);
    timer.stop();

    VLOG(4) << fmt::format(
        "Deallocating page at: {} for block_index: {}, page_index: {}, took: "
        "<{:.6f}> ms",
        Char2Addr(page_data_ptr->data), block_index_, page_index,
        timer.u_elapsed());
  }
}

std::string BlockData::ToString() const {
  return fmt::format(
      "{(ino:{}, chunk_index:{}, seq:{}, block_index: {}) chunk_range: "
      "[{}-{}], len: {}, bound: [{}-{}]}, page_count: {}",
      context_.ino, context_.chunk_index, context_.seq, block_index_,
      chunk_offset_, End(), len_, lower_bound_in_chunk_, upper_bound_in_chunk_,
      pages_.size());
}

char* BlockData::AllocPage() {
  // TODO: add metric for time spent in page allocation
  butil::Timer timer;
  timer.start();
  auto* page = page_allocator_->Allocate();
  timer.stop();
  VLOG(4) << fmt::format(
      "Allocated page at: {} for block_index: {}, allocation took: <{:.6f}> ms",
      Char2Addr(page), block_index_, timer.u_elapsed());
  return page;
}

PageData* BlockData::FindOrCreatePageData(uint64_t page_index) {
  auto [iter, inserted] = pages_.try_emplace(
      page_index, std::make_unique<PageData>(
                      PageData{.index = page_index, .data = AllocPage()}));
  if (inserted) {
    VLOG(4) << fmt::format(
        "Creating new page data for page index: {} in block data: {}",
        page_index, ToString());
  } else {
    VLOG(4) << fmt::format(
        "Found existing page data for page index: {} in block data: {}",
        page_index, ToString());
  }

  return iter->second.get();
}

Status BlockData::Write(const char* buf, uint64_t size, uint64_t block_offset) {
  uint64_t write_chunk_offset =
      (block_index_ * context_.block_size) + block_offset;
  uint64_t end_write_chunk_offset = write_chunk_offset + size;

  VLOG(4) << fmt::format(
      "Start writing data size: {}, block_range: [{}-{}], chunk_range: [{}-{}] "
      "to block_data: {}",
      size, block_offset, (block_offset + size), write_chunk_offset,
      end_write_chunk_offset, ToString());

  // TODO: use DCHECK in future
  CHECK_GT(size, 0);
  CHECK_LE(lower_bound_in_chunk_, write_chunk_offset);
  CHECK_GT(upper_bound_in_chunk_, end_write_chunk_offset);
  CHECK_GE(block_offset, write_chunk_offset);

  uint64_t page_size = context_.page_size;
  uint64_t page_index = block_offset / page_size;
  uint64_t page_offset = block_offset % page_size;

  const char* buf_pos = buf;
  uint64_t remain_len = size;

  while (remain_len > 0) {
    uint64_t write_size = std::min(remain_len, page_size - page_offset);
    PageData* page_data = FindOrCreatePageData(page_index);
    CHECK_NOTNULL(page_data->data);

    // Copy data into the allocated page
    memcpy(page_data->data + page_offset, buf_pos, write_size);

    remain_len -= write_size;
    buf_pos += write_size;
    page_offset = 0;
    ++page_index;
  }

  uint64_t old_end_write_chunk_offset = End();

  if (end_write_chunk_offset > old_end_write_chunk_offset) {
    uint64_t old_len = len_;
    len_ += end_write_chunk_offset - old_end_write_chunk_offset;

    VLOG(4) << fmt::format(
        "Updating length for block_data: {}, old_len: {}, "
        "old_end_write_chunk_offset: {}",
        ToString(), old_len, old_end_write_chunk_offset);
  }

  VLOG(4) << fmt::format(
      "End writing data size: {}, block_range: [{}-{}],  chunk_range: [{}-{}] "
      "to block_data: {}",
      size, block_offset, (block_offset + size), write_chunk_offset,
      end_write_chunk_offset, ToString());

  return Status::OK();
}


IOBuffer BlockData::ToIOBuffer() const {
  butil::IOBuf iobuf;

  uint64_t remain_len = len_;
  for (const auto& [page_index, page_data_ptr] : pages_) {
    CHECK_NOTNULL(page_data_ptr->data);
    uint64_t write_size = std::min(remain_len, context_.page_size);

    iobuf.append(page_data_ptr->data, write_size);

    remain_len -= write_size;
  }

  CHECK_EQ(remain_len, 0) << "BlockData::Flush Remaining len is not zero: "
                          << remain_len << ", block_data: " << ToString();

  VLOG(4) << "Flushing block data for block index: " << block_index_
          << ", block_data: " << ToString();

  return IOBuffer(iobuf);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs