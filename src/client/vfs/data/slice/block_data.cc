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

#include "client/common/const.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("BlockData::" + std::string(__FUNCTION__))

void BlockData::FreePageData() {
  VLOG(6) << fmt::format("{} FreePageData, block_data: {}", UUID(), ToString());

  for (auto it = pages_.begin(); it != pages_.end();) {
    uint64_t page_index = it->first;
    PageData* page_data = it->second.get();
    CHECK_NOTNULL(page_data->page);

    butil::Timer timer;
    timer.start();
    page_allocator_->DeAllocate(page_data->page);
    timer.stop();

    VLOG(12) << fmt::format("{} Deallocating page at: {} took: <{:.6f}> ms",
                            UUID(), Helper::Char2Addr(page_data->page),
                            timer.u_elapsed(0.0));

    it = pages_.erase(it);
  }
}

char* BlockData::AllocPage() {
  // TODO: add metric for time spent in page allocation
  butil::Timer timer;
  timer.start();
  auto* page = page_allocator_->Allocate();
  timer.stop();
  VLOG(12) << fmt::format(
      "{} Allocated page at: {} allocation took: <{:.6f}> ms", UUID(),
      Helper::Char2Addr(page), timer.u_elapsed(0.0));
  return page;
}

PageData* BlockData::FindOrCreatePageData(uint64_t page_index,
                                          uint64_t page_offset) {
  auto iter = pages_.find(page_index);
  if (iter != pages_.end()) {
    VLOG(6) << fmt::format("{} Found existing page data for page index: {}",
                           UUID(), page_index);
    return iter->second.get();
  }

  auto [new_iter, inserted] = pages_.emplace(
      page_index,
      std::make_unique<PageData>(vfs_hub_, page_index, context_.page_size,
                                 AllocPage(), page_offset));
  CHECK(inserted);

  VLOG(12) << fmt::format("{} Creating new page_data: {} for page index: {}",
                          UUID(), new_iter->second->ToString(), page_index);

  return new_iter->second.get();
}

Status BlockData::Write(ContextSPtr ctx, const char* buf, uint64_t size,
                        uint64_t block_offset) {
  auto span = vfs_hub_->GetTraceManager().StartChildSpan("BlockData::Write",
                                                         ctx->GetTraceSpan());

  uint64_t end_write_block_offset = (block_offset + size);

  uint64_t write_chunk_offset =
      (block_index_ * context_.block_size) + block_offset;
  uint64_t end_write_chunk_offset = write_chunk_offset + size;

  VLOG(6) << fmt::format(
      "{} Write Start data size: {}, block_range: [{}-{}], chunk_range: "
      "[{}-{}] to block_data: {}",
      UUID(), size, block_offset, end_write_block_offset, write_chunk_offset,
      end_write_chunk_offset, ToString());

  // TODO: use DCHECK in future
  CHECK_GT(size, 0);
  CHECK_LE(lower_bound_in_chunk_, write_chunk_offset);
  CHECK_GE(upper_bound_in_chunk_, end_write_chunk_offset);

  CHECK(block_offset == (block_offset_ + len_) ||
        (block_offset + size) == block_offset_)
      << fmt::format(
             "{} Write Illegal block_range: [{}-{}], chunk_range: [{}-{}] "
             "for block_data: {}",
             block_offset, end_write_block_offset, write_chunk_offset,
             end_write_chunk_offset, ToString());

  uint64_t page_size = context_.page_size;
  uint64_t page_index = block_offset / page_size;
  uint64_t page_offset = block_offset % page_size;

  const char* buf_pos = buf;
  uint64_t remain_len = size;

  while (remain_len > 0) {
    uint64_t write_size = std::min(remain_len, page_size - page_offset);
    PageData* page_data = FindOrCreatePageData(page_index, page_offset);

    page_data->Write(SpanScope::GetContext(span), buf_pos, write_size,
                     page_offset);

    remain_len -= write_size;
    buf_pos += write_size;
    page_offset = 0;
    ++page_index;
  }

  {
    uint64_t old_len = len_;
    uint64_t old_block_offset = block_offset_;

    block_offset_ = std::min(block_offset_, block_offset);
    len_ += size;

    VLOG(6) << fmt::format(
        "{} Update block_data old_block_offset: {}, old_len: {}, "
        "updated data_block: {}",
        UUID(), old_block_offset, old_len, ToString());
  }

  VLOG(6) << fmt::format("{} Write End", UUID());

  return Status::OK();
}

static void NoopDeleter(void* data) {}

IOBuffer BlockData::ToIOBuffer() const {
  butil::IOBuf iobuf;

  uint64_t remain_len = len_;

  for (const auto& [page_index, page_data_ptr] : pages_) {
    CHECK_NOTNULL(page_data_ptr->page);

    uint64_t data_size = page_data_ptr->data_len;
    char* data_ptr = page_data_ptr->page + page_data_ptr->data_offset;

    iobuf.append_user_data(data_ptr, data_size, NoopDeleter);

    remain_len -= data_size;

    VLOG(12) << fmt::format("{} Add page_data: {}, data_ptr: {} to IOBuffer",
                            UUID(), page_data_ptr->ToString(),
                            Helper::Char2Addr(data_ptr));
  }

  CHECK_EQ(remain_len, 0) << "BlockData::Flush Remaining len is not zero: "
                          << remain_len << ", block_data: " << ToString();

  VLOG(6) << fmt::format("{} Finish ToIOBuffer", UUID(), ToString());

  return IOBuffer(iobuf);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs