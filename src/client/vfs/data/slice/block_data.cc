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
#include <vector>

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("BlockData::" + std::string(__FUNCTION__))

void BlockData::FreePageData() {
  VLOG(6) << fmt::format("{} FreePageData, block_data: {}", UUID(), ToString());

  std::vector<char*> pages;
  pages.reserve(pages_.size());
  for (const auto& [page_index, page_data] : pages_) {
    CHECK_NOTNULL(page_data->page);
    pages.push_back(page_data->page);
    VLOG(16) << fmt::format("{} Deallocating page at: {}", UUID(),
                            Helper::Char2Addr(page_data->page));
  }
  pages_.clear();
  write_buffer_manager_->DeAllocateBatch(pages.data(), pages.size());
}

PageData* BlockData::FindPageData(uint32_t page_index) {
  auto iter = pages_.find(page_index);
  return iter == pages_.end() ? nullptr : iter->second.get();
}

Status BlockData::ReservePages(int32_t size, int32_t block_offset,
                               std::vector<uint32_t>* created_pages) {
  CHECK_GT(size, 0);
  CHECK_GE(block_offset, 0);
  CHECK_LE(block_offset + size, context_.block_size);

  CHECK(block_offset == (block_offset_ + len_) ||
        (block_offset + size) == block_offset_)
      << fmt::format(
             "{} Reserve Illegal block_offset: {}, size: {}, block_data: {}",
             UUID(), block_offset, size, ToString());

  int32_t page_size = context_.page_size;
  uint32_t page_index = block_offset / page_size;
  int32_t page_offset = block_offset % page_size;
  int32_t remain_len = size;
  std::vector<uint32_t> missing_indexes;
  while (remain_len > 0) {
    int32_t write_size = std::min(remain_len, page_size - page_offset);
    if (FindPageData(page_index) == nullptr) {
      missing_indexes.push_back(page_index);
    }
    remain_len -= write_size;
    page_offset = 0;
    ++page_index;
  }

  if (missing_indexes.empty()) return Status::OK();
  std::vector<char*> allocated(missing_indexes.size());
  const size_t allocated_count = write_buffer_manager_->TryAllocateBatch(
      allocated.size(), allocated.data());
  const uint32_t first_page_index =
      static_cast<uint32_t>(block_offset / page_size);
  const int32_t first_page_offset = block_offset % page_size;
  for (size_t i = 0; i < allocated_count; ++i) {
    const uint32_t new_page_index = missing_indexes[i];
    auto [iter, inserted] = pages_.emplace(
        new_page_index,
        std::make_unique<PageData>(
            vfs_hub_, new_page_index, context_.page_size, allocated[i],
            new_page_index == first_page_index ? first_page_offset : 0));
    CHECK(inserted);
    created_pages->push_back(new_page_index);
    VLOG(12) << fmt::format("{} Reserved new page_data: {} for page index: {}",
                            UUID(), iter->second->ToString(), new_page_index);
  }
  return allocated_count == missing_indexes.size()
             ? Status::OK()
             : Status::NoSpace("write page pool exhausted");
}

void BlockData::RollbackPages(const std::vector<uint32_t>& created_pages) {
  std::vector<char*> pages;
  pages.reserve(created_pages.size());
  for (uint32_t idx : created_pages) {
    auto it = pages_.find(idx);
    if (it != pages_.end()) {
      if (it->second->page != nullptr) {
        pages.push_back(it->second->page);
      }
      pages_.erase(it);
    }
  }
  write_buffer_manager_->DeAllocateBatch(pages.data(), pages.size());
  VLOG(6) << fmt::format("{} RollbackPages freed {} pages", UUID(),
                         created_pages.size());
}

// INVARIANT: ApplyWrite must be infallible -- SliceWriter's two-phase
// atomicity relies on it (all fallible work, i.e. allocation, lives in
// ReservePages/phase 1; phase 2 only memcpys). It is infallible only because
// writes are append-only (SliceWriter gates on CHECK_EQ(chunk_offset, End())):
// PageData::Write then always sees a contiguous append/prepend and never its
// "illegal range" CHECK. If a random-overwrite write path is ever added, that
// CHECK can fire mid-apply -- after other blocks were already reserved/applied
// -- and abort the process, breaking the transaction. Revisit this split then.
void BlockData::ApplyWrite(ContextSPtr ctx, const char* buf, int32_t size,
                           int32_t block_offset) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan(
      "BlockData::ApplyWrite", ctx->GetTraceSpan());

  CHECK_GT(size, 0);
  CHECK_GE(block_offset, 0);
  CHECK_LE(block_offset + size, context_.block_size);

  CHECK(block_offset == (block_offset_ + len_) ||
        (block_offset + size) == block_offset_)
      << fmt::format(
             "{} Apply Illegal block_offset: {}, size: {}, block_data: {}",
             UUID(), block_offset, size, ToString());

  int32_t page_size = context_.page_size;
  uint32_t page_index = block_offset / page_size;
  int32_t page_offset = block_offset % page_size;
  const char* buf_pos = buf;
  int32_t remain_len = size;

  // Every page was reserved already -- FindPageData must hit. No allocation.
  while (remain_len > 0) {
    int32_t write_size = std::min(remain_len, page_size - page_offset);
    PageData* page_data = FindPageData(page_index);
    CHECK_NOTNULL(page_data);

    page_data->Write(SpanScope::GetContext(span), buf_pos, write_size,
                     page_offset);

    remain_len -= write_size;
    buf_pos += write_size;
    page_offset = 0;
    ++page_index;
  }

  int32_t old_len = len_;
  int32_t old_block_offset = block_offset_;
  block_offset_ = std::min(block_offset_, block_offset);
  len_ += size;

  VLOG(6) << fmt::format(
      "{} ApplyWrite old_block_offset: {}, old_len: {}, updated: {}", UUID(),
      old_block_offset, old_len, ToString());
}

static void NoopDeleter(void* data) {}

IOBuffer BlockData::ToIOBuffer() const {
  butil::IOBuf iobuf;

  int32_t remain_len = len_;

  for (const auto& [page_index, page_data_ptr] : pages_) {
    CHECK_NOTNULL(page_data_ptr->page);

    int32_t data_size = page_data_ptr->data_len;
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
