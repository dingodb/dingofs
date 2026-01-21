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

#include "client/vfs/data/slice/slice_writer.h"

#include <butil/iobuf.h>
#include <butil/time.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>

#include "client/vfs/common/helper.h"
#include "client/vfs/data/slice/task/slice_flush_task.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "common/callback.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("SliceWriter::" + std::string(__FUNCTION__))

BlockData* SliceWriter::FindOrCreateBlockDataUnlocked(uint64_t block_index,
                                                      uint64_t block_offset) {
  auto iter = block_datas_.find(block_index);
  if (iter != block_datas_.end()) {
    VLOG(4) << fmt::format(
        "{} Found existing block data for block index: {}, block: {}", UUID(),
        block_index, iter->second->ToString());
    return iter->second.get();
  }

  auto [new_iter, inserted] = block_datas_.emplace(
      block_index, std::make_unique<BlockData>(
                       context_, vfs_hub_, vfs_hub_->GetWriteBufferManager(),
                       block_index, block_offset));
  CHECK(inserted);

  VLOG(4) << fmt::format(
      "{} Creating new block data for block index: {}, block: {}", UUID(),
      block_index, new_iter->second->ToString());

  return new_iter->second.get();
}

// no overlap slice write will come here
Status SliceWriter::Write(ContextSPtr ctx, const char* buf, uint64_t size,
                          uint64_t chunk_offset) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan("SliceWriter::Write",
                                                         ctx->GetTraceSpan());

  uint64_t end_in_chunk = chunk_offset + size;

  VLOG(4) << fmt::format("{} Start writing chunk_range: [{}-{}] len: {}",
                         UUID(), chunk_offset, end_in_chunk, size);

  CHECK_GT(size, 0);
  CHECK(chunk_offset == End() || end_in_chunk == chunk_offset_) << fmt::format(
      "{} Unexpected chunk offset: {}, end in chunk: {}, chunk_offset_: {}",
      ToString(), chunk_offset, end_in_chunk, chunk_offset_);

  uint64_t block_size = context_.block_size;
  uint64_t block_index = chunk_offset / block_size;
  uint64_t block_offset = chunk_offset % block_size;

  const char* buf_pos = buf;

  uint64_t remain_len = size;

  {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);

    while (remain_len > 0) {
      uint64_t write_size = std::min(remain_len, block_size - block_offset);
      BlockData* block_data =
          FindOrCreateBlockDataUnlocked(block_index, block_offset);

      Status s = block_data->Write(SpanScope::GetContext(span), buf_pos,
                                   write_size, block_offset);
      CHECK(s.ok()) << fmt::format(
          "{} Failed to write data to block data, block_index: {}, "
          "chunk_range: [{}-{}], len: {}, slice: {}, status: {}",
          UUID(), block_index, block_offset, (block_offset + write_size),
          write_size, ToStringUnlocked(), s.ToString());

      remain_len -= write_size;
      buf_pos += write_size;
      block_offset = 0;
      ++block_index;
    }

    {
      uint64_t old_len = len_;
      uint64_t old_chunk_offset = chunk_offset_;

      chunk_offset_ = std::min(chunk_offset, chunk_offset_);
      len_ += size;

      VLOG(4) << fmt::format(
          "{} Update slice data, old_chunk_offset: {}, old_len: {}, "
          "updated slice: {}",
          UUID(), old_chunk_offset, old_len, ToStringUnlocked());
    }
  }

  VLOG(4) << fmt::format("{} End writing chunk_range: [{}-{}], len: {}", UUID(),
                         chunk_offset, end_in_chunk, size);

  return Status::OK();
}

void SliceWriter::FlushAsync(StatusCallback cb) {
  VLOG(4) << fmt::format("{} FlushAsync Start", UUID());

  {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    CHECK(!flushing_) << fmt::format(
        "{} Flushing already in progress, unexpected state", UUID());
    flushing_ = true;
    flush_status_ = Status::OK();
    flush_cb_.swap(cb);
  }

  vfs_hub_->GetFlushExecutor()->Execute([this]() { this->DoFlush(); });
}

void SliceWriter::FlushDone(Status s) {
  VLOG(4) << fmt::format("{} FlushDone status: {}", UUID(), s.ToString());

  flushed_.store(true, std::memory_order_relaxed);

  StatusCallback cb;
  {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    cb.swap(flush_cb_);
  }

  cb(s);
}

void SliceWriter::SliceFlushed(Status status, SliceFlushTask* task) {
  if (!status.ok()) {
    LOG(WARNING) << fmt::format(
        "{} Failed to flush slice: {}, slice_flush_task: {}, status: {}",
        UUID(), ToString(), task->ToString(), status.ToString());
    flush_status_ = status;
  }

  FlushDone(status);
}

void SliceWriter::DoFlush() {
  // TODO: get ctx from parent
  auto span = vfs_hub_->GetTraceManager()->StartSpan("SliceWriter::DoFlush");

  VLOG(4) << fmt::format("{} DoFlush", UUID());

  uint64_t slice_id = 0;
  Status s = vfs_hub_->GetMetaSystem()->NewSliceId(SpanScope::GetContext(span),
                                                  context_.ino, &slice_id);
  if (!s.ok()) {
    LOG(ERROR) << fmt::format("{} Failed to get new slice id status: {}",
                              UUID(), s.ToString());
    FlushDone(s);
    return;
  }

  VLOG(4) << fmt::format("{} Got slice id: {}", UUID(), slice_id);

  std::map<uint64_t, BlockDataUPtr> to_flush;
  {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    CHECK_GT(block_datas_.size(), 0) << fmt::format(
        "{} No block data to flush, slice: {}", UUID(), ToStringUnlocked());

    id_ = slice_id;
    to_flush = std::move(block_datas_);
  }

  flush_task_ = std::make_unique<SliceFlushTask>(context_, vfs_hub_, slice_id,
                                                 std::move(to_flush));

  flush_task_->RunAsync(
      [this](Status s) { this->SliceFlushed(s, flush_task_.get()); });
}

Slice SliceWriter::GetCommitSlice() {
  uint64_t chunk_start_in_file = context_.chunk_index * context_.chunk_size;

  uint64_t len = 0;
  uint64_t chunk_offset = 0;

  {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    CHECK(flush_status_.ok())
        << "{} unexpected behavior, flush_status: " << flush_status_.ToString()
        << ", slice: " << ToStringUnlocked();

    len = len_;
    chunk_offset = chunk_offset_;
  }

  Slice slice{.id = id_,
              .offset = chunk_start_in_file + chunk_offset,
              .length = len,
              .compaction = 0,
              .is_zero = false,
              .size = len};

  VLOG(4) << fmt::format(
      "{} GetCommitSlices completed, slice: {}, slice_data: {}", UUID(),
      Slice2Str(slice), ToString());
  return slice;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs