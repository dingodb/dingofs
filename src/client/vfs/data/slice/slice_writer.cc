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
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>

#include "client/vfs/common/helper.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "common/callback.h"
#include "common/status.h"

DEFINE_int32(vfs_flush_timeout_seconds, 300,
             "Timeout in seconds for waiting inflight block uploads in "
             "DoFlush. Used by WaitInflightWithTimeout.");

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("SliceWriter::" + std::string(__FUNCTION__))

BlockData* SliceWriter::FindOrCreateBlockDataUnlocked(uint32_t block_index,
                                                      int32_t block_offset) {
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

// --- slice_id async pre-allocation ---

// --- reference counting ---

void SliceWriter::IncRef() {
  int32_t prev = refs_.fetch_add(1, std::memory_order_relaxed);
  CHECK_GE(prev, 0) << UUID() << " IncRef on dead object";
}

void SliceWriter::DecRef() {
  int32_t prev = refs_.fetch_sub(1, std::memory_order_acq_rel);
  CHECK_GE(prev, 1) << UUID() << " DecRef underflow";
  if (prev == 1) {
    VLOG(4) << fmt::format("{} DecRef -> 0, destroying", UUID());
    delete this;
  }
}

// --- slice_id async pre-allocation ---

void SliceWriter::StartPrepareSliceId() {
  vfs_hub_->GetBGExecutor()->Execute([this]() { PrepareSliceId(); });
}

void SliceWriter::PrepareSliceId() {
  auto span =
      vfs_hub_->GetTraceManager()->StartSpan("SliceWriter::PrepareSliceId");
  auto ctx = SpanScope::GetContext(span);

  uint64_t slice_id = 0;
  uint64_t start_us = butil::cpuwide_time_us();

  Status s =
      vfs_hub_->GetMetaSystem()->NewSliceId(ctx, context_.ino, &slice_id);

  uint64_t elapsed_us = butil::cpuwide_time_us() - start_us;
  slice_id_alloc_latency_us_.store(elapsed_us, std::memory_order_relaxed);

  VLOG(2) << fmt::format("{} PrepareSliceId elapsed_us={}, status={}", UUID(),
                         elapsed_us, s.ToString());

  if (s.ok()) {
    CHECK_GT(slice_id, 0);
    slice_id_.store(slice_id, std::memory_order_release);
  } else {
    LOG(WARNING) << fmt::format("{} PrepareSliceId failed: {}", UUID(),
                                s.ToString());
    slice_id_alloc_failed_.store(true, std::memory_order_release);
  }
}

// --- Write with streaming upload ---

Status SliceWriter::Write(ContextSPtr ctx, const char* buf, int32_t size,
                          int32_t chunk_offset) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan("SliceWriter::Write",
                                                          ctx->GetTraceSpan());

  int32_t end_in_chunk = chunk_offset + size;

  VLOG(4) << fmt::format("{} Start writing chunk_range: [{}-{}] len: {}",
                         UUID(), chunk_offset, end_in_chunk, size);

  CHECK_GT(size, 0);
  CHECK_EQ(chunk_offset, End()) << fmt::format(
      "{} Unexpected chunk offset: {}, End(): {}, chunk_offset_: {}",
      ToString(), chunk_offset, End(), chunk_offset_);

  int32_t block_size = context_.block_size;
  int32_t slice_internal_offset = chunk_offset - chunk_offset_;
  uint32_t block_index = slice_internal_offset / block_size;
  int32_t block_offset = slice_internal_offset % block_size;

  const char* buf_pos = buf;

  int32_t remain_len = size;

  {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);

    // ---- Write loop: only fills BlockData ----
    while (remain_len > 0) {
      int32_t write_size = std::min(remain_len, block_size - block_offset);
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

    // ---- Update len_ ----
    {
      int32_t old_len = len_;
      int32_t old_chunk_offset = chunk_offset_;

      chunk_offset_ = std::min(chunk_offset, chunk_offset_);
      len_ += size;

      VLOG(4) << fmt::format(
          "{} Update slice data, old_chunk_offset: {}, old_len: {}, "
          "updated slice: {}",
          UUID(), old_chunk_offset, old_len, ToStringUnlocked());
    }

    // ---- Trigger streaming upload ----
    if (len_ >= context_.block_size && !flushing_) {
      FlushUpTo(len_);
    }
  }

  VLOG(4) << fmt::format("{} End writing chunk_range: [{}-{}], len: {}", UUID(),
                         chunk_offset, end_in_chunk, size);

  return Status::OK();
}

// --- FlushUpTo: upload all blocks with end <= written_len ---

void SliceWriter::FlushUpTo(int32_t written_len) {
  uint64_t sid = slice_id_.load(std::memory_order_acquire);
  if (sid == 0) {
    if (slice_id_alloc_failed_.load(std::memory_order_acquire)) {
      VLOG(2) << fmt::format("{} FlushUpTo skipped: slice_id alloc failed",
                             UUID());
    } else {
      VLOG(4) << fmt::format("{} FlushUpTo skipped: slice_id pending", UUID());
    }
    return;
  }

  int32_t block_size = context_.block_size;
  uint32_t blocks_uploaded = 0;

  for (auto it = block_datas_.begin(); it != block_datas_.end();) {
    int32_t end = (it->first + 1) * block_size;
    if (end <= written_len) {
      inflight_.fetch_add(1, std::memory_order_relaxed);
      BlockData* raw = it->second.release();

      UploadBlockAsync(sid, it->first, raw);

      it = block_datas_.erase(it);
      ++blocks_uploaded;
    } else {
      ++it;
    }
  }

  if (blocks_uploaded > 0) {
    total_blocks_streamed_.fetch_add(blocks_uploaded,
                                     std::memory_order_relaxed);
    VLOG(2) << fmt::format(
        "{} FlushUpTo uploaded={}, remaining={}, written_len={}", UUID(),
        blocks_uploaded, block_datas_.size(), written_len);
  }
}

// --- Async block upload ---

void SliceWriter::UploadBlockAsync(uint64_t slice_id, uint32_t block_index,
                                   BlockData* block_data) {
  auto span =
      vfs_hub_->GetTraceManager()->StartSpan("SliceWriter::UploadBlockAsync");

  BlockKey key(slice_id, block_index, block_data->Len());
  BlockContext block_ctx(key, context_.fs_id);

  PutReq req;
  req.block_ctx = block_ctx;
  req.data = block_data->ToIOBuffer();

  bool writeback = FLAGS_vfs_data_writeback;
  if (!writeback) {
    writeback = vfs_hub_->GetFileSuffixWatcher()->ShouldWriteback(context_.ino);
  }
  req.write_back = writeback;

  VLOG(4) << fmt::format("{} UploadBlockAsync key={}, block_index={}, len={}",
                         UUID(), key.StoreKey(), block_index,
                         block_data->Len());

  // Hold a ref for the async callback to prevent use-after-free if
  // SliceWriter is destroyed (e.g. flush timeout) before callback fires.
  IncRef();
  auto on_done = [this, block_data, span](Status s) {
    SpanScope::End(span);
    vfs_hub_->GetCBExecutor()->Execute([this, block_data, s]() {
      OnBlockUploaded(block_data, s);
      DecRef();  // release the ref held by IncRef above; may delete this
    });
  };

  vfs_hub_->GetBlockStore()->PutAsync(SpanScope::GetContext(span), req,
                                      std::move(on_done));
}

// --- Upload completion callback (runs on CBExecutor) ---

void SliceWriter::OnBlockUploaded(BlockData* block_data, Status s) {
  VLOG(4) << fmt::format("{} OnBlockUploaded block_index={}, status={}", UUID(),
                         block_data->BlockIndex(), s.ToString());

  // 1. Release memory (always, even on error)
  delete block_data;

  // 2. Record first error (first-error-wins)
  if (!s.ok()) {
    LOG(WARNING) << fmt::format("{} Block upload failed: {}", UUID(),
                                s.ToString());
    std::lock_guard<std::mutex> lg(error_mutex_);
    if (upload_error_.ok()) {
      upload_error_ = s;
    }
  }

  // 3. Observability
  total_blocks_uploaded_.fetch_add(1, std::memory_order_relaxed);

  // 4. Decrement inflight and notify DoFlush waiter
  uint32_t prev = inflight_.fetch_sub(1, std::memory_order_release);
  CHECK_GE(prev, 1) << "inflight underflow";

  if (prev == 1) {
    std::lock_guard<std::mutex> lg(inflight_mutex_);
    inflight_cv_.notify_all();
  }
}

// --- WaitInflightWithTimeout ---

bool SliceWriter::WaitInflightWithTimeout(std::chrono::seconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;

  std::unique_lock<std::mutex> lock(inflight_mutex_);
  while (inflight_.load(std::memory_order_acquire) > 0) {
    auto status = inflight_cv_.wait_until(lock, deadline);
    if (status == std::cv_status::timeout) {
      if (inflight_.load(std::memory_order_acquire) > 0) {
        LOG(ERROR) << fmt::format(
            "{} WaitInflight timeout after {}s, inflight={}", UUID(),
            timeout.count(), inflight_.load(std::memory_order_relaxed));
        return false;
      }
    }
  }
  return true;
}

// --- FlushAsync / DoFlush ---

void SliceWriter::FlushAsync(StatusCallback cb) {
  VLOG(4) << fmt::format("{} FlushAsync Start", UUID());

  {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    CHECK(!flushing_) << fmt::format(
        "{} Flushing already in progress, unexpected state", UUID());
    flushing_ = true;
    flush_cb_.swap(cb);
  }

  vfs_hub_->GetFlushExecutor()->Execute([this]() { this->DoFlush(); });
}

void SliceWriter::DoFlush() {
  auto span = vfs_hub_->GetTraceManager()->StartSpan("SliceWriter::DoFlush");
  auto ctx = SpanScope::GetContext(span);
  uint64_t start_us = butil::cpuwide_time_us();

  VLOG(2) << fmt::format("{} DoFlush start", UUID());

  // === Step 1: Check existing upload errors (fast fail) ===
  {
    std::lock_guard<std::mutex> lg(error_mutex_);
    if (!upload_error_.ok()) {
      LOG(WARNING) << fmt::format("{} DoFlush early fail: {}", UUID(),
                                  upload_error_.ToString());
      FlushDone(upload_error_);
      return;
    }
  }

  // === Step 2: Wait for streaming inflight blocks to complete ===
  auto timeout = std::chrono::seconds(FLAGS_vfs_flush_timeout_seconds);
  if (!WaitInflightWithTimeout(timeout)) {
    LOG(ERROR) << fmt::format("{} DoFlush timeout waiting inflight blocks",
                              UUID());
    FlushDone(Status::Internal("DoFlush timeout waiting inflight"));
    return;
  }

  // === Step 3: Re-check errors (inflight callbacks may have added new ones)
  // ===
  {
    std::lock_guard<std::mutex> lg(error_mutex_);
    if (!upload_error_.ok()) {
      FlushDone(upload_error_);
      return;
    }
  }

  // === Step 4: Ensure slice_id ===
  uint64_t sid = slice_id_.load(std::memory_order_acquire);
  if (sid == 0) {
    if (slice_id_alloc_failed_.load(std::memory_order_acquire)) {
      LOG(WARNING) << fmt::format(
          "{} DoFlush: prepareSliceId had failed, retrying synchronously",
          UUID());
    } else {
      VLOG(2) << fmt::format(
          "{} DoFlush: prepareSliceId still pending, falling back to sync",
          UUID());
    }

    Status s = vfs_hub_->GetMetaSystem()->NewSliceId(ctx, context_.ino, &sid);
    if (!s.ok()) {
      LOG(ERROR) << fmt::format("{} DoFlush NewSliceId failed: {}", UUID(),
                                s.ToString());
      FlushDone(s);
      return;
    }
    slice_id_.store(sid, std::memory_order_release);
  }

  // === Step 5: Upload remaining blocks (partial + any not yet streamed) ===
  std::map<uint32_t, BlockDataUPtr> remaining;
  {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    id_ = sid;
    remaining = std::move(block_datas_);
  }

  if (remaining.empty()) {
    uint64_t elapsed_us = butil::cpuwide_time_us() - start_us;
    VLOG(2) << fmt::format(
        "{} DoFlush done (no remaining), elapsed_us={}, "
        "total_streamed={}, total_uploaded={}",
        UUID(), elapsed_us,
        total_blocks_streamed_.load(std::memory_order_relaxed),
        total_blocks_uploaded_.load(std::memory_order_relaxed));
    FlushDone(Status::OK());
    return;
  }

  VLOG(2) << fmt::format("{} DoFlush uploading {} remaining blocks", UUID(),
                         remaining.size());

  for (auto& [index, block_data] : remaining) {
    inflight_.fetch_add(1, std::memory_order_relaxed);
    UploadBlockAsync(sid, index, block_data.release());
  }
  remaining.clear();

  // === Step 6: Wait for remaining blocks to complete ===
  if (!WaitInflightWithTimeout(timeout)) {
    LOG(ERROR) << fmt::format("{} DoFlush timeout waiting remaining blocks",
                              UUID());
    FlushDone(Status::Internal("DoFlush timeout waiting remaining"));
    return;
  }

  // === Step 7: Final error check ===
  Status final_status;
  {
    std::lock_guard<std::mutex> lg(error_mutex_);
    final_status = upload_error_;
  }

  uint64_t elapsed_us = butil::cpuwide_time_us() - start_us;
  VLOG(2) << fmt::format(
      "{} DoFlush done, status={}, elapsed_us={}, "
      "total_streamed={}, total_uploaded={}",
      UUID(), final_status.ToString(), elapsed_us,
      total_blocks_streamed_.load(std::memory_order_relaxed),
      total_blocks_uploaded_.load(std::memory_order_relaxed));
  FlushDone(final_status);
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

Slice SliceWriter::GetCommitSlice() {
  int32_t len = 0;
  int32_t chunk_offset = 0;

  {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    len = len_;
    chunk_offset = chunk_offset_;
  }

  Slice slice{
      .id = id_, .size = len, .off = 0, .len = len, .pos = chunk_offset};

  VLOG(4) << fmt::format(
      "{} GetCommitSlices completed, slice: {}, slice_data: {}", UUID(),
      Slice2Str(slice), ToString());
  return slice;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
