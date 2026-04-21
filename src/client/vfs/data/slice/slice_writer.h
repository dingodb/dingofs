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

#ifndef DINGOFS_CLIENT_VFS_DATA_SLICE_DATA_H_
#define DINGOFS_CLIENT_VFS_DATA_SLICE_DATA_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>

#include "client/vfs/data/slice/block_data.h"
#include "client/vfs/data/slice/common.h"
#include "client/vfs/vfs_meta.h"
#include "common/callback.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "fmt/format.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

// Streaming upload: write-full-then-upload pipeline.
// Block is uploaded as soon as it is full during Write().
// DoFlush() handles remaining partial blocks.
class SliceWriter {
 public:
  explicit SliceWriter(const SliceDataContext& context, VFSHub* hub,
                       int32_t chunk_offset)
      : context_(context), vfs_hub_(hub), chunk_offset_(chunk_offset) {}

  ~SliceWriter() = default;

  // --- reference counting (prevents use-after-free on async callbacks) ---
  void IncRef();
  void DecRef();

  Status Write(ContextSPtr ctx, const char* buf, int32_t size,
               int32_t chunk_offset);

  // Should be called only once (protected by ChunkWriter).
  void FlushAsync(StatusCallback cb);

  // Trigger async slice_id pre-allocation on BGExecutor.
  // Called by ChunkWriter after construction.
  void StartPrepareSliceId();

  Slice GetCommitSlice();

  int32_t ChunkOffset() const { return chunk_offset_; }

  int32_t End() const {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    return chunk_offset_ + len_;
  }

  int32_t Len() const {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    return len_;
  }

  bool IsFlushed() const { return flushed_.load(std::memory_order_relaxed); }

  std::string UUID() const {
    return fmt::format("slice_data-{}", context_.UUID());
  }

  uint64_t Seq() const { return context_.seq; }

  // NOTE: should be called outside lock
  std::string ToString() const {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    return ToStringUnlocked();
  }

 private:
  std::string ToStringUnlocked() const {
    return fmt::format(
        "(uuid: {}, chunk_range: [{}-{}], len: {}, slice_id: "
        "{}, flushed: {}, block_data_count: {})",
        UUID(), chunk_offset_, (chunk_offset_ + len_), len_,
        slice_id_.load(std::memory_order_relaxed),
        (flushed_.load(std::memory_order_relaxed) ? "true" : "false"),
        block_datas_.size());
  }

  BlockData* FindOrCreateBlockDataUnlocked(uint32_t block_index,
                                           int32_t block_offset);

  void PrepareSliceId();
  void FlushUpTo(int32_t written_len);  // caller holds write_flush_mutex_
  void UploadBlockAsync(uint64_t slice_id, uint32_t block_index,
                        BlockData* block_data);
  void OnBlockUploaded(BlockData* block_data, Status s);
  void DoFlush();
  void FlushDone(Status s);
  bool WaitInflightWithTimeout(std::chrono::seconds timeout);

  // --- immutable ---
  const SliceDataContext context_;
  VFSHub* vfs_hub_{nullptr};
  // Set in ctor, never changes: slice is forward-append only
  // (CHECK_EQ(chunk_offset, End()) in Write).
  const int32_t chunk_offset_;

  // --- guarded by write_flush_mutex_ ---
  mutable std::mutex write_flush_mutex_;
  int32_t len_{0};
  bool flushing_{false};
  std::map<uint32_t, BlockDataUPtr> block_datas_;
  StatusCallback flush_cb_;

  // --- guarded by inflight_mutex_ ---
  std::mutex inflight_mutex_;
  std::condition_variable inflight_cv_;

  // --- guarded by error_mutex_ ---
  std::mutex error_mutex_;
  Status upload_error_;

  // --- atomic (lock-free) ---
  std::atomic<uint64_t> slice_id_{0};
  std::atomic<uint32_t> inflight_{0};
  std::atomic_bool flushed_{false};

  // --- reference counting ---
  // creator must call IncRef() after construction
  std::atomic<int32_t> refs_{0};

  // --- observability counters (atomic, lock-free) ---
  std::atomic<uint64_t> total_blocks_streamed_{0};
  std::atomic<uint64_t> total_blocks_uploaded_{0};
};

// SliceWriter uses manual ref counting (IncRef/DecRef).
// Creator must call IncRef() after construction, DecRef() to release.
// Async callbacks (UploadBlockAsync) hold additional refs.
// When last DecRef reaches 0, object is deleted.
using SliceWriterPtr = SliceWriter*;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_SLICE_DATA_H_
