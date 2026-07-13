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

#ifndef DINGOFS_CLIENT_VFS_DATA_SLICE_SLICE_WRITER_H_
#define DINGOFS_CLIENT_VFS_DATA_SLICE_SLICE_WRITER_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "client/vfs/data/slice/block_data.h"
#include "client/vfs/data/slice/common.h"
#include "client/vfs/data/slice/flush_barrier.h"
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
class SliceWriter : public std::enable_shared_from_this<SliceWriter> {
 public:
  explicit SliceWriter(const SliceDataContext& context, VFSHub* hub,
                       int32_t chunk_offset)
      : context_(context), vfs_hub_(hub), chunk_offset_(chunk_offset) {}

  ~SliceWriter() = default;

  Status Write(ContextSPtr ctx, const char* buf, int32_t size,
               int32_t chunk_offset);

  // Should be called only once (protected by ChunkWriter).
  void FlushAsync(StatusCallback cb);

  // Trigger async slice_id pre-allocation on the write background executor.
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

  bool IsFlushCompleted() const {
    return flush_completion_state_->IsCompleted();
  }

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
        "{}, flush_completed: {}, block_data_count: {})",
        UUID(), chunk_offset_, (chunk_offset_ + len_), len_,
        slice_id_state_->Get(),
        (flush_completion_state_->IsCompleted() ? "true" : "false"),
        block_datas_.size());
  }

  // Lookup only -- never creates. Returns nullptr if the block is absent.
  BlockData* FindBlockDataUnlocked(uint32_t block_index);

  // Always creates a fresh BlockData (caller must have checked it is absent);
  // the caller tracks the new index so the Write transaction can roll it back.
  BlockData* CreateBlockDataUnlocked(uint32_t block_index,
                                     int32_t block_offset);

  void FlushUpTo(int32_t written_len);  // caller holds write_flush_mutex_
  void SubmitBlockUpload(uint64_t slice_id, uint32_t block_index,
                         BlockData* block_data);
  static void HandleUploadCompletion(std::shared_ptr<FlushBarrier> barrier,
                                     BlockData* block_data, std::string uuid,
                                     std::string store_key, Status status);
  StatusCallback MakeFlushCompletion(StatusCallback cb);
  Status GetOrAllocateSliceId(ContextSPtr ctx, uint64_t* slice_id);
  std::map<uint32_t, BlockDataUPtr> TakeRemainingBlocks();
  static std::vector<FlushBarrier::BlockInfo> DescribeUploads(
      const std::map<uint32_t, BlockDataUPtr>& blocks);
  void SubmitUploads(uint64_t slice_id,
                     std::map<uint32_t, BlockDataUPtr> blocks);
  void DoFlush(StatusCallback cb);

  struct SliceIdState {
    uint64_t Get() const { return id.load(std::memory_order_acquire); }

    bool Publish(uint64_t value) {
      uint64_t expected = 0;
      return id.compare_exchange_strong(expected, value,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire);
    }

    std::atomic<uint64_t> id{0};
  };

  // Kept separately from SliceWriter so a queued completion can record its
  // delivery without retaining the writer or dereferencing it after teardown.
  struct FlushCompletionState {
    bool IsCompleted() const {
      return completed.load(std::memory_order_acquire);
    }

    void MarkCompleted() { completed.store(true, std::memory_order_release); }

    std::atomic_bool completed{false};
  };

  // --- immutable ---
  const SliceDataContext context_;
  VFSHub* vfs_hub_{nullptr};
  // Set in ctor, never changes: slice is forward-append only
  // (CHECK_EQ(chunk_offset, End()) in Write).
  const int32_t chunk_offset_;

  // --- guarded by write_flush_mutex_ ---
  mutable std::mutex write_flush_mutex_;
  int32_t len_{0};
  // One-way latch: FlushAsync has claimed this writer, so its write epoch is
  // closed even if DoFlush has not yet run on FlushExecutor.
  bool flush_requested_{false};
  std::map<uint32_t, BlockDataUPtr> block_datas_;

  std::shared_ptr<SliceIdState> slice_id_state_{
      std::make_shared<SliceIdState>()};
  std::shared_ptr<FlushBarrier> flush_barrier_{
      std::make_shared<FlushBarrier>(context_.UUID())};
  std::shared_ptr<FlushCompletionState> flush_completion_state_{
      std::make_shared<FlushCompletionState>()};
};

using SliceWriterPtr = std::shared_ptr<SliceWriter>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_SLICE_SLICE_WRITER_H_
