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

#include <glog/logging.h>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>

#include "client/vfs/data/slice/block_data.h"
#include "client/vfs/data/slice/common.h"
#include "client/vfs/vfs_meta.h"
#include "common/callback.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;
class Chunk;

// writing -> flushing -> flushed
class SliceData {
 public:
  explicit SliceData(const SliceDataContext& context, VFSHub* hub,
                     uint64_t chunk_offset)
      : context_(context), vfs_hub_(hub), chunk_offset_(chunk_offset) {}

  ~SliceData();

  Status Write(const char* buf, uint64_t size, uint64_t chunk_offset);

  // prected by chunk, this is should be called only once
  // first call freeze, then call this
  void FlushAsync(StatusCallback cb);

  void GetCommitSlices(std::vector<Slice>& slices);

  uint64_t ChunkOffset() const { return chunk_offset_; }

  uint64_t End() const {
    std::lock_guard<std::mutex> lg(lock_);
    return chunk_offset_ + len_;
  }

  uint64_t Len() const { return len_; }

  void SetFlushed() { flushed_.store(true, std::memory_order_release); }

  bool IsFlushed() const { return flushed_.load(std::memory_order_acquire); }

  std::string UUID() const {
    return fmt::format("slice_data-{}-{}-{}", context_.ino,
                       context_.chunk_index, context_.seq);
  }

  // NOTE: should be called outside lock
  std::string ToString() const {
    std::lock_guard<std::mutex> lg(lock_);
    return ToStringUnlocked();
  }

 private:
  std::string ToStringUnlocked() const {
    return fmt::format(
        "[uuid: {}, chunk_range: [{}-{}], len: {}, id: "
        "{}, flushed: {}, block_data_count: {}]",
        UUID(), chunk_offset_, (chunk_offset_ + len_), len_, id_,
        (flushed_.load(std::memory_order_relaxed) ? "true" : "false"),
        block_datas_.size());
  }

  BlockData* FindOrCreateBlockDataUnlocked(uint64_t block_index,
                                           uint64_t block_offset);

  void BlockDataFlushed(BlockData* block_data, Status status);

  void DoFlush();

  void FlushDone(Status s);

  const SliceDataContext context_;
  VFSHub* vfs_hub_{nullptr};

  mutable std::mutex lock_;
  uint64_t chunk_offset_;
  uint64_t len_{0};
  bool flushing_{false};  // used to prevent multiple flushes
  uint64_t id_{0};        // from mds
  // block_index -> BlockData, this should be immutable
  std::map<uint64_t, BlockDataUPtr> block_datas_;
  StatusCallback flush_cb_;
  Status flush_status_;

  std::atomic_bool flushed_{false};
  std::atomic_uint64_t flush_block_data_count_{0};
};

using SliceDataUPtr = std::unique_ptr<SliceData>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_SLICE_DATA_H_