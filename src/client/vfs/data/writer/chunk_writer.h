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

#ifndef DINGOFS_CLIENT_VFS_DATA_WRITER_CHUNK_WRITER_H_
#define DINGOFS_CLIENT_VFS_DATA_WRITER_CHUNK_WRITER_H_

#include <fmt/format.h>

#include <cstdint>
#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "client/meta/vfs_meta.h"
#include "client/vfs/data/slice/slice_data.h"
#include "client/vfs/data/writer/task/chunk_flush_task.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

static std::atomic<uint64_t> slice_seq_id_gen{1};
static std::atomic<uint64_t> chunk_flush_id_gen{1};

class VFSHub;

class ChunkWriter : public std::enable_shared_from_this<ChunkWriter> {
 public:
  ChunkWriter(VFSHub* hub, uint64_t ino, uint64_t index);

  ~ChunkWriter();

  // chunk_offset is the offset in the chunk, not in the file
  Status Write(const char* buf, uint64_t size, uint64_t chunk_offset);

  // All slice data can be flushed in concurrent, but commit must be in order.
  // If slice data is empty, the empty flush task must be submitted
  // to ensure that the previous flush task is completed and submitted
  void FlushAsync(StatusCallback cb);

  void TriggerFlush();

 private:
  // proteted by mutex_
  struct FlushTask {
    const uint64_t chunk_flush_id{0};
    bool done{false};
    Status status;
    const StatusCallback cb{nullptr};
    std::shared_ptr<ChunkWriter> chunk{nullptr};
    std::unique_ptr<ChunkFlushTask> chunk_flush_task{nullptr};

    std::string UUID() const {
      CHECK_NOTNULL(chunk_flush_task);
      return chunk_flush_task->UUID();
    }

    std::string ToString() const {
      return fmt::format(
          "(chunk_flush_id: {}, done: {}, status: {}, task: {})",
          chunk_flush_id, done ? "true" : "false", status.ToString(),
          chunk_flush_task ? chunk_flush_task->ToString() : "nullptr");
    }
  };


  std::string UUID() const {
    return fmt::format("chunk_reader-{}-{}", ino_, index_);
  }

  Status DirectWrite(const char* buf, uint64_t size, uint64_t chunk_offset);

  Status BufferWrite(const char* buf, uint64_t size, uint64_t chunk_offset);

  uint64_t GetChunkSize() const;

  Status WriteToBlockCache(const cache::BlockKey& key,
                           const cache::Block& block, cache::PutOption option);

  Status AllockChunkId(uint64_t* chunk_id);

  Status CommitSlices(const std::vector<Slice>& slices);

  std::unique_ptr<SliceData> FindWritableSliceUnLocked(uint64_t chunk_pos,
                                                       uint64_t size);
  std::unique_ptr<SliceData> CreateSliceUnlocked(uint64_t chunk_pos);
  std::unique_ptr<SliceData> FindOrCreateSliceUnlocked(uint64_t chunk_pos,
                                                       uint64_t size);

  void DoFlushAsync(StatusCallback cb, uint64_t chunk_flush_id);
  void FlushTaskDone(FlushTask* flush_task, Status s);

  Status GetErrorStatus() const {
    std::lock_guard<std::mutex> lg(mutex_);
    return error_status_;
  }

  // This is used to mark the chunk as error when some operation fails,
  // If the current error status is ok, it will be set to the given status.
  // If the current error status is not ok, it will not be changed.
  void MarkErrorStatus(const Status& status) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (error_status_.ok()) {
      error_status_ = status;
    }
  }

  VFSHub* hub_;
  const uint64_t ino_{0};
  const uint64_t index_{0};
  const uint64_t fs_id_{0};
  const uint64_t chunk_size_{0};
  const uint64_t block_size_{0};
  const uint64_t page_size_{0};

  const uint64_t chunk_start_{0};  // in file offset
  const uint64_t chunk_end_{0};    // in file offset

  mutable std::mutex mutex_;
  std::condition_variable writer_cv_;
  std::unique_ptr<SliceData> writing_slice_{nullptr};
  // TODO: maybe use std::vector
  // seq_id -> slice datj
  std::map<uint64_t, std::unique_ptr<SliceData>> slices_;
  std::deque<FlushTask*> flush_queue_;
  static FlushTask fake_header_;
  // when this not ok, all write and flush should return error
  Status error_status_;
};

using ChunkWriterUPtr = std::shared_ptr<ChunkWriter>;
using ChunkWriterSPtr = std::shared_ptr<ChunkWriter>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_WRITER_CHUNK_WRITER_H_