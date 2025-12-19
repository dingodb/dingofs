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

#include "client/vfs/data/writer/task/file_flush_task.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>

#include "client/vfs/data/writer/chunk_writer.h"
#include "common/callback.h"

namespace dingofs {
namespace client {
namespace vfs {

void FileFlushTask::ChunkFlushed(uint64_t chunk_index, Status status) {
  if (!status.ok()) {
    LOG(WARNING) << fmt::format(
        "{} ChunkFlushed Failed to flush chunk_index: {}, status: {}", UUID(),
        chunk_index, status.ToString());
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (status_.ok()) {
        status_ = status;
      }
    }
  }

  if (flusing_chunk_.fetch_sub(1) == 1) {
    Status tmp;
    StatusCallback cb;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      cb_.swap(cb);
      tmp = status_;
    }

    cb(tmp);

    VLOG(4) << fmt::format("End status: {}", tmp.ToString());
  }
}

void FileFlushTask::RunAsync(StatusCallback cb) {
  VLOG(4) << fmt::format("{} Start file_flush_task: {}", UUID(), ToString());

  std::unordered_map<uint64_t, ChunkWriter*> to_flush;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    to_flush.swap(chunk_writers_);
  }

  if (to_flush.empty()) {
    VLOG(1) << fmt::format("{} End directly because no chunks to flush",
                           UUID());
    cb(Status::OK());
    return;
  }

  flusing_chunk_.store(to_flush.size(), std::memory_order_relaxed);
  DCHECK_GT(flusing_chunk_.load(), 0);

  {
    std::lock_guard<std::mutex> lock(mutex_);
    cb_.swap(cb);
    status_ = Status::OK();
  }

  for (const auto& iter : to_flush) {
    uint64_t chunk_index = iter.first;
    VLOG(4) << fmt::format("{} Flushing chunk_index: {}", UUID(), chunk_index);

    ChunkWriter* chunk_writer = iter.second;
    CHECK_NOTNULL(chunk_writer);

    chunk_writer->FlushAsync([this, chunk_writer, chunk_index](Status status) {
      ChunkFlushed(chunk_index, std::move(status));
    });
  }
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
