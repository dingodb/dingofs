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

#ifndef DINGODB_CLIENT_VFS_DATA_WRITER_FILE_FLUSH_TASK_H_
#define DINGODB_CLIENT_VFS_DATA_WRITER_FILE_FLUSH_TASK_H_

#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <mutex>

#include "common/callback.h"

namespace dingofs {
namespace client {
namespace vfs {

class ChunkWriter;

class FileFlushTask {
 public:
  explicit FileFlushTask(uint64_t ino, uint64_t file_flush_id,
                         std::unordered_map<uint64_t, ChunkWriter*> chunks)
      : ino_(ino),
        file_flush_id_(file_flush_id),
        chunk_writers_(std::move(chunks)) {}

  ~FileFlushTask() = default;

  void RunAsync(StatusCallback cb);

  std::string UUID() const {
    return fmt::format("file_flush_task-{}-{}", file_flush_id_, ino_);
  }

  std::string ToString() const {
    return fmt::format("(uuid: {}, chunk_writers_size: {})", UUID(),
                       chunk_writers_.size());
  }

 private:
  void ChunkFlushed(uint64_t chunk_index, Status status);

  const uint64_t ino_;
  const uint64_t file_flush_id_;

  std::atomic_uint64_t flusing_chunk_{0};

  mutable std::mutex mutex_;
  // NOTICE: chunk_writers_ will be moved to local variable to_flush
  std::unordered_map<uint64_t, ChunkWriter*> chunk_writers_;
  StatusCallback cb_;
  Status status_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_DATA_WRITER_FILE_FLUSH_TASK_H_
