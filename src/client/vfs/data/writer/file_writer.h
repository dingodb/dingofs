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

#ifndef DINGOFS_CLIENT_VFS_DATA_WRITER_FILE_WRITER_H_
#define DINGOFS_CLIENT_VFS_DATA_WRITER_FILE_WRITER_H_

#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "client/vfs/data/writer/chunk_writer.h"
#include "client/vfs/data/writer/task/file_flush_task.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

class FileWriter {
 public:
  FileWriter(VFSHub* hub, uint64_t ino) : vfs_hub_(hub), ino_(ino) {}

  ~FileWriter() = default;

  Status Write(const char* buf, uint64_t size, uint64_t offset,
               uint64_t* out_wsize);

  void AsyncFlush(StatusCallback cb);

 private:
  uint64_t GetChunkSize() const;

  ChunkWriter* GetOrCreateChunkWriter(uint64_t chunk_index);

  VFSHub* vfs_hub_;
  const uint64_t ino_;

  std::mutex mutex_;

  // TODO: maybe move chunk_writers_ to FileFlushTask
  // chunk_index -> chunk
  // chunk is used by file/file_flush_task/chunk_flush_task
  // TODO: manage chunk use dec/inc ref mechanism
  // TODO: or maybe transfer chunk ownership to file_flush_task and then
  // transfer ownership to chunk_flush_task
  std::unordered_map<uint64_t, ChunkWriterSPtr> chunk_writers_;
  // TODO: monitor this and add a manager
  // file_flush_id -> FileFlushTask
  std::unordered_map<uint64_t, std::unique_ptr<FileFlushTask>>
      inflight_flush_tasks_;
};

using FileWriterUPtr = std::unique_ptr<FileWriter>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_WRITER_FILE_WRITER_H_