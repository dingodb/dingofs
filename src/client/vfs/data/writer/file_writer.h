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

#include <condition_variable>
#include <cstdint>
#include <memory>
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
  FileWriter(VFSHub* hub, uint64_t fh, uint64_t ino)
      : vfs_hub_(hub),
        fh_(fh),
        ino_(ino),
        uuid_(fmt::format("file_writer-{}-{}", ino_, fh_)) {}

  ~FileWriter();

  Status Open();

  void Close();

  Status Write(ContextSPtr ctx, const char* buf, uint64_t size, uint64_t offset,
               uint64_t* out_wsize);

  Status Flush();

  void AcquireRef();

  // caller should ensure ReleaseRef called outside of lock
  void ReleaseRef();

 private:
  uint64_t GetChunkSize() const;

  void AsyncFlush(StatusCallback cb);

  void SchedulePeriodicFlush();
  void RunPeriodicFlush();

  ChunkWriter* GetOrCreateChunkWriter(uint64_t chunk_index);

  void FileFlushTaskDone(uint64_t file_flush_id, StatusCallback cb,
                         Status status);
  VFSHub* vfs_hub_;
  const uint64_t fh_;
  const uint64_t ino_;
  const std::string uuid_;

  std::atomic<int64_t> refs_{0};

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool closed_{false};
  int64_t writers_count_{0};

  // chunk_index -> chunk
  // chunk is used by file/file_flush_task/chunk_flush_task
  // owned by FileWriter
  std::unordered_map<uint64_t, ChunkWriter*> chunk_writers_;
  // TODO: monitor this and add a manager
  // file_flush_id -> FileFlushTask
  std::unordered_map<uint64_t, std::unique_ptr<FileFlushTask>>
      inflight_flush_tasks_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_WRITER_FILE_WRITER_H_