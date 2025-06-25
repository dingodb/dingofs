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

#ifndef DINGODB_CLIENT_VFS_DATA_FILE_H_
#define DINGODB_CLIENT_VFS_DATA_FILE_H_

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "client/vfs/data/chunk.h"
#include "client/vfs/data/ifile.h"
#include "client/vfs/data/task/file_flush_task.h"
#include "common/callback.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

class File : public IFile {
 public:
  File(VFSHub* hub, uint64_t ino) : vfs_hub_(hub), ino_(ino) {}

  ~File() override = default;

  Status Write(const char* buf, uint64_t size, uint64_t offset,
               uint64_t* out_wsize) override;

  Status Read(char* buf, uint64_t size, uint64_t offset,
              uint64_t* out_rsize) override;

  Status Flush() override;

  void AsyncFlush(StatusCallback cb) override;

 private:
  VFSHub* vfs_hub_;
  uint64_t ino_;

  uint64_t GetChunkSize() const;

  Chunk* GetOrCreateChunk(uint64_t chunk_index);

  // when sync fail, we need set file status to error
  Status file_status_;

  std::mutex mutex_;
  // chunk_index -> chunk
  std::unordered_map<uint64_t, std::unique_ptr<Chunk>> chunks_;
  // TODO: monitor this and add a manager
  // file_flush_id -> FileFlushTask
  std::unordered_map<uint64_t, std::unique_ptr<FileFlushTask>>
      inflight_flush_tasks_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_DATA_FILE_H_