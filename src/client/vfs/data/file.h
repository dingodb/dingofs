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

#include "client/vfs/data/ifile.h"
#include "client/vfs/data/reader/file_reader.h"
#include "client/vfs/data/writer/file_writer.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

class File : public IFile {
 public:
  File(VFSHub* hub, uint64_t fh, int64_t ino);

  ~File() override;

  Status Open() override;

  void Close() override;

  Status Write(ContextSPtr ctx, const char* buf, uint64_t size, uint64_t offset,
               uint64_t* out_wsize) override;

  Status Read(ContextSPtr ctx, DataBuffer* data_buffer, uint64_t size,
              uint64_t offset, uint64_t* out_rsize) override;

  void Invalidate(int64_t offset, int64_t size) override;

  Status Flush() override;

 private:
  Status PreCheck();
  uint64_t GetChunkSize() const;

  VFSHub* vfs_hub_;
  const uint64_t fh_;
  const uint64_t ino_;
  const std::string uuid_;

  FileWriter* file_writer_;
  FileReader* file_reader_;

  std::mutex mutex_;
  // when sync fail, we need set file status to error
  Status file_status_;
};

using FileUPtr = std::unique_ptr<File>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_DATA_FILE_H_