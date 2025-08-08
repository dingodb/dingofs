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

#ifndef DINGOFS_CLIENT_VFS_DATA_READER_FILE_READER_H_
#define DINGOFS_CLIENT_VFS_DATA_READER_FILE_READER_H_

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "client/vfs/data/reader/chunk_reader.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

class FileReader {
 public:
  FileReader(VFSHub* hub, uint64_t ino) : vfs_hub_(hub), ino_(ino) {}

  ~FileReader() = default;

  Status Read(char* buf, uint64_t size, uint64_t offset, uint64_t* out_rsize);

 private:
  uint64_t GetChunkSize() const;
  ChunkReader* GetOrCreateChunkReader(uint64_t chunk_index);

  VFSHub* vfs_hub_;
  const uint64_t ino_;

  std::mutex mutex_;
  // chunk index -> chunk reader
  std::unordered_map<uint64_t, ChunkReaderUptr> chunk_readers_;
};

using FileReaderUPtr = std::unique_ptr<FileReader>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_FILE_READER_H_