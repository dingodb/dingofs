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

#ifndef DINGODB_CLIENT_VFS_DATA_FLAT_FILE_H_
#define DINGODB_CLIENT_VFS_DATA_FLAT_FILE_H_

#include <glog/logging.h>
#include <sys/types.h>

#include <cstdint>
#include <map>
#include <memory>

#include "client/vfs/data/common/common.h"
#include "client/vfs/data/flat/flat_chunk.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

class FlatFile {
 public:
  FlatFile(uint64_t fs_id, uint64_t ino, uint64_t chunk_size,
           uint64_t block_size)
      : fs_id_(fs_id),
        ino_(ino),
        chunk_size_(chunk_size),
        block_size_(block_size) {}

  ~FlatFile() = default;

  uint64_t GetFsId() const { return fs_id_; }

  uint64_t GetIno() const { return ino_; }

  uint64_t GetChunkSize() const { return chunk_size_; }

  uint64_t GetBlockSize() const { return block_size_; }

  // if chunk has no slices, must fill an empty slices
  void FillChunk(uint64_t index, std::vector<Slice> chunk_slices);

  std::vector<BlockReadReq> GenBlockReadReqs() const;

 private:
  const uint64_t fs_id_{0};
  const uint64_t ino_{0};
  const uint64_t chunk_size_{0};
  const uint64_t block_size_{0};

  std::map<uint64_t, std::unique_ptr<FlatFileChunk>> chunk_map_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
#endif  // DINGODB_CLIENT_VFS_DATA_FLAT_FILE_H_
