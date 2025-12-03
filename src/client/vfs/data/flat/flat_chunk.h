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

#ifndef DINGODB_CLIENT_VFS_DATA_FLAT_CHUNK_H_
#define DINGODB_CLIENT_VFS_DATA_FLAT_CHUNK_H_

#include <glog/logging.h>

#include <cstdint>
#include <utility>
#include <vector>

#include "client/vfs/data/common/common.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

class SliceHolder;

class FlatFileChunk {
 public:
  FlatFileChunk(uint64_t fs_id, uint64_t ino, uint64_t index,
                uint64_t chunk_size, uint64_t block_size,
                std::vector<Slice> chunk_slices)
      : fs_id_(fs_id),
        ino_(ino),
        index_(index),
        chunk_size_(chunk_size),
        block_size_(block_size),
        chunk_slices_(std::move(chunk_slices)) {}

  std::vector<BlockReadReq> GenBlockReadReqs() const;

 private:
  const uint64_t fs_id_{0};
  const uint64_t ino_{0};
  const uint64_t index_{0};
  const int64_t chunk_size_{0};
  const int64_t block_size_{0};
  const std::vector<Slice> chunk_slices_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
#endif  // DINGODB_CLIENT_VFS_DATA_FLAT_CHUNK_H_
