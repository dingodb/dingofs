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

#ifndef DINGOFS_CLIENT_VFS_DATA_CHUNK_H_
#define DINGOFS_CLIENT_VFS_DATA_CHUNK_H_

#include <cstdint>
#include <mutex>

#include "cache/blockcache/cache_store.h"
#include "client/common/status.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

class Chunk {
 public:
  Chunk(VFSHub* hub, uint64_t ino, uint64_t index);

  ~Chunk() = default;

  // chunk_offset is the offset in the chunk, not in the file
  Status Write(const char* buf, uint64_t size, uint64_t chunk_offset);

  // chunk_offset is the offset in the chunk, not in the file
  Status Read(char* buf, uint64_t size, uint64_t chunk_offset);

  std::uint64_t Start() const { return chunk_start_; }

  std::uint64_t End() const { return chunk_end_; }

 private:
  Status WriteToBlockCache(const cache::blockcache::BlockKey& key,
                           const cache::blockcache::Block& block,
                           cache::blockcache::BlockContext ctx);

  Status AllockChunkId(uint64_t* chunk_id);

  Status CommitSlices(const std::vector<Slice>& slices);

  VFSHub* hub_{nullptr};
  uint64_t ino_{0};
  uint64_t index_{0};

  uint64_t fs_id_{0};
  uint64_t chunk_size_{0};
  uint64_t block_size_{0};

  const uint64_t chunk_start_{0};  // in file offset
  const uint64_t chunk_end_{0};    // in file offset
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_CHUNK_H_