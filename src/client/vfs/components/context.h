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

#ifndef DINGOFS_SRC_CLIENT_VFS_COMPONENT_CONTEXT_H_
#define DINGOFS_SRC_CLIENT_VFS_COMPONENT_CONTEXT_H_

#include "cache/blockcache/cache_store.h"
#include "client/vfs/vfs_meta.h"
#include "fmt/format.h"

namespace dingofs {
namespace client {
namespace vfs {

enum class WarmupType : uint8_t {
  kWarmupIntime = 0,
  kWarmupManual = 1,
  kWarmupUnknown = 2,
};

struct BlockContext {
  BlockContext(cache::BlockKey block_key, uint64_t block_len)
      : key(block_key), len(block_len) {}
  cache::BlockKey key;
  uint64_t len;
};

struct ChunkContext {
  ChunkContext(uint64_t fs_id, Ino ino, int64_t chunk_idx, int64_t offset,
               int64_t length)
      : fs_id(fs_id),
        ino{ino},
        chunk_idx(chunk_idx),
        offset(offset),
        len(length) {}
  uint64_t fs_id;
  Ino ino;
  int64_t chunk_idx;
  int64_t offset;
  int64_t len;

  std::string ToString() const {
    return fmt::format("Chunk context, ino: {}, chunk: {}-{}-{}", ino,
                       chunk_idx, offset, offset + len);
  }
};

struct WarmupTaskContext {
  WarmupTaskContext(const WarmupTaskContext& task) = default;
  WarmupTaskContext(Ino ino) : task_key(ino), type(WarmupType::kWarmupIntime) {}
  WarmupTaskContext(Ino ino, const std::string& xattr)
      : task_key(ino), type(WarmupType::kWarmupManual), task_inodes(xattr) {}

  WarmupType type;
  Ino task_key;
  // comma separated inodeid('1000023,10000021,...'), provide by dingo-tool
  std::string task_inodes;
};

struct PrefetchContext {
  PrefetchContext(uint64_t ino, int64_t prefetch_offset, uint64_t file_size,
                  uint64_t prefetch_blocks)
      : ino(ino),
        prefetch_offset(prefetch_offset),
        file_size(file_size),
        prefetch_blocks(prefetch_blocks) {}

  uint64_t ino;
  uint64_t prefetch_offset;
  uint64_t file_size;
  uint64_t prefetch_blocks;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_COMPONENT_CONTEXT_H_