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

#ifndef DINGOFS_SRC_CLIENT_VFS_PREFETCH_UTILS_H_
#define DINGOFS_SRC_CLIENT_VFS_PREFETCH_UTILS_H_

#include <glog/logging.h>

#include <vector>

#include "cache/blockcache/cache_store.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/common/data_utils.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "fmt/format.h"

namespace dingofs {
namespace client {
namespace vfs {

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

inline std::vector<ChunkContext> File2Chunk(const uint64_t fs_id, const Ino ino,
                                            const uint64_t offset,
                                            const uint64_t len,
                                            const uint64_t chunk_size) {
  std::vector<ChunkContext> chunk_contexts;

  uint64_t chunk_idx = offset / chunk_size;
  uint64_t chunk_offset = offset % chunk_size;
  uint64_t prefetch_size = len;

  while (prefetch_size > 0) {
    uint64_t chunk_fetch_size =
        std::min(prefetch_size, chunk_size - chunk_offset);
    ChunkContext chunk_context(fs_id, ino, chunk_idx, chunk_offset,
                               chunk_fetch_size);
    chunk_contexts.push_back(chunk_context);

    chunk_idx++;
    chunk_offset = 0;
    prefetch_size -= chunk_fetch_size;
  }

  return chunk_contexts;
}

inline std::vector<BlockContext> Chunk2Block(ContextSPtr ctx, VFSHub* vfs_hub,
                                             const ChunkContext& req,
                                             const int64_t chunk_size,
                                             const int64_t block_size) {
  std::vector<Slice> slices;
  uint64_t chunk_version = 0;

  Status status = vfs_hub->GetMetaSystem().ReadSlice(
      ctx, req.ino, req.chunk_idx, 0, &slices, chunk_version);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "Read slice failed, ino: {}, chunk: {}, status: {}.", req.ino,
        req.chunk_idx, status.ToString());

    return {};
  }

  FileRange range = {(req.chunk_idx * chunk_size) + req.offset, req.len};
  std::vector<SliceReadReq> slice_reqs = ProcessReadRequest(slices, range);

  std::vector<BlockReadReq> block_reqs;
  for (auto& slice_req : slice_reqs) {
    VLOG(9) << "Read slice_seq : " << slice_req.ToString();

    if (slice_req.slice.has_value() && !slice_req.slice.value().is_zero) {
      std::vector<BlockReadReq> reqs = ConvertSliceReadReqToBlockReadReqs(
          slice_req, req.fs_id, req.ino, chunk_size, block_size);

      block_reqs.insert(block_reqs.end(), std::make_move_iterator(reqs.begin()),
                        std::make_move_iterator(reqs.end()));
    }
  }

  std::vector<BlockContext> block_contexts;
  for (const auto& block_req : block_reqs) {
    cache::BlockKey key(req.fs_id, req.ino, block_req.block.slice_id,
                        block_req.block.index, block_req.block.version);
    block_contexts.emplace_back(key, block_req.block.block_len);
  }

  return block_contexts;
}

inline std::vector<BlockContext> RemoveDuplicateBlocks(
    const std::vector<BlockContext>& blocks) {
  std::unordered_set<std::string> seen_filenames;
  std::vector<BlockContext> result;

  seen_filenames.reserve(blocks.size());
  result.reserve(blocks.size());

  for (const auto& block : blocks) {
    auto [_, ok] = seen_filenames.insert(block.key.Filename());
    if (ok) {
      result.push_back(block);
    }
  }

  return result;
}

inline std::vector<BlockContext> FileRange2BlockKey(ContextSPtr ctx,
                                                    VFSHub* vfs_hub, Ino ino,
                                                    const uint64_t offset,
                                                    const uint64_t len) {
  CHECK_NOTNULL(vfs_hub);
  uint64_t fs_id = vfs_hub->GetFsInfo().id;
  uint64_t chunk_size = vfs_hub->GetFsInfo().chunk_size;
  uint64_t block_size = vfs_hub->GetFsInfo().block_size;

  std::vector<ChunkContext> chunk_contexts =
      File2Chunk(fs_id, ino, offset, len, chunk_size);

  std::vector<BlockContext> block_contexts;
  for (const auto& chunk_context : chunk_contexts) {
    std::vector<BlockContext> block_contexts_temp =
        Chunk2Block(ctx, vfs_hub, chunk_context, chunk_size, block_size);

    block_contexts.insert(block_contexts.end(),
                          std::make_move_iterator(block_contexts_temp.begin()),
                          std::make_move_iterator(block_contexts_temp.end()));
  }

  return RemoveDuplicateBlocks(block_contexts);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_PREFETCH_UTILS_H_