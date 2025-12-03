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

#ifndef DINGOFS_CLIENT_VFS_DATA_READER_CHUNK_READER_H_
#define DINGOFS_CLIENT_VFS_DATA_READER_CHUNK_READER_H_

#include <fmt/format.h>

#include <cstdint>
#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "client/vfs/data/chunk.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/reader/reader_common.h"
#include "common/callback.h"
#include "common/io_buffer.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

struct BlockCacheReadReq {
  uint32_t req_index;
  cache::BlockKey key;
  cache::RangeOption option;
  IOBuffer io_buffer;
  const BlockReadReq& block_req;
};

struct ChunkSlices {
  uint32_t version;
  std::vector<Slice> slices;
};

class ChunkReader {
 public:
  ChunkReader(VFSHub* hub, uint64_t fh, const ChunkReadReq& req);

  ~ChunkReader() = default;

  Status Read(ContextSPtr ctx, IOBuffer* out_buf);

 private:
  void DoRead(ContextSPtr ctx, IOBuffer* out_buf, StatusCallback cb);

  Status GetSlices(ContextSPtr ctx, ChunkSlices* chunk_slices);

  static void BlockReadCallback(ContextSPtr ctx, ChunkReader* reader,
                                const BlockCacheReadReq& req,
                                ReaderSharedState& shared, Status s);
  uint64_t GetChunkSize() const;

  uint64_t GetBlockSize() const;

  std::string UUID() const {
    return fmt::format("rreq-{}-chunk-{}", req_.req_id, chunk_.UUID());
  }

  VFSHub* hub_;
  const uint64_t fh_;
  const ChunkReadReq req_;
  const Chunk chunk_;
  const uint64_t block_size_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_CHUNK_READER_H_