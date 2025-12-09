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
#include <mutex>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "client/vfs/data/chunk.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/reader/reader_common.h"
#include "common/callback.h"
#include "common/io_buffer.h"
#include "common/trace/context.h"
#include "common/trace/itrace_span.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

struct BlockCacheReadReq {
  const uint64_t req_id;
  const uint32_t req_index;
  const BlockReadReq block_req;
  const cache::BlockKey key;
  const cache::RangeOption option;
  IOBuffer io_buffer;
  std::unique_ptr<ITraceSpan> read_span;

  std::string UUID() const;
  std::string ToString() const;
};

using BlockCacheReadReqUPtr = std::unique_ptr<BlockCacheReadReq>;

struct ChunkSlices {
  uint32_t version;
  std::vector<Slice> slices;
};

struct ReaderSharedState {
  std::mutex mtx;
  uint64_t total;
  uint64_t num_done;
  Status status;
  std::unique_ptr<ITraceSpan> read_span;
  std::vector<BlockCacheReadReqUPtr> block_cache_reqs;
};

class ChunkReader {
 public:
  ChunkReader(VFSHub* hub, uint64_t fh, const ChunkReadReq& req);

  ~ChunkReader() = default;

  void ReadAsync(ContextSPtr ctx, StatusCallback cb);

  IOBuffer GetDataBuffer() const;

 private:
  void ExecuteAsyncRead();
  void AsyncRange(ContextSPtr ctx, ReaderSharedState* shared,
                  BlockCacheReadReq* block_cache_req);

  std::vector<SliceReadReq> ConvertToSliceReadReqs(
      ContextSPtr ctx, const std::vector<Slice>& slices,
      const FileRange& frange);

  std::vector<BlockReadReq> GetBlockReadReqs(
      ContextSPtr ctx, const std::vector<SliceReadReq>& slice_reqs);

  void ProcessBlockCacheReadReq(ContextSPtr ctx, ReaderSharedState* shared,
                                BlockCacheReadReq* block_cache_req);

  IOBuffer GatherIoBuf(ContextSPtr ctx, ReaderSharedState* shared);
  // delete shared
  void OnAllBlocksComplete(ReaderSharedState* shared);
  void OnBlockReadComplete(ReaderSharedState* shared, BlockCacheReadReq* req,
                           Status s);

  Status GetSlices(ContextSPtr ctx, ChunkSlices* chunk_slices);

  uint64_t GetChunkSize() const;

  uint64_t GetBlockSize() const;

  std::string UUID() const {
    return fmt::format("rreq-{}-chunk-{}", req_.req_id, chunk_.UUID());
  }

  VFSHub* hub_;
  const uint64_t fh_;
  const Chunk chunk_;
  const ChunkReadReq req_;

  mutable std::mutex mtx_;
  StatusCallback cb_;
  std::unique_ptr<ITraceSpan> root_span_;
  IOBuffer data_buf_;
  bool ready_{false};
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_CHUNK_READER_H_