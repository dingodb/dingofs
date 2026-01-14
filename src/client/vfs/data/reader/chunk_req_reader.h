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

#ifndef DINGOFS_CLIENT_VFS_DATA_READER_SLICES_READER_H_
#define DINGOFS_CLIENT_VFS_DATA_READER_SLICES_READER_H_

#include <fmt/format.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>

#include "client/vfs/data/chunk.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/reader/chunk_req.h"
#include "common/callback.h"
#include "common/io_buffer.h"
#include "common/trace/context.h"
#include "common/trace/trace_manager.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;
class ChunkReader;

struct BlockCacheReadReq {
  const uint64_t req_id;
  const uint32_t req_index;
  const uint64_t fs_id;
  const uint64_t ino;
  const BlockReadReq block_req;
  IOBuffer io_buffer;

  std::string UUID() const;
  std::string ToString() const;
};

using BlockCacheReadReqUPtr = std::unique_ptr<BlockCacheReadReq>;

struct ReaderSharedState {
  std::atomic<uint64_t> total;
  std::atomic<uint64_t> num_done;

  std::mutex mtx;
  Status status;
  SpanScopeSPtr read_span;
  std::vector<BlockCacheReadReqUPtr> block_cache_reqs;
};

class ChunkReqReader {
 public:
  ChunkReqReader(VFSHub* hub, const ChunkReq& req);

  ~ChunkReqReader() = default;

  void ReadAsync(ContextSPtr ctx, const std::vector<Slice>& slices,
                 StatusCallback cb);

  IOBuffer GetDataBuffer() const;

 private:
  friend class ChunkReader;

  std::vector<BlockReadReq> GetBlockReadReqs(
      const std::vector<SliceReadReq>& slice_reqs);

  void ProcessBlockCacheReadReq(ContextSPtr ctx, ReaderSharedState* shared,
                                BlockCacheReadReq* block_cache_req);

  IOBuffer GatherIoBuf(ContextSPtr ctx, ReaderSharedState* shared);
  // delete shared
  void OnAllBlocksComplete(ReaderSharedState* shared);
  void OnBlockReadComplete(ReaderSharedState* shared, BlockCacheReadReq* req,
                           Status s);

  uint64_t GetChunkSize() const;

  uint64_t GetBlockSize() const;

  std::string UUID() const {
    return fmt::format("rreq-{}-chunk-{}", req_.req_id, chunk_.UUID());
  }

  VFSHub* hub_;
  const Chunk chunk_;
  const ChunkReq req_;

  mutable std::mutex mtx_;
  StatusCallback cb_;
  IOBuffer data_buf_;
  bool ready_{false};
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_SLICES_READER_H_