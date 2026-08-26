/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CLIENT_VFS_DATA_READER_CHUNK_READ_OP_H_
#define DINGOFS_CLIENT_VFS_DATA_READER_CHUNK_READ_OP_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "client/vfs/common/read_buf_view.h"
#include "client/vfs/data/chunk.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/reader/chunk_req.h"
#include "common/callback.h"
#include "common/status.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

namespace detail {

// Implementation type for StartChunkRead. Callers should use the free function
// below; the detail namespace keeps the type available for normal declaration /
// definition separation without treating it as a public VFS abstraction.
class ChunkReadOp : public std::enable_shared_from_this<ChunkReadOp> {
 public:
  ChunkReadOp(VFSHub* hub, const ChunkReq& req, StatusCallback callback);
  // The caller keeps a local shared_ptr owner across Run, so inline callbacks
  // cannot destroy the operation midway through the dispatch loop.
  void Run(ContextSPtr ctx, const std::vector<Slice>& slices, ReadBufView dst);

 private:
  struct BlockCacheReadReq {
    BlockCacheReadReq(uint64_t req_id, uint32_t req_index, uint32_t fs_id,
                      uint64_t ino, const BlockReadReq& block_req,
                      ReadBufView dst);
    BlockCacheReadReq(BlockCacheReadReq&& other) noexcept;
    BlockCacheReadReq() = delete;
    BlockCacheReadReq(const BlockCacheReadReq&) = delete;
    BlockCacheReadReq& operator=(const BlockCacheReadReq&) = delete;
    BlockCacheReadReq& operator=(BlockCacheReadReq&&) = delete;

    const uint64_t req_id;
    const uint32_t req_index;
    const uint32_t fs_id;
    const uint64_t ino;
    BlockReadReq block_req;
    ReadBufView dst;
    std::atomic<bool> completed{false};

    std::string UUID() const;
    std::string ToString() const;
  };

  std::vector<BlockReadReq> BuildBlockReadReqs(
      const std::vector<SliceReadReq>& slice_reqs) const;
  void Prepare(const std::vector<Slice>& slices, ReadBufView dst);
  void DispatchAll(ContextSPtr ctx);
  void DispatchOne(ContextSPtr ctx, size_t index);
  void OnBlockDone(size_t index, Status status);
  void Finish();

  std::string UUID() const;

  VFSHub* const hub_;
  const Chunk chunk_;
  const ChunkReq req_;

  std::mutex mutex_;  // guards status_ and callback_
  Status status_;
  StatusCallback callback_;

  std::atomic<uint64_t> remaining_{0};
  // Stable after Prepare: no growth or structural change, so index-based
  // access stays valid across the whole async lifetime.
  std::vector<BlockCacheReadReq> requests_;
};

}  // namespace detail

// Starts one self-owned chunk read operation. The callback is invoked exactly
// once -- possibly inline, before this function returns. Every accepted block
// callback owns the operation until that callback returns.
void StartChunkRead(ContextSPtr ctx, VFSHub* hub, const ChunkReq& req,
                    const std::vector<Slice>& slices, ReadBufView dst,
                    StatusCallback callback);

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_CHUNK_READ_OP_H_
