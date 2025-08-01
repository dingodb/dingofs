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
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/reader/reader_common.h"
#include "common/callback.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

struct BlockCacheReadReq {
  cache::BlockKey key;
  cache::RangeOption option;
  IOBuffer io_buffer;
  char* buf_pos;
  const BlockReadReq& block_req;
};

class ChunkReader {
 public:
  ChunkReader(VFSHub* hub, uint64_t ino, uint64_t index);

  ~ChunkReader() = default;

  void ReadAsync(const ChunkReadReq& req, StatusCallback cb);

 private:
  void DoRead(const ChunkReadReq& req, StatusCallback cb);

  static void BlockReadCallback(ChunkReader* reader,
                                const BlockCacheReadReq& req,
                                ReaderSharedState& shared, Status s);
  uint64_t GetChunkSize() const;

  std::string UUID() const {
    return fmt::format("chunk_reader-{}-{}", ino_, index_);
  }

  VFSHub* hub_;
  const uint64_t ino_{0};
  const uint64_t index_{0};
  const uint64_t fs_id_{0};
  const uint64_t chunk_size_{0};
  const uint64_t block_size_{0};
  const uint64_t page_size_{0};

  const uint64_t chunk_start_{0};  // in file offset
  const uint64_t chunk_end_{0};    // in file offset
};

using ChunkReaderUptr = std::shared_ptr<ChunkReader>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_CHUNK_READER_H_