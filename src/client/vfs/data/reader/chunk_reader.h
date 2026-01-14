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

#include <cstdint>
#include <memory>

#include "client/vfs/data/reader/chunk_req_reader.h"
#include "client/vfs/data/reader/chunk_req.h"
#include "common/callback.h"
#include "common/io_buffer.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

struct ChunkSlices {
  uint32_t version;
  std::vector<Slice> slices;
};

class ChunkReader {
 public:
  ChunkReader(VFSHub* hub, uint64_t fh, const ChunkReq& req);

  ~ChunkReader() = default;

  void ReadAsync(ContextSPtr ctx, StatusCallback cb);

  IOBuffer GetDataBuffer() const;

 private:
  Status GetSlices(ContextSPtr ctx, ChunkSlices* chunk_slices);

  std::string UUID() const { return reader_->UUID(); }

  VFSHub* hub_;
  const uint64_t fh_;
  std::unique_ptr<ChunkReqReader> reader_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_CHUNK_READER_H_