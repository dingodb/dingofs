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

#include "client/vfs/common/read_buf_view.h"
#include "client/vfs/data/reader/chunk_req.h"
#include "common/callback.h"
#include "common/status.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

struct ChunkSlices {
  uint32_t version;
  std::vector<Slice> slices;
};

// Synchronous slice-lookup plus chunk-read-operation factory. Owns no async
// state: after ReadAsync() submits the operation the object can be destroyed
// at any time -- the operation (ChunkReadOp) carries its own lifetime.
// Suitable as a stack object.
class ChunkReader {
 public:
  ChunkReader(VFSHub* hub, uint64_t fh, const ChunkReq& req);

  ~ChunkReader() = default;

  // Reads req_.frange into dst, invoking cb exactly once (possibly inline).
  void ReadAsync(ContextSPtr ctx, ReadBufView dst, StatusCallback cb);

 private:
  Status GetSlices(ContextSPtr ctx, ChunkSlices* chunk_slices);

  VFSHub* hub_;
  const uint64_t fh_;
  const ChunkReq req_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_CHUNK_READER_H_
