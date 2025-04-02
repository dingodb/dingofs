// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/vfs_old/service/flat_file_util.h"

#include "dingofs/metaserver.pb.h"

namespace dingofs {
namespace client {

using pb::metaserver::Inode;

FlatFile InodeWrapperToFlatFile(std::shared_ptr<InodeWrapper> inode_wrapper,
                                uint64_t chunk_size, uint64_t block_size) {
  Inode inode = inode_wrapper->GetInode();
  FlatFile flat_file(inode.fsid(), inode.inodeid(), chunk_size, block_size);
  for (const auto& chunk : inode.s3chunkinfomap()) {
    uint64_t chunk_index = chunk.first;

    for (int i = 0; i < chunk.second.s3chunks_size(); i++) {
      const auto& chunk_info = chunk.second.s3chunks(i);
      VLOG(12) << "Insert chunk info, " << chunk_info.DebugString();
      flat_file.InsertChunkInfo(chunk_index, chunk_info);
    }
  }
  return flat_file;
}

}  // namespace client

}  // namespace dingofs