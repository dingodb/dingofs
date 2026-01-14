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

#ifndef DINGODB_CLIENT_VFS_COMPACTION_CHUNK_COMPACTION_H_
#define DINGODB_CLIENT_VFS_COMPACTION_CHUNK_COMPACTION_H_

#include <cstdint>

#include "client/vfs/vfs_meta.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

class ChunkCompaction {
 public:
  explicit ChunkCompaction(VFSHub* hub, Ino ino, uint64_t chunk_index)
      : vfs_hub_(hub), ino_(ino), chunk_index_(chunk_index) {}

  ~ChunkCompaction();

  Status Compact(const std::vector<Slice>& slices,
                 std::vector<Slice> out_slices);

 private:
  VFSHub* vfs_hub_;
  const Ino ino_;
  const uint64_t chunk_index_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_COMPACTION_CHUNK_COMPACTION_H_
