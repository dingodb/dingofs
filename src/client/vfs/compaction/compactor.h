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

#ifndef DINGODB_CLIENT_VFS_COMPACTION_COMPACTOR_H_
#define DINGODB_CLIENT_VFS_COMPACTION_COMPACTOR_H_

#include <cstdint>

#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

class Compactor {
 public:
  virtual ~Compactor() = default;

  virtual Status Start() = 0;

  virtual Status Stop() = 0;

  virtual Status Compact(ContextSPtr ctx, Ino ino, int64_t chunk_index,
                         const std::vector<Slice>& slices,
                         std::vector<Slice>& out_slices) = 0;

  virtual Status ForceCompact(ContextSPtr ctx, Ino ino, int64_t chunk_index,
                              const std::vector<Slice>& slices,
                              std::vector<Slice>& out_slices) = 0;

  // Best-effort cleanup for slices produced by compaction but never committed
  // to metadata. Callers must never pass committed slices: their blocks are
  // live data. Two traps for the commit path:
  // - An unknown-outcome commit failure (e.g. CompactChunk RPC timeout) must
  //   NOT be cleaned up: the MDS may have accepted the slices, and deleting
  //   their blocks would destroy committed data. Leak instead.
  // - Compact() folds the skipped prefix of the input into out_slices, so on
  //   commit failure the caller must filter out_slices down to the ids absent
  //   from the input before passing them here.
  virtual Status CleanupUncommittedSlices(ContextSPtr ctx,
                                          const std::vector<Slice>& slices) = 0;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_COMPACTION_COMPACTOR_H_
