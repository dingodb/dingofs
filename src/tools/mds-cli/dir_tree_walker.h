// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_MDS_CLIENT_DIR_TREE_WALKER_H_
#define DINGOFS_MDS_CLIENT_DIR_TREE_WALKER_H_

#include <cstdint>
#include <vector>

#include "dingofs/mds.pb.h"
#include "mds/common/type.h"
#include "tools/mds-cli/owner_router.h"

namespace dingofs {
namespace mds {
namespace client {

// Client-driven directory-tree usage walk. The recursion lives here (in the CLI)
// instead of on the MDS: each directory level is a single-level RPC routed to the
// owning MDS via OwnerRouter, parallelized across a thread pool. This removes the
// server-side bthread stack-depth cap (deep trees no longer fail) and makes the
// result correct across a multi-MDS partition (every level hits its true owner).
struct WalkOptions {
  // Force the authoritative per-level dentry scan (ReadDir) instead of trusting
  // the stored single-level dir-stat. Also implied when the fs has no dir-stats.
  bool strict{false};
  // The fs has dir-stats enabled; without it every level must scan dentries.
  bool dirstats_enabled{false};
  // Parallel fan-out (directories visited concurrently).
  uint32_t threads{50};
};

// Recursive subtree totals. Signed so intermediate subtraction can clamp at zero
// (the rendered values are unsigned).
struct DirAgg {
  int64_t files{0};
  int64_t dirs{0};
  int64_t length{0};
};

// Walk the whole subtree under `root` and aggregate recursive {files, dirs,
// length}; `root` itself is counted as one directory. Returns false when the
// tree could not be fully traversed (an inode had no owner route, or a non
// not-found error); partial totals are still filled and the problem is logged.
bool WalkAggregate(OwnerRouter& router, Ino root, const WalkOptions& opts, DirAgg& out);

// Build a TreeSummary under `root` expanded to `depth` levels (deeper levels fold
// into each node's recursive aggregate), keeping the top `top_n` sub-directories
// per level by length and merging the rest into a synthetic "..." node. Returns
// false on an incomplete traversal (see WalkAggregate).
bool WalkTree(OwnerRouter& router, Ino root, const WalkOptions& opts, uint32_t depth, uint32_t top_n,
              pb::mds::TreeSummary& out);

// Walk every directory under `root` and run a single-level dir-stat check/repair
// on each, routed to its owner; collects the mismatches (sorted by inode).
// Returns false on an incomplete traversal.
bool WalkSyncDirStat(OwnerRouter& router, Ino root, const WalkOptions& opts, bool repair,
                     std::vector<pb::mds::DirStatMismatch>& mismatches);

// Authoritative single-level stat for ONE directory (no recursion). Always does
// the dentry scan (strict): files = direct non-dir children, dirs = direct
// sub-directory count, length = direct FILE child lengths. Routed to ino's owner.
// Returns false on a read failure / no owner (logged); out is zeroed on entry so
// it is always 0 on a false return.
bool ReadDirStatStrict(OwnerRouter& router, Ino ino, DirAgg& out);

// Single-level stat for ONE directory from the maintained counter (stored record
// + owner's unflushed delta), routed to ino's owner. Fast sibling of
// ReadDirStatStrict. Returns false on no owner / read error; out zeroed on entry.
bool ReadDirStatFast(OwnerRouter& router, Ino ino, DirAgg& out);

// Sort `node`'s children by length (desc) and, when top_n>0 and there are more
// than top_n of them, merge the tail into a synthetic "..." child (type FILE)
// aggregating their dirs/files/length. Mirrors the removed server-side collapse.
// Exposed for unit testing.
void CollapseChildrenTopN(pb::mds::TreeSummary& node, uint32_t top_n);

}  // namespace client
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_CLIENT_DIR_TREE_WALKER_H_
