// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDS_DIR_STAT_DIR_STAT_MANAGER_H_
#define DINGOFS_MDS_DIR_STAT_DIR_STAT_MANAGER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "mds/common/context.h"
#include "mds/common/runnable.h"
#include "mds/common/status.h"
#include "mds/common/type.h"
#include "mds/filesystem/fs_info.h"
#include "mds/filesystem/store_operation.h"
#include "utils/shards.h"

namespace dingofs {
namespace mds {
namespace dir_stat {

// One directory's buffered deltas, summed, plus the timepoint (max logical
// timestamp) of the deltas included -- the unit GetFlushSnapshot hands to the
// flusher. After the flush commits, Compact(ino, timepoint) drops exactly this
// prefix while any delta that arrived during the flush (a strictly larger
// timestamp) survives. Mirrors quota's GetUsage()/CompactDeltaUsage(timepoint).
struct DirStatFlushEntry {
  DirStatDelta delta;
  uint64_t timepoint{0};
};

// Thread-safe in-memory per-directory delta log. Modeled on quota's
// Quota{used,delta_usages_}: each mutation appends a single-level delta tagged
// with a strictly-monotonic logical timestamp; reads fold the un-flushed deltas
// on top of the stored absolute; the flusher snapshots, persists, then compacts
// only the persisted prefix. This time-bounded compaction (rather than a
// blanket buffer drop) is what lets the recompute/repair paths keep deltas that
// post-date their tree scan.
class DirStatManager {
 public:
  DirStatManager(FsInfoSPtr fs_info, OperationProcessorSPtr operation_processor, WorkerSetSPtr worker_set)
      : fs_id_(fs_info->GetFsId()),
        fs_info_(std::move(fs_info)),
        operation_processor_(std::move(operation_processor)),
        worker_set_(std::move(worker_set)) {}
  ~DirStatManager() = default;

  DirStatManager(const DirStatManager&) = delete;
  DirStatManager& operator=(const DirStatManager&) = delete;

  // recompute one directory's single-level stat from the live tree -- supplied
  // by FileSystem (CalcDirStat) since the scan needs the dentry/inode traversal
  // primitives. Used by the seed/repair paths of GetDirStat/SyncDirStat/Flush.
  using RecomputeFn = std::function<Status(Context&, Ino, DirStatEntry&)>;

  // Inject the recompute callback. Set once right after construction (the
  // callback captures the owning FileSystem, which is not yet usable in the
  // member initializer list).
  void SetRecomputeFn(RecomputeFn fn) { recompute_ = std::move(fn); }

  // A directory whose stored dir-stat disagreed with a fresh recompute.
  // `want` is the recomputed (authoritative) value; `got` is the stored record
  // (default-constructed, i.e. all-zero, when `found` is false).
  struct DirStatMismatch {
    Ino ino;
    bool found;
    DirStatEntry want;
    DirStatEntry got;
  };

  // with_pending: fold the owner-local unflushed dir-stat delta onto the stored
  // record so the returned value is real-time. Only the RPC handler (single-
  // directory read) opts in; the internal recursive-summary caller must leave it
  // false because it folds the delta itself (else the delta is double-counted).
  Status GetDirStat(Context& ctx, Ino ino, DirStatEntry& out, bool with_pending = false);

  // Enumerate all dir-stat records stored in the KV store for this fs. Reads the
  // store only (does not fold the in-memory unflushed delta), so a freshly
  // mutated directory shows up after the next background flush. Used by the
  // dashboard dir-stats page.
  Status GetAllDirStats(std::map<Ino, DirStatEntry>& dir_stats);

  // Single-level check/repair for one directory; tree-wide recursion now lives
  // in the client (mds-cli), which calls this per directory routed to the owner.
  Status SyncDirStat(Context& ctx, Ino ino, bool repair, std::vector<DirStatMismatch>& mismatches);

  // Snapshot the in-memory delta log and apply the sums to the store, then drop
  // the persisted prefix. Serialized against itself by `flushing_`.
  Status FlushDirStats();

  // dispatch the accumulation to a worker hashed by `parent` so the request
  // thread never touches the shard lock. Same-parent updates land on the same
  // worker, preserving per-directory ordering. The actual append is UpdateDirStat.
  void AsyncUpdateDirStat(Ino parent, int64_t length_delta, int64_t inode_delta, int64_t dir_delta,
                          const std::string& reason);

  // append one delta (tagged with a fresh logical timestamp) to `parent`'s log.
  void UpdateDirStat(Ino parent, int64_t length_delta, int64_t inode_delta, int64_t dir_delta,
                     const std::string& reason);

  // snapshot every directory's currently-buffered deltas (summed) plus the
  // timepoint of the newest included delta. Does NOT remove anything -- removal
  // happens only via Compact after the flush commits, so a failed flush leaves
  // the log intact and a delta arriving mid-flush is never lost.
  std::map<uint64_t, DirStatFlushEntry> GetFlushSnapshot();

  // drop the prefix of `ino`'s log with time_ns <= timepoint (the entries a just-
  // committed flush/recompute already persisted). Entries with a larger timestamp
  // remain. Erases the ino when its log becomes empty.
  void Compact(Ino ino, uint64_t timepoint);

  // sum of `ino`'s currently-buffered deltas (zero if none). Lets a single-level
  // read fold the un-flushed delta on top of the stored record.
  DirStatDelta GetPending(Ino ino);

  // the largest logical timestamp currently buffered for `ino` (0 if none).
  // Captured before a tree scan so the post-scan Compact keeps deltas that
  // raced in after the scan snapshot.
  uint64_t PeekMaxTimeNs(Ino ino);

  // number of directories currently holding buffered deltas (test-only hook).
  size_t Size();

 private:
  // Read live from fs_info_ so a runtime toggle takes effect without recreating
  // the manager. Mirrors FileSystem::EnableDirStats().
  bool EnableDirStats() const { return fs_info_->EnableDirStats(); }

  const uint32_t fs_id_;

  FsInfoSPtr fs_info_;

  // run dir-stat store operations (read/seed/repair/scan/flush) directly via
  // RunAlone -- all dir-stat ops are non-batch. Mirrors QuotaManager.
  OperationProcessorSPtr operation_processor_;

  // serialize FlushDirStats: it snapshots the in-memory delta log then applies
  // the sums to the store, so two concurrent flushes would both read and apply
  // the same deltas (double count). The cron is single-timer but Destroy/manual
  // paths could overlap it.
  std::atomic<bool> flushing_{false};

  // injected live-tree recompute (FileSystem::CalcDirStat).
  RecomputeFn recompute_;

  // AsyncUpdateDirStat offloads every append onto this worker set (hashed by
  // parent), keeping the shard lock off the request thread entirely.
  WorkerSetSPtr worker_set_;

  // One directory's buffered deltas plus its own logical clock -- the per-
  // directory analogue of quota's Quota{delta_usages_, last_time_ns_}. The
  // timestamp is allocated under the shard lock as max(now, last_time_ns)+1, so
  // the log stays append-ordered == time-ordered (Compact pops a sorted prefix)
  // with no cross-directory atomic clock to contend on.
  struct DirStatLog {
    std::deque<DirStatDeltaEntry> deltas;
    uint64_t last_time_ns{0};
  };

  // Per-directory delta log, sharded (lock striping) so the worker pool applies
  // many parents in parallel without convoying on one lock.
  using Map = absl::flat_hash_map<uint64_t, DirStatLog>;
  constexpr static size_t kShardNum = 128;
  utils::Shards<Map, kShardNum> shard_map_;
};

class UpdateDirStatTask;
using UpdateDirStatTaskSPtr = std::shared_ptr<UpdateDirStatTask>;

class UpdateDirStatTask : public TaskRunnable {
 public:
  UpdateDirStatTask(DirStatManager& dir_stat_manager, Ino parent, int64_t length_delta, int64_t inode_delta,
                    int64_t dir_delta, const std::string& reason)
      : dir_stat_manager_(dir_stat_manager),
        parent_(parent),
        length_delta_(length_delta),
        inode_delta_(inode_delta),
        dir_delta_(dir_delta),
        reason_(reason) {}

  ~UpdateDirStatTask() override = default;

  static UpdateDirStatTaskSPtr New(DirStatManager& dir_stat_manager, Ino parent, int64_t length_delta,
                                   int64_t inode_delta, int64_t dir_delta, const std::string& reason) {
    return std::make_shared<UpdateDirStatTask>(dir_stat_manager, parent, length_delta, inode_delta, dir_delta, reason);
  }

  std::string Type() override { return "UpdateDirStatTask"; }

  void Run() override;

 private:
  DirStatManager& dir_stat_manager_;

  Ino parent_;
  int64_t length_delta_;
  int64_t inode_delta_;
  int64_t dir_delta_;

  const std::string reason_;
};

}  // namespace dir_stat
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_DIR_STAT_DIR_STAT_MANAGER_H_
