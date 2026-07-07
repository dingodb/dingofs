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

#include "mds/statistics/dir_stat_manager.h"

#include <algorithm>
#include <map>
#include <set>
#include <vector>

#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/codec.h"
#include "mds/common/synchronization.h"
#include "mds/common/tracing.h"
#include "mds/common/trash.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {
namespace dir_stat {

void DirStatManager::AsyncUpdateDirStat(Ino parent, int64_t length_delta, int64_t inode_delta, int64_t dir_delta,
                                        const std::string& reason) {
  auto task = UpdateDirStatTask::New(*this, parent, length_delta, inode_delta, dir_delta, reason);

  if (!worker_set_->ExecuteHash(parent, task)) {
    LOG(ERROR) << fmt::format(
        "[dirstat.{}.{}] async update dir stat fail, length_delta({}) inode_delta({}) dir_delta({}) reason({}).",
        fs_id_, parent, length_delta, inode_delta, dir_delta, reason);
  }
}

void DirStatManager::UpdateDirStat(Ino parent, int64_t length_delta, int64_t inode_delta, int64_t dir_delta,
                                   const std::string& reason) {
  LOG_DEBUG << fmt::format(
      "[dirstat.{}.{}] update dir stat, length_delta({}) inode_delta({}) dir_delta({}) reason({}).", fs_id_, parent,
      length_delta, inode_delta, dir_delta, reason);

  shard_map_.withWLock(
      [parent, length_delta, inode_delta, dir_delta](Map& map) {
        auto& log = map[parent];
        uint64_t now = utils::TimestampNs();
        uint64_t time_ns = std::max(now, log.last_time_ns) + 1;
        log.last_time_ns = time_ns;

        DirStatDeltaEntry entry;
        entry.length = length_delta;
        entry.inodes = inode_delta;
        entry.dirs = dir_delta;
        entry.time_ns = time_ns;
        log.deltas.push_back(entry);
      },
      parent);
}

void UpdateDirStatTask::Run() {
  dir_stat_manager_.UpdateDirStat(parent_, length_delta_, inode_delta_, dir_delta_, reason_);
}

std::map<uint64_t, DirStatFlushEntry> DirStatManager::GetFlushSnapshot() {
  std::map<uint64_t, DirStatFlushEntry> out;
  shard_map_.iterate([&out](Map& map) {
    for (const auto& [ino, log] : map) {
      if (log.deltas.empty()) continue;
      DirStatFlushEntry fe;
      for (const auto& e : log.deltas) {
        fe.delta.length += e.length;
        fe.delta.inodes += e.inodes;
        fe.delta.dirs += e.dirs;
        fe.timepoint = e.time_ns;  // deque is time-ordered, so the last is the max
      }
      out.emplace(ino, fe);
    }
  });
  return out;
}

void DirStatManager::Compact(Ino ino, uint64_t timepoint) {
  shard_map_.withWLock(
      [ino, timepoint](Map& map) {
        auto it = map.find(ino);
        if (it == map.end()) return;
        auto& deque = it->second.deltas;
        while (!deque.empty() && deque.front().time_ns <= timepoint) {
          deque.pop_front();
        }
        // Drop the whole entry (and its retained clock) once empty: with a
        // monotonic physical clock the next append still gets a fresh, larger
        // timestamp, and timestamps are only ever compared within a live deque.
        if (deque.empty()) map.erase(it);
      },
      ino);
}

DirStatDelta DirStatManager::GetPending(Ino ino) {
  DirStatDelta out;
  shard_map_.withRLock(
      [ino, &out](Map& map) {
        auto it = map.find(ino);
        if (it == map.end()) return;
        for (const auto& e : it->second.deltas) {
          out.length += e.length;
          out.inodes += e.inodes;
          out.dirs += e.dirs;
        }
      },
      ino);
  return out;
}

uint64_t DirStatManager::PeekMaxTimeNs(Ino ino) {
  uint64_t out = 0;
  shard_map_.withRLock(
      [ino, &out](Map& map) {
        auto it = map.find(ino);
        if (it == map.end() || it->second.deltas.empty()) return;
        out = it->second.deltas.back().time_ns;  // time-ordered
      },
      ino);
  return out;
}

size_t DirStatManager::Size() {
  size_t n = 0;
  shard_map_.iterate([&n](Map& map) {
    for (const auto& [ino, log] : map) {
      if (!log.deltas.empty()) ++n;
    }
  });
  return n;
}

Status DirStatManager::GetDirStat(Context& ctx, Ino ino, DirStatEntry& out, bool with_pending) {
  GetDirStatOperation operation(ctx.GetTrace(), fs_id_, ino);
  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) return status;

  if (operation.GetResult().found) {
    out = operation.GetResult().dir_stat;

    if (with_pending) {
      DirStatDelta pending = GetPending(ino);
      out.set_length(out.length() + pending.length);
      out.set_inodes(out.inodes() + pending.inodes);
      out.set_dirs(out.dirs() + pending.dirs);
    }
    return Status::OK();
  }

  // No stored record (a directory created before the feature was enabled). The
  // read must not corrupt the store-vs-buffer reconciliation, so: capture the
  // buffer's high-water timestamp BEFORE the scan, recompute the absolute from
  // the live tree, then seed it with a single-txn no-clobber op (a concurrent
  // flush/seed that already created the record wins). On a successful seed,
  // compact the delta prefix that pre-dates the scan; deltas that raced in after
  // the scan (time_ns > cut) survive and flush on top. The returned value is the
  // scan absolute itself, which already reflects every committed mutation -- we
  // do NOT fold pending here (that would double-count what the scan saw). The
  // residual (a mutation committed-and-scanned whose async delta predates `cut`)
  // is bounded by async dispatch latency and converges via SyncDirStat; there is
  // no store commit clock to make this exact.
  uint64_t cut = EnableDirStats() ? PeekMaxTimeNs(ino) : 0;

  status = recompute_(ctx, ino, out);
  if (!status.ok()) return status;

  if (EnableDirStats() && !IsTrashInode(ino)) {
    SeedDirStatOperation seed_op(ctx.GetTrace(), fs_id_, ino, out);
    auto st = operation_processor_->RunAlone(&seed_op);
    if (st.ok()) {
      if (seed_op.GetResult().seeded) Compact(ino, cut);
    } else {
      LOG(WARNING) << fmt::format("[dirstat.{}] seed recomputed dir stat fail, ino({}), status({}).", fs_id_, ino,
                                  st.error_str());
    }
  }

  return Status::OK();
}

Status DirStatManager::GetAllDirStats(std::map<Ino, DirStatEntry>& dir_stats) {
  Trace trace;
  ScanDirStatOperation operation(trace, fs_id_, [&](const std::string& key, const std::string& value) -> bool {
    uint32_t fs_id = 0;
    Ino ino = 0;
    MetaCodec::DecodeDirStatKey(key, fs_id, ino);
    dir_stats[ino] = MetaCodec::DecodeDirStatValue(value);
    return true;
  });
  operation.SetIsolationLevel(Txn::kReadCommitted);

  return operation_processor_->RunAlone(&operation);
}

// Single-level dir-stat check/repair for one directory: recompute its stat from
// the live tree and compare against the stored record, recording (and optionally
// repairing) a mismatch. Tree-wide recursion now lives in the client (mds-cli),
// which calls this per directory routed to the owning MDS.
Status DirStatManager::SyncDirStat(Context& ctx, Ino ino, bool repair, std::vector<DirStatMismatch>& mismatches) {
  // Capture the buffer high-water BEFORE the scan so a delta that races in after
  // the scan snapshot is kept (compacted only after a successful repair write).
  uint64_t cut = (repair && EnableDirStats()) ? PeekMaxTimeNs(ino) : 0;

  DirStatEntry calc;
  auto status = recompute_(ctx, ino, calc);
  if (!status.ok()) return status;

  // Read + (on repair) overwrite in a single SI txn: a flush committing between
  // the read and the write triggers a retry that re-reads, so the recomputed
  // absolute is never lost-updated against it. repair=false issues only the read.
  RepairDirStatOperation op(ctx.GetTrace(), fs_id_, ino, calc, repair);
  status = operation_processor_->RunAlone(&op);
  if (!status.ok()) return status;

  const auto& r = op.GetResult();
  if (r.mismatch) {
    DirStatMismatch brk;
    brk.ino = ino;
    brk.found = r.found;
    brk.want = calc;
    if (r.found) brk.got = r.stored;
    mismatches.push_back(std::move(brk));
  }

  // Drop the pre-scan delta prefix only when we actually wrote the recomputed
  // absolute (it already reflects those deltas); deltas with time_ns > cut survive.
  if (r.wrote) Compact(ino, cut);

  return Status::OK();
}

Status DirStatManager::FlushDirStats() {
  if (!EnableDirStats()) return Status::OK();

  // Serialize flushes: a flush snapshots the delta log then applies the sums to
  // the store. Two concurrent flushes would both read and apply the same deltas
  // (double count). The atomic gate makes overlap (cron vs Destroy/manual) safe.
  bool expected = false;
  if (!flushing_.compare_exchange_strong(expected, true)) return Status::OK();
  ON_SCOPE_EXIT([this]() { flushing_.store(false); });

  auto snapshot = GetFlushSnapshot();
  if (snapshot.empty()) return Status::OK();

  std::map<uint64_t, DirStatDelta> deltas;
  for (const auto& [ino, fe] : snapshot) deltas.emplace(ino, fe.delta);

  Trace trace;
  FlushDirStatsOperation operation(trace, fs_id_, deltas);
  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    // The flush transaction applied nothing; leave the delta log untouched (we
    // never removed anything) so the next flush simply retries.
    LOG(ERROR) << fmt::format("[dirstat.{}] flush dir stats fail, status({}).", fs_id_, status.error_str());
    return status;
  }

  // Drop the persisted prefix per ino. A delta appended during the flush has a
  // larger logical timestamp than the snapshot timepoint and survives for the
  // next cycle. Skip inos the op could not apply (missing/negative) -- handled
  // by the recompute below.
  const auto& missing_inos = operation.GetResult().missing_inos;
  std::set<uint64_t> missing(missing_inos.begin(), missing_inos.end());
  for (const auto& [ino, fe] : snapshot) {
    if (missing.count(ino)) continue;
    Compact(ino, fe.timepoint);
  }

  // Records that were absent or whose merged value went negative: recompute the
  // absolute from the live tree and overwrite it (version-guarded), then compact
  // the pre-scan delta prefix -- same reconciliation as the GetDirStat miss path.
  for (Ino ino : missing_inos) {
    if (IsTrashInode(ino)) continue;
    Context ctx;
    uint64_t cut = PeekMaxTimeNs(ino);

    DirStatEntry calc;
    auto st = recompute_(ctx, ino, calc);
    if (!st.ok()) {
      LOG(WARNING) << fmt::format("[dirstat.{}] recalc dir stat fail, ino({}), status({}).", fs_id_, ino,
                                  st.error_str());
      continue;
    }

    RepairDirStatOperation rep(ctx.GetTrace(), fs_id_, ino, calc, /*repair=*/true);
    st = operation_processor_->RunAlone(&rep);
    if (!st.ok()) {
      LOG(WARNING) << fmt::format("[dirstat.{}] repair recomputed dir stat fail, ino({}), status({}).", fs_id_, ino,
                                  st.error_str());
      continue;
    }
    Compact(ino, cut);
  }

  return Status::OK();
}

}  // namespace dir_stat
}  // namespace mds
}  // namespace dingofs
