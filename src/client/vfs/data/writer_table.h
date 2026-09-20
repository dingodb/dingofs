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

#ifndef DINGOFS_CLIENT_VFS_DATA_WRITER_TABLE_H_
#define DINGOFS_CLIENT_VFS_DATA_WRITER_TABLE_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/callback.h"
#include "common/status.h"

namespace dingofs {

namespace client {
namespace vfs {

class VFSHub;
class FileWriter;

// WriterTable shares a single FileWriter per inode across all writable fhs.
//
// Responsibility scope: index + lifetime + share. Periodic flush is driven by
// a WriterTableTask registered with MaintenanceManager, not by self-arming
// tasks. The table only orchestrates Close() on eviction.
//
// Lifetime contract — two-layer ref counting:
//   - FileWriter::refs_ remains the single source of truth for "is the
//     object alive". `FileWriter::ReleaseRef()` is the sole place that
//     calls `delete this` (when refs_ hits 0).
//   - WriterTable maintains a *separate* `holders` counter per entry. It counts
//     outstanding AcquireWriter / PeekWriter callers and short-lived
//     FlushAll/background snapshot pins. It does NOT count internal
//     FileWriter lambdas (e.g. async flush callbacks that
//     AcquireRef/ReleaseRef themselves).
//   - When `holders` drops to 0, the entry is removed from the map BEFORE
//     the matching `Close()` and `ReleaseRef()` calls. Any lingering
//     internal lambdas will eventually drop refs_ to 0 and delete-this;
//     by then the map no longer has a dangling pointer to chase.
//   - Close() on the evicted writer flips its `closed_` flag so late flush
//     attempts fail fast instead of writing behind the metadata lifecycle.
//
// FlushAll vs Stop:
//   - FlushAll() synchronously flushes every live writer; its transient holder
//     pins prevent a concurrent last external release from closing a
//     snapshotted writer before that writer's Flush completes.
//   - Stop() marks stopped_ to refuse new acquires. It does NOT flush.
//     Callers that need both should call FlushAll() first.
// One inode partition: its own writer index and lock. The table owns
// routing, snapshots, and lifecycle; holders protect against Close while
// FileWriter refs protect against deletion.
class alignas(64) WriterTableShard {
 public:
  using Lock = std::unique_lock<std::mutex>;

  // Acquire/Peek return one holder; balance every success with Release.
  FileWriter* Acquire(uint64_t ino, VFSHub* hub);
  FileWriter* Peek(uint64_t ino);
  // Erase under the local lock, then Close/ReleaseRef outside it.
  void Release(FileWriter* writer);
  size_t Size() const;

  // Locked operations require LockForSnapshot(). Multi-shard callers
  // acquire locks in ascending order; pinned holders must outlive flushes.
  Lock LockForSnapshot() { return Lock(mutex_); }
  size_t SizeLocked() const { return writers_.size(); }
  void StopLocked() { stopped_ = true; }
  void AppendPinnedLocked(std::vector<FileWriter*>& out);

 private:
  struct Entry {
    FileWriter* writer;
    int64_t holders;
  };

  mutable std::mutex mutex_;
  bool stopped_{false};
  std::unordered_map<uint64_t, Entry> writers_;
};

class WriterTable {
 public:
  // Shard count for background single-shard scans (see SnapshotShard).
  static constexpr size_t kShardCount = 64;

  explicit WriterTable(VFSHub* hub);
  ~WriterTable();

  WriterTable(const WriterTable&) = delete;
  WriterTable& operator=(const WriterTable&) = delete;

  // Lifecycle. The owner must drain users/callbacks before destruction.
  Status Start();
  void Stop();  // refuse new acquires; does not flush

  // Pin a consistent table-membership snapshot, then flush outside shard locks.
  Status FlushAll();

  // Flush dirty snapshot members, then release each holder on the hub's
  // independent cleanup executor before completing cb exactly once.
  // The hub, table, cleanup executor and writer dependencies must remain
  // alive until cb finishes. Cleanup must enqueue, never run inline or reject
  // accepted-lifecycle work. Empty snapshots complete inline; otherwise cb
  // runs on cleanup. A callback must not synchronously drain its own executor.
  void FlushDirtyAsync(StatusCallback cb);

  // Background single-shard snapshot: pins every writer of shard
  // `shard_index` (< kShardCount) with a FileWriter ref plus a transient
  // holder, under that shard's lock only. The caller owns the release
  // responsibility (ReleaseWriter per entry, typically via a cleanup
  // executor); never hold shard locks while processing or releasing.
  std::vector<FileWriter*> SnapshotShard(size_t shard_index);

  // Get-or-create the FileWriter for ino. Returned pointer has a holder
  // outstanding; caller MUST call ReleaseWriter exactly once.
  FileWriter* AcquireWriter(uint64_t ino);

  // Probe without creating. Returns nullptr if no writer exists for ino.
  // On hit, takes a holder; caller MUST call ReleaseWriter.
  FileWriter* PeekWriter(uint64_t ino);

  // Drop one holder. If it was the last holder, the entry is erased from
  // the map and Close() is called on the writer (which may synchronously
  // flush and block on storage/CB dependencies), and finally ReleaseRef()
  // is invoked (which may delete-this once refs_ reaches 0). Do not call
  // this while holding any table/maintenance lock.
  void ReleaseWriter(FileWriter* writer);

  // Number of live entries (best-effort, for metrics / tests).
  size_t Size() const;

 private:
  using ShardLocks = std::array<WriterTableShard::Lock, kShardCount>;

  WriterTableShard& GetShard(uint64_t ino);
  ShardLocks LockShards();
  std::vector<FileWriter*> Snapshot();

  VFSHub* vfs_hub_{nullptr};

  std::array<WriterTableShard, kShardCount> shards_;
  std::mutex lifecycle_mutex_;
  bool stopped_{false};  // Protected by lifecycle_mutex_.
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_WRITER_TABLE_H_
