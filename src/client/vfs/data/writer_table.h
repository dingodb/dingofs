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

#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;
class FileWriter;

// WriterTable shares a single FileWriter per inode across all writable fhs.
//
// Responsibility scope: index + lifetime + share. Periodic flush is left to
// each FileWriter's own self-managed scheduler (SchedulePeriodicFlush /
// RunPeriodicFlush in file_writer.cc). The table only orchestrates Close()
// on eviction so the writer's self-loop can terminate.
//
// Lifetime contract — two-layer ref counting:
//   - FileWriter::refs_ remains the single source of truth for "is the
//     object alive". `FileWriter::ReleaseRef()` is the sole place that
//     calls `delete this` (when refs_ hits 0).
//   - WriterTable maintains a *separate* `holders` counter per entry —
//     it only counts outstanding AcquireWriter / PeekWriter callers. It
//     does NOT count internal FileWriter lambdas (e.g. async flush
//     callbacks that AcquireRef/ReleaseRef themselves).
//   - When `holders` drops to 0, the entry is removed from the map BEFORE
//     the matching `Close()` and `ReleaseRef()` calls. Any lingering
//     internal lambdas will eventually drop refs_ to 0 and delete-this;
//     by then the map no longer has a dangling pointer to chase.
//   - Close() on the evicted writer flips its `closed_` flag so its own
//     periodic flush loop stops re-arming itself, allowing the self-ref
//     to drain on the next interval (≤ FLAGS_vfs_periodic_flush_interval_ms
//     latency until refs_ actually hits 0 and the writer is destroyed).
//
// FlushAll vs Stop:
//   - FlushAll() synchronously flushes every live writer; idempotent, can
//     be called any time, no state change.
//   - Stop() marks stopped_ to refuse new acquires. It does NOT flush.
//     Callers that need both should call FlushAll() first.
class WriterTable {
 public:
  explicit WriterTable(VFSHub* hub);
  ~WriterTable();

  WriterTable(const WriterTable&) = delete;
  WriterTable& operator=(const WriterTable&) = delete;

  // Lifecycle.
  Status Start();
  void Stop();   // refuse new acquires; does not flush

  // Synchronously flush all live writers; idempotent.
  Status FlushAll();

  // Get-or-create the FileWriter for ino. Returned pointer has a holder
  // outstanding; caller MUST call ReleaseWriter exactly once.
  FileWriter* AcquireWriter(uint64_t ino);

  // Probe without creating. Returns nullptr if no writer exists for ino.
  // On hit, takes a holder; caller MUST call ReleaseWriter.
  FileWriter* PeekWriter(uint64_t ino);

  // Drop one holder. If it was the last holder, the entry is erased from
  // the map, Close() is called on the writer (so its self-managed
  // periodic flush stops re-arming), and finally ReleaseRef() is invoked
  // (which may delete-this once refs_ reaches 0).
  void ReleaseWriter(FileWriter* writer);

  // Number of live entries (best-effort, for metrics / tests).
  size_t Size() const;

 private:
  struct Entry {
    FileWriter* writer{nullptr};
    int64_t holders{0};   // tracked by WriterTable, NOT FileWriter
  };

  VFSHub* vfs_hub_{nullptr};

  mutable std::mutex mutex_;
  bool stopped_{false};
  std::unordered_map<uint64_t, Entry> writers_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_WRITER_TABLE_H_
