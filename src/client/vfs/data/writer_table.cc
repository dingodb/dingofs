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

#include "client/vfs/data/writer_table.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <memory>
#include <utility>
#include <vector>

#include "absl/hash/hash.h"
#include "client/vfs/data/writer/file_writer.h"
#include "client/vfs/hub/vfs_hub.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace client {
namespace vfs {

WriterTable::WriterTable(VFSHub* hub) : vfs_hub_(hub) {}

WriterTable::~WriterTable() {
  // Defensive: caller should Stop() explicitly. If not, mark stopped now.
  Stop();
}

WriterTableShard& WriterTable::GetShard(uint64_t ino) {
  return shards_[absl::HashOf(ino) & (kShardCount - 1)];
}

WriterTable::ShardLocks WriterTable::LockShards() {
  ShardLocks locks;
  for (size_t i = 0; i < shards_.size(); ++i) {
    locks[i] = shards_[i].LockForSnapshot();
  }
  return locks;
}

std::vector<FileWriter*> WriterTable::Snapshot() {
  auto locks = LockShards();
  size_t count = 0;
  for (const auto& shard : shards_) {
    count += shard.SizeLocked();
  }
  std::vector<FileWriter*> snapshot;
  snapshot.reserve(count);
  for (auto& shard : shards_) {
    shard.AppendPinnedLocked(snapshot);
  }
  return snapshot;
}

Status WriterTable::Start() {
  std::lock_guard<std::mutex> lg(lifecycle_mutex_);
  if (stopped_) {
    return Status::Internal("WriterTable already stopped");
  }
  LOG(INFO) << "WriterTable started";
  return Status::OK();
}

void WriterTable::Stop() {
  std::lock_guard<std::mutex> lg(lifecycle_mutex_);
  if (stopped_) return;
  {
    auto locks = LockShards();
    for (auto& shard : shards_) {
      shard.StopLocked();
    }
  }
  stopped_ = true;
  LOG(INFO) << "WriterTable stopped";
}

Status WriterTable::FlushAll() {
  // Snapshot live writers and pin each entry with a transient holder. A plain
  // FileWriter ref would prevent UAF but would still allow the last external
  // holder to erase the entry and Close() the writer before Flush() starts.
  auto snap = Snapshot();

  Status final_status;
  for (auto* w : snap) {
    Status s = w->Flush();
    if (!s.ok() && final_status.ok()) {
      final_status = s;
      LOG(WARNING) << fmt::format("FlushAll: writer flush failed: {}",
                                  s.ToString());
    }
    // Drop the transient holder. If all external holders were concurrently
    // released, this is now the last holder and performs Close after Flush.
    ReleaseWriter(w);
  }
  return final_status;
}

void WriterTable::FlushDirtyAsync(StatusCallback cb) {
  auto snap = Snapshot();

  if (snap.empty()) {
    cb(Status::OK());
    return;
  }

  Executor* cleanup_executor = CHECK_NOTNULL(vfs_hub_->GetCleanupExecutor());

  struct FlushGroup {
    std::mutex mutex;
    size_t remaining{0};
    Status status;
    StatusCallback done;

    // Called only after the member's holder has actually been released.
    void CompleteMember(const Status& member_status) {
      StatusCallback callback;
      Status final_status;
      {
        std::lock_guard<std::mutex> lock(mutex);
        if (!member_status.ok() && status.ok()) {
          status = member_status;
        }
        CHECK_GT(remaining, 0);
        if (--remaining == 0) {
          final_status = status;
          callback = std::move(done);
        }
      }
      if (callback) callback(std::move(final_status));
    }
  };

  auto group = std::make_shared<FlushGroup>();
  group->remaining = snap.size();
  group->done = std::move(cb);

  for (FileWriter* writer : snap) {
    writer->FlushDirtyAsync(
        [this, cleanup_executor, writer, group](Status status) {
          // Member completion only transfers responsibility: the holder release
          // below may be the last holder and synchronously Close the writer,
          // which can block on flush/CB/storage progress. Running that on this
          // completion thread (possibly a CBExecutor worker) self-deadlocks, so
          // it must happen on the independent cleanup executor.
          if (!cleanup_executor->Execute(
                  [this, writer, group, status = std::move(status)] {
                    ReleaseWriter(writer);
                    group->CompleteMember(status);
                  })) {
            // A live ExecutorImpl never rejects; rejection means the "cleanup
            // executor outlives every producer" lifecycle invariant is broken.
            // Leak-free fallbacks (inline Close, drop the holder, fake success)
            // are all forbidden by the interface contract.
            LOG(FATAL) << "WriterTable::FlushDirtyAsync: cleanup executor "
                          "rejected the member cleanup task";
          }
        });
  }
}

std::vector<FileWriter*> WriterTable::SnapshotShard(size_t shard_index) {
  CHECK_LT(shard_index, kShardCount);
  std::vector<FileWriter*> out;
  auto& shard = shards_[shard_index];
  auto lock = shard.LockForSnapshot();
  shard.AppendPinnedLocked(out);
  return out;
}

size_t WriterTable::Size() const {
  size_t count = 0;
  for (const auto& shard : shards_) {
    count += shard.Size();
  }
  return count;
}

FileWriter* WriterTable::AcquireWriter(uint64_t ino) {
  return GetShard(ino).Acquire(ino, vfs_hub_);
}

FileWriter* WriterTable::PeekWriter(uint64_t ino) {
  return GetShard(ino).Peek(ino);
}

void WriterTable::ReleaseWriter(FileWriter* writer) {
  if (writer != nullptr) {
    GetShard(writer->Ino()).Release(writer);
  }
}

FileWriter* WriterTableShard::Acquire(uint64_t ino, VFSHub* hub) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopped_) return nullptr;
  auto it = writers_.find(ino);
  if (it != writers_.end()) {
    it->second.writer->AcquireRef();
    ++it->second.holders;
    return it->second.writer;
  }

  auto* writer = new FileWriter(hub, ino);
  writer->AcquireRef();
  writers_.emplace(ino, Entry{writer, 1});
  return writer;
}

FileWriter* WriterTableShard::Peek(uint64_t ino) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopped_) return nullptr;
  auto it = writers_.find(ino);
  if (it == writers_.end()) return nullptr;
  it->second.writer->AcquireRef();
  ++it->second.holders;
  return it->second.writer;
}

void WriterTableShard::Release(FileWriter* writer) {
  const uint64_t ino = writer->Ino();
  bool close = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = writers_.find(ino);
    if (it == writers_.end()) {
      LOG(WARNING) << "ReleaseWriter: ino " << ino << " not in table";
    } else {
      CHECK_EQ(it->second.writer, writer)
          << "ReleaseWriter: pointer mismatch for ino=" << ino;
      CHECK_GT(it->second.holders, 0);
      if (--it->second.holders == 0) {
        writers_.erase(it);
        close = true;
      }
    }
  }
  if (close) writer->Close();
  writer->ReleaseRef();
}

size_t WriterTableShard::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return writers_.size();
}

void WriterTableShard::AppendPinnedLocked(std::vector<FileWriter*>& out) {
  for (auto& [ino, entry] : writers_) {
    entry.writer->AcquireRef();
    ++entry.holders;
    out.push_back(entry.writer);
  }
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
