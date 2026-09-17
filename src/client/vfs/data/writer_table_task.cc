/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
#include "client/vfs/data/writer_table_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <array>
#include <utility>

#include "client/vfs/data/writer/file_writer.h"
#include "client/vfs/data/writer_table.h"
#include "common/sync_point.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace client {
namespace vfs {
WriterTableTask::WriterTableTask(WriterTable* table, Executor* cleanup_executor)
    : table_(CHECK_NOTNULL(table)),
      cleanup_executor_(CHECK_NOTNULL(cleanup_executor)) {}

bool WriterTableTask::RunOnce(size_t budget) {
  CHECK_GT(budget, 0u);
  while (next_ == snapshot_.size()) {
    snapshot_.clear();
    next_ = 0;
    if (shard_ == WriterTable::kShardCount) {
      shard_ = 0;
      return false;
    }
    snapshot_ = table_->SnapshotShard(shard_++);
  }

  std::array<Member, kMaxInFlight> batch{};
  size_t count = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return false;
    while (next_ < snapshot_.size() && count < std::min(budget, kMaxInFlight) &&
           in_flight_ < kMaxInFlight) {
      auto* writer = snapshot_[next_++];
      batch[count++] = {writer, flushing_.insert(writer).second};
      ++in_flight_;
    }
  }
  if (count == 0) {
    TEST_SYNC_POINT("WriterTableTask:paused");
    return false;  // Keep the cursor; the next periodic tick retries it.
  }
  for (size_t i = 0; i < count; ++i) StartFlush(batch[i]);
  return true;
}

void WriterTableTask::StartFlush(Member member) {
  if (!member.owns_record) {
    QueueCleanup(member);  // Even a duplicate snapshot owns an extra holder.
    return;
  }
  TEST_SYNC_POINT_CALLBACK("WriterTableTask:before_flush", member.writer);
  member.writer->FlushDirtyAsync(
      [self = shared_from_this(), member](Status status) {
        if (!status.ok())
          LOG(WARNING) << "Periodic flush failed: " << status.ToString();
        self->QueueCleanup(member);
      });
}

void WriterTableTask::QueueCleanup(Member member) {
  CHECK(cleanup_executor_->Execute([self = shared_from_this(), member] {
    self->Cleanup(member);
  })) << "cleanup executor rejected maintenance holder";
}

void WriterTableTask::Cleanup(Member member) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (member.owns_record) flushing_.erase(member.writer);
  }
  table_->ReleaseWriter(
      member.writer);  // May synchronously Close; no task lock.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_GT(in_flight_, 0u);
    --in_flight_;  // Only completed resource release returns capacity.
    drained_.notify_all();
  }
}

void WriterTableTask::OnStop() {
  std::unique_lock<std::mutex> lock(mutex_);
  stopping_ = true;
  drained_.wait(lock, [this] { return in_flight_ == 0; });
  DCHECK(flushing_.empty());

  snapshot_.erase(snapshot_.begin(), snapshot_.begin() + next_);
  auto tail = std::exchange(snapshot_, {});
  next_ = 0;
  if (tail.empty()) return;

  // All active members drained. Release the unprocessed tail as one cleanup
  // operation; shutdown does not need a scan slot or a retained resume token.
  in_flight_ = 1;
  lock.unlock();

  CHECK(cleanup_executor_->Execute([self = shared_from_this(),
                                    tail = std::move(tail)] {
    for (auto* writer : tail) self->table_->ReleaseWriter(writer);
    std::lock_guard<std::mutex> lock(self->mutex_);
    --self->in_flight_;
    self->drained_.notify_all();
  })) << "cleanup executor rejected residual snapshot";

  lock.lock();
  drained_.wait(lock, [this] { return in_flight_ == 0; });
}
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
