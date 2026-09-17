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

#include "client/vfs/data/reader/reader_registry_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <utility>

#include "client/vfs/data/reader/file_reader.h"
#include "client/vfs/data/reader/reader_registry.h"
#include "common/sync_point.h"
namespace dingofs {
namespace client {
namespace vfs {
ReaderRegistryTask::ReaderRegistryTask(ReaderRegistry* registry)
    : registry_(CHECK_NOTNULL(registry)) {}

bool ReaderRegistryTask::RunOnce(size_t budget) {
  CHECK_GT(budget, 0u);
  while (next_ == snapshot_.size()) {
    snapshot_.clear();
    next_ = 0;
    if (shard_ == ReaderRegistry::kShardCount) {
      shard_ = 0;
      return false;
    }
    snapshot_ = registry_->SnapshotShard(shard_++);
  }
  const size_t end = next_ + std::min(budget, snapshot_.size() - next_);
  while (next_ < end) {
    auto* reader = snapshot_[next_++];
    reader->ShrinkIfOpen();
    reader->ReleaseRef();
  }
  TEST_SYNC_POINT("ReaderRegistryTask:after_batch");
  return true;
}

void ReaderRegistryTask::OnStop() {
  // Scheduler guarantees the RunOnce stack has returned before OnStop.
  while (next_ < snapshot_.size()) snapshot_[next_++]->ReleaseRef();
  snapshot_.clear();
  next_ = 0;
}
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
