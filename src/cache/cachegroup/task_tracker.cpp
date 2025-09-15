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

/*
 * Project: DingoFS
 * Created Date: 2025-09-03
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/task_tracker.h"

#include <brpc/reloadable_flags.h>
#include <bthread/mutex.h>

#include <mutex>

#include "cache/blockcache/cache_store.h"
#include "cache/storage/storage.h"

namespace dingofs {
namespace cache {

DownloadTask::DownloadTask(ContextSPtr ctx, StorageSPtr storage,
                           const BlockKey& key, size_t length, IOBuffer* buffer)
    : ctx(ctx), storage(storage), key(key), length(length), buffer(buffer) {}

Status DownloadTask::Run(bool notify) {
  status = storage->Range(ctx, key, 0, length, buffer);
  if (notify) {
    std::lock_guard<bthread::Mutex> lock(mutex);
    finished = true;
    cond.notify_all();
  }
  return status;
}

bool DownloadTask::Wait(long timeout_ms) {
  std::unique_lock<bthread::Mutex> lock(mutex);
  if (!finished) {
    cond.wait_for(lock, timeout_ms * 1000);
  }
  return finished;
}

bool TaskTracker::GetOrCreateTask(ContextSPtr ctx, StorageSPtr storage,
                                  const BlockKey& key, size_t length,
                                  IOBuffer* buffer, DownloadTaskSPtr& task) {
  std::lock_guard<bthread::Mutex> lock(mutex_);
  auto iter = tasks_.find(key.Filename());
  if (iter != tasks_.end()) {
    task = iter->second;
    return false;
  }

  tasks_[key.Filename()] =
      std::make_shared<DownloadTask>(ctx, storage, key, length, buffer);
  return true;
}

void TaskTracker::RemoveTask(const BlockKey& key) {
  std::lock_guard<bthread::Mutex> lock(mutex_);
  tasks_.erase(key.Filename());
}

}  // namespace cache
}  // namespace dingofs
