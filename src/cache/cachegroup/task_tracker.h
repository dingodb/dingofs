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

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_TASK_TRACKER_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_TASK_TRACKER_H_

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <cstddef>
#include <memory>
#include <unordered_map>

#include "cache/blockcache/cache_store.h"
#include "cache/common/closure.h"
#include "cache/common/context.h"
#include "cache/common/storage_client.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

struct DownloadTask {
  DownloadTask(ContextSPtr ctx, StorageClient* storage_client,
               const BlockKey& key, size_t length, IOBuffer* buffer)
      : ctx(ctx),
        storage_client(storage_client),
        key(key),
        length(length),
        buffer(buffer) {}

  // Pls gurantee that Run() is called by the task owner.
  void Run() {
    status = storage_client->Range(ctx, key, 0, length, buffer);
    std::lock_guard<bthread::Mutex> lock(mutex);
    finish = true;
    cond.notify_all();
  }

  bool Wait(long timeout_ms) {
    std::unique_lock<bthread::Mutex> lock(mutex);
    if (!finish) {
      cond.wait_for(lock, timeout_ms * 1000);
    }
    return finish;
  }

  ContextSPtr ctx;
  StorageClient* storage_client;
  BlockKey key;
  size_t length;
  IOBuffer* buffer;
  Status status;

  // FIXME: make members private?
  bool finish{false};
  bthread::Mutex mutex;
  bthread::ConditionVariable cond;
};

using DownloadTaskSPtr = std::shared_ptr<DownloadTask>;

class TaskTracker {
 public:
  TaskTracker() = default;

  // return true if new task created
  bool GetOrCreateTask(ContextSPtr ctx, StorageClient* storage_client,
                       const BlockKey& key, size_t length, IOBuffer* buffer,
                       DownloadTaskSPtr& task) {
    std::lock_guard<bthread::Mutex> lock(mutex_);
    auto iter = tasks_.find(key.Filename());
    if (iter != tasks_.end()) {
      task = iter->second;
      return false;
    }

    task = std::make_shared<DownloadTask>(ctx, storage_client, key, length,
                                          buffer);
    tasks_[key.Filename()] = task;
    return true;
  }

  void RemoveTask(const BlockKey& key) {
    std::lock_guard<bthread::Mutex> lock(mutex_);
    tasks_.erase(key.Filename());
  }

 private:
  bthread::Mutex mutex_;
  std::unordered_map<std::string, DownloadTaskSPtr> tasks_;
};

using TaskTrackerUPtr = std::unique_ptr<TaskTracker>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_TASK_TRACKER_H_
