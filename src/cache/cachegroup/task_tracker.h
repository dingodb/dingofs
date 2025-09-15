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
#include "cache/storage/storage.h"
#include "cache/utils/context.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

struct DownloadTask {
  DownloadTask(ContextSPtr ctx, StorageSPtr storage, const BlockKey& key,
               size_t length, IOBuffer* buffer);

  Status Run(bool notify);
  bool Wait(long timeout_ms);

  ContextSPtr ctx;
  StorageSPtr storage;
  BlockKey key;
  size_t length;
  IOBuffer* buffer;
  Status status;

  bool finished{false};
  bthread::Mutex mutex;
  bthread::ConditionVariable cond;
};

using DownloadTaskSPtr = std::shared_ptr<DownloadTask>;

class TaskTracker {
 public:
  TaskTracker() = default;

  // return true if new task created
  bool GetOrCreateTask(ContextSPtr ctx, StorageSPtr storage,
                       const BlockKey& key, size_t length, IOBuffer* buffer,
                       DownloadTaskSPtr& task);
  void RemoveTask(const BlockKey& key);

 private:
  bthread::Mutex mutex_;
  std::unordered_map<std::string, DownloadTaskSPtr> tasks_;
};

using TaskTrackerUPtr = std::unique_ptr<TaskTracker>;

};  // namespace cache
};  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_TASK_TRACKER_H_
