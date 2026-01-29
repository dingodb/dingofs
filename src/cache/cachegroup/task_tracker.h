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
#include <ostream>
#include <unordered_map>

#include "cache/blockcache/cache_store.h"
#include "cache/common/context.h"
#include "cache/common/storage_client.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class DownloadTask {
 public:
  struct Attr {
    ContextSPtr ctx;
    BlockKey key;
    size_t length;
  };

  struct Result {
    Status status;
    IOBuffer buffer;
  };

  DownloadTask(ContextSPtr ctx, const BlockKey& key, size_t length)
      : attr_{ctx, key, length}, result_{} {}

  Attr& Attr() { return attr_; }
  Result& Result() { return result_; }

  void Run() {
    std::lock_guard<bthread::Mutex> lock(mutex_);
    finish_ = true;
    cond_.notify_all();
  }

  bool Wait(long timeout_ms) {
    std::unique_lock<bthread::Mutex> lock_(mutex_);
    if (!finish_) {
      cond_.wait_for(lock_, timeout_ms * 1000);
    }
    return finish_;
  }

 private:
  struct Attr attr_;
  struct Result result_;
  bool finish_{false};
  bthread::Mutex mutex_;
  bthread::ConditionVariable cond_;
};

using DownloadTaskSPtr = std::shared_ptr<DownloadTask>;

class TaskTracker {
 public:
  TaskTracker() = default;

  // return true if new task created
  bool GetOrCreateTask(ContextSPtr ctx, const BlockKey& key, size_t length,
                       DownloadTaskSPtr& task) {
    std::lock_guard<bthread::Mutex> lock(mutex_);
    auto iter = tasks_.find(key.Filename());
    if (iter != tasks_.end()) {
      task = iter->second;
      return false;
    }

    task = std::make_shared<DownloadTask>(ctx, key, length);
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

inline std::ostream& operator<<(std::ostream& os,
                                const DownloadTaskSPtr& task) {
  os << "DownloadTask{key=" << task->Attr().key.Filename()
     << " length=" << task->Attr().length << "}";
  return os;
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_TASK_TRACKER_H_
