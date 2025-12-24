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
 * Created Date: 2025-05-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_STORAGE_CLIENT_H_
#define DINGOFS_SRC_CACHE_COMMON_STORAGE_CLIENT_H_

#include <bthread/execution_queue.h>
#include <bthread/mutex.h>
#include <bvar/passive_status.h>
#include <bvar/reducer.h>

#include <cstdint>
#include <memory>
#include <ostream>

#include "cache/blockcache/cache_store.h"
#include "cache/common/closure.h"
#include "cache/common/context.h"
#include "cache/iutil/task_execution_queue.h"
#include "common/blockaccess/block_accesser.h"

namespace dingofs {
namespace cache {

class TaskClosure : public Closure {
 public:
  void Wait() {
    std::unique_lock<bthread::Mutex> lock(mutex_);
    while (!finish_) {
      cond_.wait(lock);
    }
  }

  void Run() override {
    std::lock_guard<bthread::Mutex> lock(mutex_);
    finish_ = true;
    cond_.notify_one();
  }

 private:
  bool finish_{false};
  bthread::Mutex mutex_;
  bthread::ConditionVariable cond_;
};

class PutBlockTask final : public TaskClosure {
 public:
  PutBlockTask(ContextSPtr ctx, const BlockKey& key, const Block* block,
               blockaccess::BlockAccesser* block_accesser,
               iutil::TaskExecutionQueueSPtr retry_queue);

  void Run() override;

 private:
  friend std::ostream& operator<<(std::ostream&, const PutBlockTask&);

  blockaccess::PutObjectAsyncContextSPtr OnPrepare();
  void OnCallback(const blockaccess::PutObjectAsyncContextSPtr& ctx);
  void OnRetry(const blockaccess::PutObjectAsyncContextSPtr& ctx);
  void OnComplete(Status s);

  ContextSPtr ctx_;
  BlockKey key_;
  const Block* block_;
  blockaccess::BlockAccesser* block_accesser_;
  iutil::TaskExecutionQueueSPtr retry_queue_;
};

class RangeBlockTask final : public TaskClosure {
 public:
  RangeBlockTask(ContextSPtr ctx, const BlockKey& key, off_t offset,
                 size_t length, IOBuffer* buffer,
                 blockaccess::BlockAccesser* block_accesser,
                 iutil::TaskExecutionQueueSPtr retry_queue);

  void Run() override;

 private:
  friend std::ostream& operator<<(std::ostream&, const RangeBlockTask&);

  blockaccess::GetObjectAsyncContextSPtr OnPrepare();
  void OnCallback(const blockaccess::GetObjectAsyncContextSPtr& ctx);
  void OnRetry(const blockaccess::GetObjectAsyncContextSPtr& ctx);
  void OnComplete(Status s);

  ContextSPtr ctx_;
  BlockKey key_;
  off_t offset_;
  size_t length_;
  IOBuffer* buffer_;
  blockaccess::BlockAccesser* block_accesser_;
  iutil::TaskExecutionQueueSPtr retry_queue_;
};

// Why use bthread::ExecutionQueue?
//  bthread -> Put(...) -> BlockAccesser::AsyncGet(...)
//  maybe there is pthread synchronization semantics in function
//  BlockAccesser::AsyncGet.
class StorageClient {
 public:
  explicit StorageClient(blockaccess::BlockAccesser* block_accesser);
  Status Start();
  Status Shutdown();

  Status Put(ContextSPtr ctx, const BlockKey& key, const Block* block);
  Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
               size_t length, IOBuffer* buffer);

 private:
  static int HandleClosure(void* meta,
                           bthread::TaskIterator<TaskClosure*>& iter);

  std::atomic<bool> running_;
  blockaccess::BlockAccesser* block_accesser_;
  bthread::ExecutionQueueId<TaskClosure*> queue_id_;
  iutil::TaskExecutionQueueSPtr upload_retry_queue_;
  iutil::TaskExecutionQueueSPtr download_retry_queue_;
  bvar::PassiveStatus<int64_t> num_upload_retry_task_;
  bvar::PassiveStatus<int64_t> num_download_retry_task_;
};

using StorageClientUPtr = std::unique_ptr<StorageClient>;
using StorageClientSPtr = std::shared_ptr<StorageClient>;

std::ostream& operator<<(std::ostream& os, const PutBlockTask& task);
std::ostream& operator<<(std::ostream& os, const RangeBlockTask& task);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_STORAGE_CLIENT_H_
