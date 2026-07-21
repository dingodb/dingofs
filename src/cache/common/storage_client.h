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

#include <bthread/condition_variable.h>
#include <bthread/execution_queue.h>
#include <bthread/mutex.h>
#include <bvar/reducer.h>

#include <cstdint>
#include <functional>
#include <memory>

#include "cache/common/closure.h"
#include "cache/local/cache_store.h"
#include "common/blockaccess/block_accesser.h"
#include "utils/concurrent/task_thread_pool.h"

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

// Backoff before the tried-th retry (tried starts from 1):
//   upload:   min(base * tried * tried, 60s)
//   download: min(base * tried, 10s)
uint64_t UploadRetryBackoffMs(uint32_t tried);
uint64_t DownloadRetryBackoffMs(uint32_t tried);

struct RetryOption {
  uint32_t max_tries{0};  // 0 means use the corresponding FLAGS default
};

// The layer that owns storage-request retry: sdk-internal retry is
// minimized (see AwsSdkConfig::sdk_max_retries) and callers are expected
// not to add hot retry loops of their own. Put/Range run a bounded retry
// loop in the calling bthread with backoff between attempts, so failures
// slow down instead of hammering the storage backend.
class StorageClient {
 public:
  explicit StorageClient(blockaccess::BlockAccesser* block_accesser);
  Status Start();
  Status Shutdown();

  Status Put(BlockHandle handle, const IOBuffer& block,
             RetryOption option = {});
  Status Range(BlockHandle handle, off_t offset, size_t length,
               IOBuffer* buffer);

 private:
  // Why use bthread::ExecutionQueue?
  //  bthread -> Put(...) -> BlockAccesser::AsyncGet(...)
  //  maybe there is pthread synchronization semantics in function
  //  BlockAccesser::AsyncGet.
  class DispatchTask {
   public:
    DispatchTask(std::function<void()> dispatch, std::function<void()> abort)
        : dispatch_(std::move(dispatch)), abort_(std::move(abort)) {}

    void Dispatch() { dispatch_(); }
    void Abort() { abort_(); }

   private:
    std::function<void()> dispatch_;
    std::function<void()> abort_;
  };

  static int HandleTask(void* meta, bthread::TaskIterator<DispatchTask*>& iter);

  Status DoPut(const BlockHandle& handle, const IOBuffer& block,
               RetryOption option);
  Status PutAttempt(const BlockHandle& handle, const IOBuffer& block);
  Status RangeAttempt(const BlockHandle& handle, off_t offset, size_t length,
                      char* data, size_t* actual_len);
  // Returns false if the client is shut down before the backoff elapses.
  bool BackoffSleep(uint64_t backoff_ms);
  static bool IsRetriable(const Status& status) {
    return !status.IsNotFound() && !status.IsNotSupport();
  }

  std::atomic<bool> running_;
  blockaccess::BlockAccesser* block_accesser_;
  bthread::ExecutionQueueId<DispatchTask*> queue_id_;
  std::atomic<int64_t> inflight_puts_{0};
  std::unique_ptr<
      utils::TaskThreadPool<bthread::Mutex, bthread::ConditionVariable>>
      thread_pool_;
  bvar::Adder<int64_t> num_upload_retry_{"dingofs_storage_upload_total_retry"};
  bvar::Adder<int64_t> num_download_retry_{
      "dingofs_storage_download_total_retry"};
  bvar::Adder<int64_t> num_async_put_{"dingofs_storage_num_async_put"};
  bvar::Adder<int64_t> pending_async_put_{"dingofs_storage_pending_async_put"};
};

using StorageClientUPtr = std::unique_ptr<StorageClient>;
using StorageClientSPtr = std::shared_ptr<StorageClient>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_STORAGE_CLIENT_H_
