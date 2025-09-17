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
 * Created Date: 2025-05-22
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_STORAGE_STORAGE_CLOSURE_H_
#define DINGOFS_SRC_CACHE_STORAGE_STORAGE_CLOSURE_H_

#include <mutex>

#include "blockaccess/accesser_common.h"
#include "blockaccess/block_accesser.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/type.h"
#include "cache/storage/closure.h"
#include "cache/storage/storage.h"
#include "cache/utils/execution_queue.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class StorageClosure : public Closure {
 public:
  ~StorageClosure() override = default;

  void Wait() {
    std::unique_lock<BthreadMutex> lk(mutex_);
    while (!finish_) {
      cond_.wait(lk);
    }
  }

  void Run() override {
    std::lock_guard<BthreadMutex> lk(mutex_);
    finish_ = true;
    cond_.notify_one();
  }

 private:
  bool finish_{false};
  BthreadMutex mutex_;
  BthreadConditionVariable cond_;
};

class UploadClosure final : public StorageClosure {
 public:
  UploadClosure(ContextSPtr ctx, const BlockKey& key, const Block& block,
                UploadOption option, blockaccess::BlockAccesser* block_accesser,
                ExecutionQueueSPtr retry_queue);

  void Run() override;

 private:
  blockaccess::PutObjectAsyncContextSPtr OnPrepare();
  void OnCallback(const blockaccess::PutObjectAsyncContextSPtr& ctx);
  void OnRetry(const blockaccess::PutObjectAsyncContextSPtr& ctx);
  void OnComplete(Status s);

  Block CopyBlock();

  ContextSPtr ctx_;
  BlockKey key_;
  Block block_;
  UploadOption option_;
  blockaccess::BlockAccesser* block_accesser_;
  ExecutionQueueSPtr retry_queue_;
};

class DownloadClosure final : public StorageClosure {
 public:
  DownloadClosure(ContextSPtr ctx, const BlockKey& key, off_t offset,
                  size_t length, IOBuffer* buffer, DownloadOption option,
                  blockaccess::BlockAccesser* block_accesser,
                  ExecutionQueueSPtr retry_queue);

  void Run() override;

 private:
  blockaccess::GetObjectAsyncContextSPtr OnPrepare();
  void OnCallback(const blockaccess::GetObjectAsyncContextSPtr& ctx);
  void OnRetry(const blockaccess::GetObjectAsyncContextSPtr& ctx);
  void OnComplete(Status s);

  ContextSPtr ctx_;
  BlockKey key_;
  off_t offset_;
  size_t length_;
  IOBuffer* buffer_;
  DownloadOption option_;
  blockaccess::BlockAccesser* block_accesser_;
  ExecutionQueueSPtr retry_queue_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_STORAGE_CLOSURE_H_
