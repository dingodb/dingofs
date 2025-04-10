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
 * Created Date: 2025-03-27
 * Author: Jingli Chen (Wine93)
 */

#include "client/blockcache/block_prefetcher.h"

#include <glog/logging.h>

#include <cassert>

#include "client/common/status.h"

namespace dingofs {
namespace client {
namespace blockcache {

using ::dingofs::utils::ReadLockGuard;
using ::dingofs::utils::WriteLockGuard;

BlockPrefetcherImpl::BlockPrefetcherImpl()
    : running_(false),
      prefetch_thread_pool_(std::make_unique<TaskThreadPool<>>()) {}

Status BlockPrefetcherImpl::Init(uint32_t workers, uint32_t queue_size,
                                 PrefetchFunc prefetch_func) {
  if (!running_.exchange(true)) {
    prefetch_func_ = prefetch_func;

    bthread::ExecutionQueueOptions queue_options;
    queue_options.use_pthread = true;
    int rc = bthread::execution_queue_start(&submit_queue_id_, &queue_options,
                                            BatchSubmit, this);
    if (rc != 0) {
      LOG(ERROR) << "execution_queue_start() failed, rc=" << rc;
      return Status::Internal("execution_queue_start fail");
    }

    rc = prefetch_thread_pool_->Start(workers, queue_size);
    if (rc != 0) {
      LOG(ERROR) << "Start prefetch thread pool failed, rc = " << rc;
      return Status::Internal("start fail");
    }
    LOG(INFO) << "Start prefetch thread pool success.";
  }
  return Status::OK();
}

Status BlockPrefetcherImpl::Shutdown() {
  if (running_.exchange(false)) {
    bthread::execution_queue_stop(submit_queue_id_);
    int rc = bthread::execution_queue_join(submit_queue_id_);
    if (rc != 0) {
      return Status::Internal("execution_queue_join fail");
    }
    prefetch_thread_pool_->Stop();
  }
  return Status::OK();
}

void BlockPrefetcherImpl::Submit(const BlockKey& block_key, size_t length) {
  CHECK_EQ(0, bthread::execution_queue_execute(submit_queue_id_,
                                               Task(block_key, length)));
}

int BlockPrefetcherImpl::BatchSubmit(void* meta,
                                     bthread::TaskIterator<Task>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  BlockPrefetcherImpl* prefetcher = static_cast<BlockPrefetcherImpl*>(meta);
  for (; iter; iter++) {
    auto& task = *iter;
    auto& block_key = task.block_key;
    size_t block_size = task.block_size;
    prefetcher->prefetch_thread_pool_->Enqueue(
        [prefetcher, block_key, block_size] {
          prefetcher->Prefetch(block_key, block_size);
        });
  }
  return 0;
}

void BlockPrefetcherImpl::Prefetch(const BlockKey& key, size_t length) {
  {
    WriteLockGuard lk(rwlock_);

    if (busy_.find(key.Filename()) != busy_.end()) {
      return;
    }
    busy_[key.Filename()] = true;
  }

  auto status = prefetch_func_(key, length);
  if (!status.ok()) {
    LOG(ERROR) << "PreFetch for block(" << key.StoreKey()
               << ") failed, rc = " << status.ToString();
  }

  {
    WriteLockGuard lk(rwlock_);
    busy_.erase(key.Filename());
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs
