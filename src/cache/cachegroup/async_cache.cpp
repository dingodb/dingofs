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
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/async_cache.h"

namespace dingofs {
namespace cache {

AsyncCacheImpl::AsyncCacheImpl(BlockCacheSPtr block_cache)
    : running_(false), block_cache_(block_cache), async_cache_queue_id_({0}) {}

Status AsyncCacheImpl::Start() {
  if (!running_.exchange(true)) {
    LOG(INFO) << "Async cache starting...";

    bthread::ExecutionQueueOptions queue_options;
    queue_options.use_pthread = true;
    int rc = bthread::execution_queue_start(&async_cache_queue_id_,
                                            &queue_options, DoCache, this);
    if (rc != 0) {
      return Status::Internal("start async cache execution queue failed");
    }

    LOG(INFO) << "Async cache started.";
  }
  return Status::OK();
}

Status AsyncCacheImpl::Stop() {
  if (running_.exchange(false)) {
    LOG(INFO) << "Async cache stoping...";

    if (bthread::execution_queue_stop(async_cache_queue_id_) != 0) {
      return Status::Internal("stop async cache execution queue failed");
    } else if (bthread::execution_queue_join(async_cache_queue_id_) != 0) {
      return Status::Internal("join async cache execution queue failed");
    }

    LOG(INFO) << "Async cache stopped.";
  }
  return Status::OK();
}

void AsyncCacheImpl::Cache(const BlockKey& block_key, const Block& block) {
  Task task(block_key, block);
  CHECK_EQ(0, bthread::execution_queue_execute(async_cache_queue_id_, task));
}

// TODO:
// 1) MUST retrive the block which in async cache queue but not in disk
//    instead of request storage
// 2) add option to lock the blocks which request storage at the same time
int AsyncCacheImpl::DoCache(void* meta, bthread::TaskIterator<Task>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  AsyncCacheImpl* async_cache = static_cast<AsyncCacheImpl*>(meta);
  auto& block_cache = async_cache->block_cache_;
  for (; iter; iter++) {
    auto& task = *iter;
    block_cache->AsyncCache(task.block_key, task.block, [task](Status status) {
      if (!status.ok()) {
        LOG(ERROR) << "Async cache block (key=" << task.block_key.Filename()
                   << ") failed: " << status.ToString();
      }
    });
  }
  return 0;
}

}  // namespace cache
}  // namespace dingofs
