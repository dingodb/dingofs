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

#include <sys/types.h>

#include <cstdlib>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/common.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using dingofs::cache::blockcache::Block;

AsyncCacheImpl::AsyncCacheImpl(std::shared_ptr<BlockCache> block_cache)
    : running_(false), block_cache_(block_cache) {}

Status AsyncCacheImpl::Start() {
  if (!running_.exchange(true)) {
    bthread::ExecutionQueueOptions queue_options;
    queue_options.use_pthread = true;
    int rc = bthread::execution_queue_start(&async_cache_queue_id_,
                                            &queue_options, DoCache, this);
    if (rc != 0) {
      return Status::Internal("stop execution queue failed");
    }
  }
  return Status::OK();
}

Status AsyncCacheImpl::Stop() {
  if (running_.exchange(false)) {
    bthread::execution_queue_stop(async_cache_queue_id_);
    int rc = bthread::execution_queue_join(async_cache_queue_id_);
    if (rc != 0) {
      return Status::Internal("stop execution queue failed");
    }
  }
  return Status::OK();
}

void AsyncCacheImpl::Cache(const BlockKey& block_key,
                           const butil::IOBuf& block) {
  CacheTask task(block_key, block);
  CHECK_EQ(0, bthread::execution_queue_execute(async_cache_queue_id_, task));
}

int AsyncCacheImpl::DoCache(void* meta,
                            bthread::TaskIterator<CacheTask>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  AsyncCacheImpl* async_cache = static_cast<AsyncCacheImpl*>(meta);
  for (; iter; iter++) {
    auto& task = *iter;
    // TODO(Wine93): user async interface
    Block block((char*)task.block.fetch1(), task.block.length());
    auto status = async_cache->block_cache_->Cache(task.block_key, block);
    if (!status.ok()) {
      LOG(ERROR) << "Async cache block(" << task.block_key.Filename()
                 << ") failed, status=" << status.ToString();
    }
  }
  return 0;
}

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs
