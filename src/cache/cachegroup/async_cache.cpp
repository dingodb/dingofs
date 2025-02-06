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

#include "cache/common/errno.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using cache::common::Errno;

AsyncCacheImpl::AsyncCacheImpl(std::shared_ptr<BlockCache> block_cache)
    : running_(false), block_cache_(block_cache) {}

bool AsyncCacheImpl::Start() {
  if (!running_.exchange(true)) {
    bthread::ExecutionQueueOptions queue_options;
    queue_options.bthread_attr = BTHREAD_ATTR_NORMAL;
    int rc = bthread::execution_queue_start(&async_cache_queue_id_,
                                            &queue_options, DoCache, this);
    return rc == 0;
  }
  return true;
}

bool AsyncCacheImpl::Stop() {
  if (running_.exchange(false)) {
    bthread::execution_queue_stop(async_cache_queue_id_);
    int rc = bthread::execution_queue_join(async_cache_queue_id_);
    return rc == 0;
  }
  return true;
}

int AsyncCacheImpl::DoCache(void* meta,
                            bthread::TaskIterator<CacheTask>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  AsyncCacheImpl* async_cache = static_cast<AsyncCacheImpl*>(meta);
  for (; iter; iter++) {
    auto& task = *iter;
    auto rc = async_cache->block_cache_->Cache(task.block_key, task.block);
    if (rc != Errno::OK) {
      LOG(ERROR) << "Async cache block(" << task.block_key.Filename()
                 << ") failed, rc=" << StrErr(rc);
    }
  }
  return 0;
}

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs
