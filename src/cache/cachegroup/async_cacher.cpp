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

#include "cache/cachegroup/async_cacher.h"

#include "cache/common/macro.h"
#include "cache/utils/context.h"

namespace dingofs {
namespace cache {

AsyncCacherImpl::AsyncCacherImpl(BlockCacheSPtr block_cache)
    : running_(false), block_cache_(block_cache), queue_id_({0}) {}

Status AsyncCacherImpl::Start() {
  LOG(INFO) << "Async cacher is starting...";

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&queue_id_, &queue_options,
                                          HandleTask, this);
  if (rc != 0) {
    return Status::Internal("start async cache execution queue failed");
  }

  LOG(INFO) << "Async cacher is up.";
  return Status::OK();
}

Status AsyncCacherImpl::Shutdown() {
  LOG(INFO) << "Async cacher is shutting down...";

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    return Status::Internal("stop async cache execution queue failed");
  } else if (bthread::execution_queue_join(queue_id_) != 0) {
    return Status::Internal("join async cache execution queue failed");
  }

  LOG(INFO) << "Async cacher is down.";

  CHECK_DOWN("Async cacher");
  return Status::OK();
}

void AsyncCacherImpl::AsyncCache(ContextSPtr ctx, const BlockKey& block_key,
                                 const Block& block) {
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_,
                                               Task(ctx, block_key, block)));
}

// TODO:
// 1) MUST retrive the block which in async cache queue but not in disk
//    instead of request storage
// 2) add option to lock the blocks which request storage at the same time
int AsyncCacherImpl::HandleTask(void* meta, bthread::TaskIterator<Task>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  AsyncCacherImpl* self = static_cast<AsyncCacherImpl*>(meta);
  for (; iter; iter++) {
    auto& task = *iter;
    self->block_cache_->AsyncCache(
        task.ctx, task.key, task.block, [task](Status status) {
          if (!status.ok()) {
            LOG(ERROR) << "Async cache block (key=" << task.key.Filename()
                       << ") failed: " << status.ToString();
          }
        });
  }
  return 0;
}

}  // namespace cache
}  // namespace dingofs
