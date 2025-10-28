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
 * Created Date: 2025-06-22
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/async_filler.h"

#include <absl/strings/str_format.h>
#include <bthread/execution_queue.h>

#include <atomic>

#include "cache/common/macro.h"
#include "cache/utils/context.h"

namespace dingofs {
namespace cache {

AsyncFillerImpl::AsyncFillerImpl(BlockCacheSPtr block_cache)
    : running_(false), block_cache_(block_cache) {}

Status AsyncFillerImpl::Start() {
  CHECK_NOTNULL(block_cache_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Async filler is starting...";

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&queue_id_, &queue_options,
                                          HandleTask, this);
  if (rc != 0) {
    LOG(ERROR) << "Start execution queue failed: rc = " << rc;
    return Status::Internal("start execution queue failed");
  }

  running_ = true;

  LOG(INFO) << "Async filler is up.";

  CHECK_RUNNING("Async filler");
  return Status::OK();
}

Status AsyncFillerImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Async filler is shutting down...";

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    LOG(ERROR) << "Stop execution queue failed.";
    return Status::Internal("stop execution queue failed");
  } else if (bthread::execution_queue_join(queue_id_) != 0) {
    LOG(ERROR) << "Join execution queue failed.";
    return Status::Internal("join execution queue failed");
  }

  LOG(INFO) << "Async filler is down.";

  CHECK_DOWN("Async filler");
  return Status::OK();
}

void AsyncFillerImpl::AsyncFill(ContextSPtr ctx, const BlockKey& key,
                                const Block& block) {
  DCHECK_RUNNING("Async filler");
  CHECK_EQ(0, bthread::execution_queue_execute(
                  queue_id_, Task(NewContext(ctx->TraceId()), key, block)));
}

int AsyncFillerImpl::HandleTask(void* meta, bthread::TaskIterator<Task>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  AsyncFillerImpl* self = static_cast<AsyncFillerImpl*>(meta);
  for (; iter; iter++) {
    auto& task = *iter;
    self->block_cache_->AsyncCache(
        task.ctx, task.key, task.block, [task](Status status) {
          if (!status.ok()) {
            LOG(ERROR) << absl::StrFormat(
                "Async fill cache to cache group failed: key = %s, status = %s",
                task.key.Filename(), status.ToString());
          }
        });
  }

  return 0;
}

}  // namespace cache
}  // namespace dingofs
