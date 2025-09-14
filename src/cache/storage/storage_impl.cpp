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

#include "cache/storage/storage_impl.h"

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <butil/iobuf.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <cstddef>

#include "blockaccess/block_accesser.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/macro.h"
#include "cache/storage/storage.h"
#include "cache/storage/storage_closure.h"
#include "cache/utils/offload_thread_pool.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

const std::string kModule = "storage";

StorageImpl::StorageImpl(blockaccess::BlockAccesser* block_accesser)
    : running_(false), block_accesser_(block_accesser), queue_id_({0}) {}

Status StorageImpl::Start() {
  CHECK_NOTNULL(block_accesser_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Storage is starting...";

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&queue_id_, &queue_options,
                                          HandleClosure, this);
  if (rc != 0) {
    LOG(ERROR) << "Start execution queue failed: rc = " << rc;
    return Status::Internal("start execution queue fail");
  }

  running_ = true;

  LOG(INFO) << "Storage is up.";

  CHECK_RUNNING("Storage");
  return Status::OK();
}

Status StorageImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Storage is shutting down...";

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    LOG(ERROR) << "Stop execution queue failed.";
    return Status::Internal("stop execution queue failed");
  } else if (bthread::execution_queue_join(queue_id_) != 0) {
    LOG(ERROR) << "Join execution queue failed";
    return Status::Internal("join execution queue failed");
  }

  LOG(INFO) << "Storage is down.";

  CHECK_DOWN("Storage");
  return Status::OK();
}

Status StorageImpl::Put(ContextSPtr ctx, const BlockKey& key,
                        const Block& block, PutOption option) {
  DCHECK_RUNNING("Storage");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "put(%s,%zu)", key.Filename(),
                    block.size);
  StepTimerGuard guard(timer);

  NEXT_STEP("enqueue");
  auto closure = PutClosure(ctx, key, block, option, block_accesser_);
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, &closure));

  NEXT_STEP("s3_put");
  closure.Wait();

  status = closure.status();
  if (!status.ok()) {
    GENERIC_LOG_UPLOAD_ERROR();
    return status;
  }
  return status;
}

Status StorageImpl::Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                          size_t length, IOBuffer* buffer, RangeOption option) {
  DCHECK_RUNNING("Storage");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "range(%s,%lld,%zu)",
                    key.Filename(), offset, length);
  StepTimerGuard guard(timer);

  NEXT_STEP("enqueue");
  auto closure =
      RangeClosure(ctx, key, offset, length, buffer, option, block_accesser_);
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, &closure));

  NEXT_STEP("s3_range");
  closure.Wait();

  status = closure.status();
  if (!status.ok()) {
    GENERIC_LOG_DOWNLOAD_ERROR();
  }
  return status;
}

int StorageImpl::HandleClosure(void* meta,
                               bthread::TaskIterator<StorageClosure*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  StorageImpl* self = static_cast<StorageImpl*>(meta);
  for (; iter; iter++) {
    auto* op = *iter;
    OffloadThreadPool::Submit(  // copy memory
        [self, op]() { op->Run(); });
  }
  return 0;
}

}  // namespace cache
}  // namespace dingofs
