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
#include <memory>
#include <mutex>

#include "blockaccess/block_accesser.h"
#include "cache/storage/storage.h"
#include "cache/storage/storage_operator.h"

namespace dingofs {
namespace cache {

using blockaccess::RetryStrategy;

StorageImpl::StorageImpl(blockaccess::BlockAccesserSPtr block_accesser)
    : running_(false),
      block_accesser_(block_accesser),
      task_thread_pool_(
          std::make_unique<TaskThreadPool>("cache_storage_worker")),
      submit_queue_id_({0}) {}

Status StorageImpl::Init() {
  if (!running_.exchange(true)) {
    auto status = block_accesser_->Init();
    if (!status.ok()) {
      LOG(ERROR) << "Init block accesser failed: " << status.ToString();
      return status;
    }

    int rc = task_thread_pool_->Start(3);
    if (rc != 0) {
      LOG(ERROR) << "Start storage thread pool failed: rc = " << rc;
      return Status::Internal("start storage thread pool fail");
    }

    bthread::ExecutionQueueOptions queue_options;
    queue_options.use_pthread = true;
    rc = bthread::execution_queue_start(&submit_queue_id_, &queue_options,
                                        Executor, this);
    if (rc != 0) {
      LOG(ERROR) << "Start storage submit execution queue failed: rc = " << rc;
      return Status::Internal("start storage submit execution queue fail");
    }
  }
  return Status::OK();
}

Status StorageImpl::Shutdown() {
  if (running_.exchange(false)) {
    bthread::execution_queue_stop(submit_queue_id_);
    int rc = bthread::execution_queue_join(submit_queue_id_);
    if (rc != 0) {
      LOG(ERROR) << "Stop storage submit execution queue failed: rc = " << rc;
      return Status::Internal("stop storage submit execution queue failed");
    }
    task_thread_pool_->Stop();
  }
  return Status::OK();
}

Status StorageImpl::Put(const std::string& key, const IOBuffer& buffer) {
  auto closure = StoragePutClosure(key, buffer);
  CHECK_EQ(0, bthread::execution_queue_execute(submit_queue_id_, &closure));
  closure.Wait();
  return closure.status();
}

Status StorageImpl::Range(const std::string& key, off_t offset, size_t length,
                          IOBuffer* buffer) {
  auto closure = StorageRangeClosure(key, offset, length, buffer);
  CHECK_EQ(0, bthread::execution_queue_execute(submit_queue_id_, &closure));
  closure.Wait();
  return closure.status();
}

int StorageImpl::Executor(void* meta,
                          bthread::TaskIterator<StorageClosure*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  StorageImpl* storage = static_cast<StorageImpl*>(meta);
  for (; iter; iter++) {
    auto* closure = *iter;
    storage->task_thread_pool_->Enqueue(  // multi-thread for memory copy
        [storage, closure]() { storage->Execute(closure); });
  }
  return 0;
}

StorageOperator* StorageImpl::NewOperator(StorageClosure* closure) {
  if (IsPutOp(closure)) {
    return new PutOp(closure);
  } else if (IsRangeOp(closure)) {
    return new RangeOp(closure);
  }

  CHECK(false) << "Unknown storage operation type: "
               << static_cast<int>(closure->optype);
}

void StorageImpl::Execute(StorageClosure* closure) {
  auto* op = NewOperator(closure);
  char* data = op->OnPrepare();
  auto retry_cb = [op](int code) {
    bool succ = (code == 0);
    auto strategy = op->OnRetry(succ);
    if (strategy == RetryStrategy::kNotRetry) {
      op->OnComplete(succ);
    }
    return strategy;
  };

  if (IsPutOp(closure)) {
    block_accesser_->AsyncPut(closure->key, data, closure->length, retry_cb);
  } else {
    block_accesser_->AsyncRange(closure->key, closure->offset, closure->length,
                                data, retry_cb);
  }
}

}  // namespace cache
}  // namespace dingofs
