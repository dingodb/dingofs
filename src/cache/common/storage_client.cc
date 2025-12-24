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

#include "cache/common/storage_client.h"

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <butil/iobuf.h>
#include <glog/logging.h>
#include <sys/types.h>

#include "cache/blockcache/cache_store.h"
#include "cache/iutil/string_util.h"
#include "cache/iutil/task_execution_queue.h"
#include "common/blockaccess/block_accesser.h"

namespace dingofs {
namespace cache {

DEFINE_int64(storage_upload_retry_timeout_s, 1800,
             "timeout in seconds for upload retry");
DEFINE_validator(storage_upload_retry_timeout_s, brpc::PassValidate);

DEFINE_int64(storage_download_retry_timeout_s, 1800,
             "timeout in seconds for download retry");
DEFINE_validator(storage_download_retry_timeout_s, brpc::PassValidate);

static int64_t GetQueueSize(void* meta) {
  iutil::TaskExecutionQueue* queue =
      static_cast<iutil::TaskExecutionQueue*>(meta);
  return queue->Size();
}

PutBlockTask::PutBlockTask(ContextSPtr ctx, const BlockKey& key,
                           const Block* block,
                           blockaccess::BlockAccesser* block_accesser,
                           iutil::TaskExecutionQueueSPtr retry_queue)
    : ctx_(ctx),
      key_(key),
      block_(block),
      block_accesser_(block_accesser),
      retry_queue_(retry_queue) {}

void PutBlockTask::Run() {
  auto ctx = OnPrepare();
  block_accesser_->AsyncPut(ctx);
}

blockaccess::PutObjectAsyncContextSPtr PutBlockTask::OnPrepare() {
  auto ctx = std::make_shared<blockaccess::PutObjectAsyncContext>();
  ctx->start_time = butil::gettimeofday_us();
  ctx->key = key_.StoreKey();
  ctx->buffer = block_->buffer.Fetch1();
  ctx->buffer_size = block_->buffer.Size();
  ctx->retry = 0;
  ctx->cb = [this](const blockaccess::PutObjectAsyncContextSPtr& ctx) {
    OnCallback(ctx);
  };
  return ctx;
}

void PutBlockTask::OnCallback(
    const blockaccess::PutObjectAsyncContextSPtr& ctx) {
  auto status = ctx->status;
  if (status.ok()) {
    OnComplete(status);
  } else if (ctx->start_time + FLAGS_storage_upload_retry_timeout_s * 1e6 >
             butil::gettimeofday_us()) {
    OnRetry(ctx);
  } else {
    LOG(ERROR) << "Upload block exceed max retry timeout: key = " << ctx->key
               << ", retry(" << ctx->retry << "), elapsed("
               << (butil::gettimeofday_us() - ctx->start_time) / 1e6 << "s)"
               << ",  status = " << ctx->status.ToString();
    OnComplete(status);
  }
}

void PutBlockTask::OnRetry(const blockaccess::PutObjectAsyncContextSPtr& ctx) {
  ctx->retry++;
  LOG(WARNING) << "Retry upload block: key = " << ctx->key << ", retry("
               << ctx->retry << "), elapsed("
               << (butil::gettimeofday_us() - ctx->start_time) / 1e6 << "s)"
               << ",  status = " << ctx->status.ToString();

  retry_queue_->Submit([this, ctx]() { block_accesser_->AsyncPut(ctx); });
}

void PutBlockTask::OnComplete(Status s) {
  TaskClosure::status() = s;
  TaskClosure::Run();
}

RangeBlockTask::RangeBlockTask(ContextSPtr ctx, const BlockKey& key,
                               off_t offset, size_t length, IOBuffer* buffer,
                               blockaccess::BlockAccesser* block_accesser,
                               iutil::TaskExecutionQueueSPtr retry_queue)
    : ctx_(ctx),
      key_(key),
      offset_(offset),
      length_(length),
      buffer_(buffer),
      block_accesser_(block_accesser),
      retry_queue_(retry_queue) {}

void RangeBlockTask::Run() {
  auto ctx = OnPrepare();
  block_accesser_->AsyncGet(ctx);
}

blockaccess::GetObjectAsyncContextSPtr RangeBlockTask::OnPrepare() {
  char* data = new char[length_];
  buffer_->AppendUserData(data, length_, iutil::DeleteBuffer);

  auto ctx = std::make_shared<blockaccess::GetObjectAsyncContext>();
  ctx->start_time = butil::gettimeofday_us();
  ctx->key = key_.StoreKey();
  ctx->buf = buffer_->Fetch1();
  ctx->offset = offset_;
  ctx->len = length_;
  ctx->retry = 0;
  ctx->actual_len = 0;
  ctx->cb = [this](const blockaccess::GetObjectAsyncContextSPtr& ctx) {
    OnCallback(ctx);
  };

  return ctx;
}

void RangeBlockTask::OnCallback(
    const blockaccess::GetObjectAsyncContextSPtr& ctx) {
  auto status = ctx->status;
  if (status.ok()) {
    CHECK_EQ(ctx->actual_len, length_)
        << "actual_len: " << ctx->actual_len << ", expected_len: " << length_;
    OnComplete(status);
  } else if (status.IsNotFound()) {
    LOG(WARNING) << "Download block failed, object not found : key = "
                 << ctx->key << ",  status = " << ctx->status.ToString();
    OnComplete(status);
  } else if (ctx->start_time + FLAGS_storage_download_retry_timeout_s * 1e6 >
             butil::gettimeofday_us()) {
    OnRetry(ctx);
  } else {
    LOG(ERROR) << "Download block exceed max retry timeout: key = " << ctx->key
               << ", retry(" << ctx->retry << "), elapsed("
               << (butil::gettimeofday_us() - ctx->start_time) / 1e6 << "s)"
               << ",  status = " << ctx->status.ToString();
    OnComplete(status);
  }
}

void RangeBlockTask::OnRetry(
    const blockaccess::GetObjectAsyncContextSPtr& ctx) {
  ctx->retry++;
  LOG(WARNING) << "Retry download block: key = " << ctx->key << ", retry("
               << ctx->retry << "), elapsed("
               << (butil::gettimeofday_us() - ctx->start_time) / 1e6 << "s)"
               << ",  status = " << ctx->status.ToString();

  retry_queue_->Submit([this, ctx]() { block_accesser_->AsyncGet(ctx); });
}

void RangeBlockTask::OnComplete(Status s) {
  TaskClosure::status() = s;
  TaskClosure::Run();
}

StorageClient::StorageClient(blockaccess::BlockAccesser* block_accesser)
    : running_(false),
      block_accesser_(block_accesser),
      queue_id_({0}),
      upload_retry_queue_(std::make_shared<iutil::TaskExecutionQueue>()),
      download_retry_queue_(std::make_shared<iutil::TaskExecutionQueue>()),
      num_upload_retry_task_("dingofs_storage_upload_retry_count", GetQueueSize,
                             upload_retry_queue_.get()),
      num_download_retry_task_("dingofs_storage_download_retry_count",
                               GetQueueSize, download_retry_queue_.get()) {}

Status StorageClient::Start() {
  if (running_.exchange(true)) {
    LOG(WARNING) << "StorageClient already running";
    return Status::OK();
  }

  LOG(INFO) << "StorageClient is starting...";

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&queue_id_, &queue_options,
                                          HandleClosure, this);
  if (rc != 0) {
    LOG(ERROR) << "Fail to start ExecutionQueue";
    return Status::Internal("start execution queue fail");
  }

  upload_retry_queue_->Start();
  download_retry_queue_->Start();

  LOG(INFO) << "StorageClient is up";

  return Status::OK();
}

Status StorageClient::Shutdown() {
  if (!running_.exchange(false)) {
    LOG(WARNING) << "StorageClient already down";
    return Status::OK();
  }

  LOG(INFO) << "StorageClient is shutting down...";

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    LOG(ERROR) << "Fail to stop ExecutionQueue";
    return Status::Internal("stop execution queue failed");
  } else if (bthread::execution_queue_join(queue_id_) != 0) {
    LOG(ERROR) << "Fail to join ExecutionQueue";
    return Status::Internal("join execution queue failed");
  }

  upload_retry_queue_->Shutdown();
  download_retry_queue_->Shutdown();

  LOG(INFO) << "StorageClient is down";
  return Status::OK();
}

Status StorageClient::Put(ContextSPtr ctx, const BlockKey& key,
                          const Block* block) {
  auto task =
      PutBlockTask(ctx, key, block, block_accesser_, upload_retry_queue_);
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, &task));

  task.Wait();
  auto status = task.status();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to put block to storage, task=" << task;
  }
  return status;
}

Status StorageClient::Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                            size_t length, IOBuffer* buffer) {
  auto task = RangeBlockTask(ctx, key, offset, length, buffer, block_accesser_,
                             download_retry_queue_);
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, &task));

  task.Wait();
  auto status = task.status();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to range block from storage, task=" << task;
  }
  return status;
}

int StorageClient::HandleClosure(void* meta,
                                 bthread::TaskIterator<TaskClosure*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  StorageClient* self = static_cast<StorageClient*>(meta);
  for (; iter; iter++) {
    auto* task = *iter;
    task->Run();
  }
  return 0;
}

std::ostream& operator<<(std::ostream& os, const PutBlockTask& task) {
  os << "PutBlockTask{key=" << task.key_.Filename()
     << " size=" << task.block_->buffer.Size() << "}";
  return os;
}

std::ostream& operator<<(std::ostream& os, const RangeBlockTask& task) {
  os << "RangeBlockTask{key=" << task.key_.Filename()
     << " offset=" << task.offset_ << " length=" << task.length_ << "}";
  return os;
}

}  // namespace cache
}  // namespace dingofs
