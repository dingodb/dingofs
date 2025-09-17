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

#include "cache/storage/storage_closure.h"

#include <brpc/reloadable_flags.h>
#include <butil/iobuf.h>
#include <bvar/reducer.h>
#include <gflags/gflags.h>

#include <cstdint>
#include <memory>

#include "blockaccess/accesser_common.h"
#include "cache/storage/storage.h"
#include "cache/utils/execution_queue.h"
#include "cache/utils/helper.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_int64(storage_download_retry_timeout_s, 1800,
             "Timeout in seconds for download retry");
DEFINE_validator(storage_download_retry_timeout_s, brpc::PassValidate);

UploadClosure::UploadClosure(ContextSPtr ctx, const BlockKey& key,
                             const Block& block, UploadOption option,
                             blockaccess::BlockAccesser* block_accesser,
                             ExecutionQueueSPtr retry_queue)
    : ctx_(ctx),
      key_(key),
      block_(block),
      option_(option),
      block_accesser_(block_accesser),
      retry_queue_(retry_queue) {}

void UploadClosure::Run() {
  auto ctx = OnPrepare();
  block_accesser_->AsyncPut(ctx);
}

blockaccess::PutObjectAsyncContextSPtr UploadClosure::OnPrepare() {
  auto block = CopyBlock();  // Copy data to continuous memory
  if (option_.async_cache_func) {
    option_.async_cache_func(key_, block);
  }

  auto ctx = std::make_shared<blockaccess::PutObjectAsyncContext>();
  ctx->start_time = butil::gettimeofday_us();
  ctx->key = key_.StoreKey();
  ctx->buffer = block.buffer.Fetch1();
  ctx->buffer_size = block.buffer.Size();
  ctx->retry = 0;
  ctx->cb = [this](const blockaccess::PutObjectAsyncContextSPtr& ctx) {
    OnCallback(ctx);
  };
  return ctx;
}

void UploadClosure::OnCallback(
    const blockaccess::PutObjectAsyncContextSPtr& ctx) {
  auto status = ctx->status;
  if (status.ok()) {
    OnComplete(status);
  } else {
    OnRetry(ctx);
  }
}

void UploadClosure::OnRetry(const blockaccess::PutObjectAsyncContextSPtr& ctx) {
  ctx->retry++;
  LOG(WARNING) << "Retry upload block: key = " << ctx->key << ", retry("
               << ctx->retry << "), elapsed("
               << (butil::gettimeofday_us() - ctx->start_time) / 1e6 << "s)"
               << ",  status = " << ctx->status.ToString();

  retry_queue_->Submit([this, ctx]() { block_accesser_->AsyncPut(ctx); });
}

void UploadClosure::OnComplete(Status s) {
  StorageClosure::status() = s;
  StorageClosure::Run();
}

Block UploadClosure::CopyBlock() {
  char* data = new char[block_.size];
  block_.buffer.CopyTo(data);

  IOBuffer buffer;
  buffer.AppendUserData(data, block_.size, Helper::DeleteBuffer);
  return Block(buffer);
}

DownloadClosure::DownloadClosure(ContextSPtr ctx, const BlockKey& key,
                                 off_t offset, size_t length, IOBuffer* buffer,
                                 DownloadOption option,
                                 blockaccess::BlockAccesser* block_accesser,
                                 ExecutionQueueSPtr retry_queue)
    : ctx_(ctx),
      key_(key),
      offset_(offset),
      length_(length),
      buffer_(buffer),
      option_(option),
      block_accesser_(block_accesser),
      retry_queue_(retry_queue) {}

void DownloadClosure::Run() {
  auto ctx = OnPrepare();
  block_accesser_->AsyncGet(ctx);
}

blockaccess::GetObjectAsyncContextSPtr DownloadClosure::OnPrepare() {
  char* data = new char[length_];
  buffer_->AppendUserData(data, length_, Helper::DeleteBuffer);

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

void DownloadClosure::OnCallback(
    const blockaccess::GetObjectAsyncContextSPtr& ctx) {
  auto status = ctx->status;
  if (status.ok() || status.IsNotFound()) {
    CHECK_EQ(ctx->actual_len, length_)
        << "actual_len: " << ctx->actual_len << ", expected_len: " << length_;
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

void DownloadClosure::OnRetry(
    const blockaccess::GetObjectAsyncContextSPtr& ctx) {
  ctx->retry++;
  LOG(WARNING) << "Retry download block: key = " << ctx->key << ", retry("
               << ctx->retry << "), elapsed("
               << (butil::gettimeofday_us() - ctx->start_time) / 1e6 << "s)"
               << ",  status = " << ctx->status.ToString();

  retry_queue_->Submit([this, ctx]() { block_accesser_->AsyncGet(ctx); });
}

void DownloadClosure::OnComplete(Status s) {
  StorageClosure::status() = s;
  StorageClosure::Run();
}

}  // namespace cache
}  // namespace dingofs
