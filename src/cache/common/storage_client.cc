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

#include <brpc/reloadable_flags.h>
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <butil/time.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <algorithm>
#include <memory>

#include "cache/local/cache_store.h"
#include "common/blockaccess/block_accesser.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(storage_upload_max_tries, 10,
              "maximum tries (including the first attempt) for uploading one "
              "block to storage");
DEFINE_validator(storage_upload_max_tries, brpc::PassValidate);

DEFINE_uint32(storage_download_max_tries, 10,
              "maximum tries (including the first attempt) for downloading "
              "one block from storage");
DEFINE_validator(storage_download_max_tries, brpc::PassValidate);

DEFINE_uint32(storage_upload_retry_backoff_base_ms, 1000,
              "base backoff in milliseconds between upload retries, the real "
              "backoff is base * tried * tried, capped at 60 seconds");
DEFINE_validator(storage_upload_retry_backoff_base_ms, brpc::PassValidate);

DEFINE_uint32(storage_download_retry_backoff_base_ms, 300,
              "base backoff in milliseconds between download retries, the "
              "real backoff is base * tried, capped at 10 seconds");
DEFINE_validator(storage_download_retry_backoff_base_ms, brpc::PassValidate);

DEFINE_uint64(storage_upload_thread_pool_size, 4,
              "thread pool size for upload tasks");

static constexpr uint64_t kUploadBackoffCapMs = 60 * 1000;
static constexpr uint64_t kDownloadBackoffCapMs = 10 * 1000;

uint64_t UploadRetryBackoffMs(uint32_t tried) {
  uint64_t base = FLAGS_storage_upload_retry_backoff_base_ms;
  return std::min(base * tried * tried, kUploadBackoffCapMs);
}

uint64_t DownloadRetryBackoffMs(uint32_t tried) {
  uint64_t base = FLAGS_storage_download_retry_backoff_base_ms;
  return std::min(base * tried, kDownloadBackoffCapMs);
}

static blockaccess::PutPayload ToPutPayload(const IOBuffer& buffer) {
  // butil::IOBuf does not expose zero-length backing blocks, so every fetched
  // iovec satisfies PutPayload's non-empty segment invariant.
  auto iovs = buffer.Fetch();
  std::vector<blockaccess::PayloadSegment> segments;
  segments.reserve(iovs.size());
  for (const auto& iov : iovs) {
    segments.push_back({static_cast<const char*>(iov.iov_base), iov.iov_len});
  }
  return blockaccess::PutPayload::Build(std::move(segments));
}

StorageClient::StorageClient(blockaccess::BlockAccesser* block_accesser)
    : running_(false),
      block_accesser_(block_accesser),
      queue_id_({0}),
      thread_pool_(std::make_unique<utils::TaskThreadPool<
                       bthread::Mutex, bthread::ConditionVariable>>()) {}

Status StorageClient::Start() {
  if (running_.exchange(true)) {
    LOG(WARNING) << "StorageClient already running";
    return Status::OK();
  }

  LOG(INFO) << "StorageClient is starting...";

  CHECK_EQ(thread_pool_->Start(FLAGS_storage_upload_thread_pool_size), 0);

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&queue_id_, &queue_options,
                                          HandleTask, this);
  if (rc != 0) {
    LOG(ERROR) << "Fail to start ExecutionQueue";
    return Status::Internal("start execution queue fail");
  }

  LOG(INFO) << "StorageClient is up";

  return Status::OK();
}

Status StorageClient::Shutdown() {
  if (!running_.exchange(false)) {
    LOG(WARNING) << "StorageClient already down";
    return Status::OK();
  }

  LOG(INFO) << "StorageClient is shutting down...";

  // Wait for inflight Put calls to drain before stopping the thread pool:
  // Stop() drops queued tasks and a dropped dispatch would strand its waiter.
  // Put calls drain promptly since backoff sleeps abort once running_ is
  // false and new calls are rejected at entry.
  while (inflight_puts_.load(std::memory_order_acquire) > 0) {
    bthread_usleep(10 * 1000);
  }

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    LOG(ERROR) << "Fail to stop ExecutionQueue";
    return Status::Internal("stop execution queue failed");
  } else if (bthread::execution_queue_join(queue_id_) != 0) {
    LOG(ERROR) << "Fail to join ExecutionQueue";
    return Status::Internal("join execution queue failed");
  }

  thread_pool_->Stop();

  LOG(INFO) << "StorageClient is down";
  return Status::OK();
}

Status StorageClient::Put(BlockHandle handle, const IOBuffer& block,
                          RetryOption option) {
  // Counted so Shutdown() can wait for inflight Put calls to drain before
  // stopping the thread pool.
  inflight_puts_.fetch_add(1, std::memory_order_acq_rel);
  auto status = DoPut(handle, block, option);
  inflight_puts_.fetch_sub(1, std::memory_order_acq_rel);
  return status;
}

Status StorageClient::DoPut(const BlockHandle& handle, const IOBuffer& block,
                            RetryOption option) {
  if (!running_.load(std::memory_order_acquire)) {
    return Status::Abort("storage client is not running");
  }

  uint32_t max_tries =
      option.max_tries > 0 ? option.max_tries : FLAGS_storage_upload_max_tries;
  max_tries = std::max(max_tries, 1U);

  size_t size = block.Size();

  Status status;
  for (uint32_t tried = 1;; tried++) {
    status = PutAttempt(handle, block);
    if (status.ok()) {
      return status;
    } else if (!IsRetriable(status)) {
      LOG(ERROR) << "Give up uploading block for unretriable error: key = "
                 << handle.Filename() << ", size = " << size
                 << ", status = " << status.ToString();
      return status;
    } else if (tried >= max_tries) {
      LOG(ERROR) << "Upload block exceed max tries: key = " << handle.Filename()
                 << ", size = " << size << ", tried(" << tried << "/"
                 << max_tries << "), status = " << status.ToString();
      return status;
    }

    auto backoff_ms = UploadRetryBackoffMs(tried);
    num_upload_retry_ << 1;
    LOG(WARNING) << "Retry upload block: key = " << handle.Filename()
                 << ", size = " << size << ", tried(" << tried << "/"
                 << max_tries << "), backoff(" << backoff_ms
                 << "ms), status = " << status.ToString();

    if (!BackoffSleep(backoff_ms)) {
      return Status::Abort("storage client is shutting down");
    }
  }
}

Status StorageClient::PutAttempt(const BlockHandle& handle,
                                 const IOBuffer& block) {
  // The payload only references the block's backing segments (zero-copy);
  // rebuilding it per attempt is cheap iovec metadata gathering.
  auto aws_ctx = std::make_shared<blockaccess::PutObjectAsyncContext>(
      handle.StoreKey(), ToPutPayload(block));
  aws_ctx->start_time = butil::gettimeofday_us();
  aws_ctx->retry = 0;

  TaskClosure done;
  aws_ctx->cb = [&done](const blockaccess::PutObjectAsyncContextSPtr&) {
    done.Run();
  };

  pending_async_put_ << 1;
  thread_pool_->Enqueue([this, aws_ctx]() {
    pending_async_put_ << -1;

    num_async_put_ << 1;
    block_accesser_->AsyncPut(aws_ctx->origin_key, aws_ctx);
    num_async_put_ << -1;
  });

  done.Wait();
  return aws_ctx->status;
}

Status StorageClient::Range(BlockHandle handle, off_t offset, size_t length,
                            IOBuffer* buffer) {
  if (!running_.load(std::memory_order_acquire)) {
    return Status::Abort("storage client is not running");
  }

  uint32_t max_tries = std::max(FLAGS_storage_download_max_tries, 1U);

  // The caller pre-allocates a single backing block (slab/RDMA registered);
  // only one attempt is in flight at a time, so the same buffer is safely
  // reused across attempts.
  char* data = buffer->Fetch1();

  Status status;
  for (uint32_t tried = 1;; tried++) {
    size_t actual_len = 0;
    status = RangeAttempt(handle, offset, length, data, &actual_len);
    if (status.ok()) {
      if (actual_len != length) {
        // e.g. a ranged GET straddling the end of a truncated object gets
        // 206 with fewer bytes; retrying cannot heal it, fail so callers
        // degrade gracefully instead of aborting the process.
        LOG(ERROR) << "Downloaded block is shorter than requested: key = "
                   << handle.Filename() << ", offset = " << offset
                   << ", length = " << length
                   << ", actual_len = " << actual_len;
        return Status::Internal("downloaded block too short");
      }
      return status;
    } else if (status.IsNotFound()) {
      LOG(WARNING) << "Download block failed, object not found: key = "
                   << handle.Filename()
                   << ", status = " << status.ToString();
      return status;
    } else if (!IsRetriable(status)) {
      LOG(ERROR) << "Give up downloading block for unretriable error: key = "
                 << handle.Filename() << ", offset = " << offset
                 << ", length = " << length
                 << ", status = " << status.ToString();
      return status;
    } else if (tried >= max_tries) {
      LOG(ERROR) << "Download block exceed max tries: key = "
                 << handle.Filename() << ", offset = " << offset
                 << ", length = " << length << ", tried(" << tried << "/"
                 << max_tries << "), status = " << status.ToString();
      return status;
    }

    auto backoff_ms = DownloadRetryBackoffMs(tried);
    num_download_retry_ << 1;
    LOG(WARNING) << "Retry download block: key = " << handle.Filename()
                 << ", offset = " << offset << ", length = " << length
                 << ", tried(" << tried << "/" << max_tries << "), backoff("
                 << backoff_ms << "ms), status = " << status.ToString();

    if (!BackoffSleep(backoff_ms)) {
      return Status::Abort("storage client is shutting down");
    }
  }
}

Status StorageClient::RangeAttempt(const BlockHandle& handle, off_t offset,
                                   size_t length, char* data,
                                   size_t* actual_len) {
  auto aws_ctx =
      std::make_shared<blockaccess::GetObjectAsyncContext>(handle.StoreKey());
  aws_ctx->start_time = butil::gettimeofday_us();
  aws_ctx->buf = data;
  aws_ctx->offset = offset;
  aws_ctx->len = length;
  aws_ctx->retry = 0;
  aws_ctx->actual_len = 0;

  TaskClosure done;
  aws_ctx->cb = [&done](const blockaccess::GetObjectAsyncContextSPtr&) {
    done.Run();
  };

  DispatchTask task(
      [this, aws_ctx]() {
        block_accesser_->AsyncGet(aws_ctx->origin_key, aws_ctx);
      },
      [aws_ctx, &done]() {
        aws_ctx->status = Status::Abort("storage client is shutting down");
        done.Run();
      });
  if (bthread::execution_queue_execute(queue_id_, &task) != 0) {
    return Status::Abort("storage client is shutting down");
  }

  done.Wait();
  *actual_len = aws_ctx->actual_len;
  return aws_ctx->status;
}

bool StorageClient::BackoffSleep(uint64_t backoff_ms) {
  constexpr uint64_t kSliceMs = 100;
  while (backoff_ms > 0) {
    if (!running_.load(std::memory_order_acquire)) {
      return false;
    }
    auto sleep_ms = std::min(backoff_ms, kSliceMs);
    bthread_usleep(sleep_ms * 1000);
    backoff_ms -= sleep_ms;
  }
  return running_.load(std::memory_order_acquire);
}

int StorageClient::HandleTask(void* meta,
                              bthread::TaskIterator<DispatchTask*>& iter) {
  (void)meta;
  if (iter.is_queue_stopped()) {
    // brpc dispatches every successfully-enqueued task in a normal pass
    // before this stop pass, so this iteration is expected to be empty;
    // the Abort() is purely defensive in case that ever changes.
    for (; iter; iter++) {
      (*iter)->Abort();
    }
    return 0;
  }

  for (; iter; iter++) {
    (*iter)->Dispatch();
  }
  return 0;
}

}  // namespace cache
}  // namespace dingofs
