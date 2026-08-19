/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "blockcache/block/uploader.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <utility>

#include "blockcache/core/runtime/smp.h"
#include "blockcache/utils/align.h"

namespace dingofs {
namespace blockcache {

DEFINE_uint32(upload_stage_max_inflights, 32, "upload stage block concurrency");
DEFINE_uint32(upload_stage_max_tries, 3, "max tries for upload stage block");
DEFINE_validator(upload_stage_max_tries, brpc::PassValidate);
DEFINE_uint32(upload_stage_retry_delay_s, 60, "delay before a failed retry");
DEFINE_validator(upload_stage_retry_delay_s, brpc::PassValidate);

Uploader::Uploader(CacheStore* store, ObjectStorage* storage)
    : store_(store),
      storage_(storage),
      inflight_(0),
      max_inflight_(
          std::max(1U, FLAGS_upload_stage_max_inflights / ShardCount())) {}

Future<> Uploader::Start() {
  LOG(INFO) << "Uploader is starting...";

  running_ = true;
  (void)Worker();

  LOG(INFO) << "Successfully start Uploader{max_inflights=" << max_inflight_
            << "}";
  return MakeReadyFuture<>();
}

Future<> Uploader::Shutdown() {
  running_ = false;
  queue_.clear();
  return gate_.Close();
}

void Uploader::Enqueue(BlockHandle handle) {
  DCHECK(HasReactor()) << "Enqueue is shard-only (all feeds are)";
  if (!running_) {
    LOG(WARNING) << "Fail to enqueue " << handle << "  for uploader is down";
    return;
  }
  queue_.push_back(handle);
}

Future<> Uploader::Worker() {
  Gate::Holder holder(gate_);
  CHECK(holder.ok()) << "Uploader is shutdown";

  while (running_) {
    if (queue_.empty() || inflight_ >= max_inflight_) {
      co_await Sleep(std::chrono::milliseconds(kIdleSleepMs));
      continue;
    }

    const BlockHandle handle = queue_.front();
    queue_.pop_front();
    UploadingBlock block{.handle = handle, .holder = Gate::Holder(gate_)};
    if (!block.holder.ok()) {
      co_return;
    }

    ++inflight_;
    (void)UploadOne(std::move(block));
  }
}

Future<> Uploader::UploadOne(UploadingBlock block) {
  const Status status = co_await SendToStorage(block.handle);
  if (status.ok()) {
    const Status removed = co_await store_->RemoveStage(block.handle);
    LOG_IF(WARNING, !removed.ok() && !IsGone(removed))
        << "Fail to remove the uploaded " << block.handle << ": "
        << removed.ToString();
  } else if (IsGone(status)) {
    LOG(WARNING) << "Fail to upload stage " << block.handle
                 << " that is already gone: " << status.ToString();
  } else {
    LOG(WARNING) << "Fail to upload stage " << block.handle
                 << ", it will retry " << FLAGS_upload_stage_retry_delay_s
                 << " seconds: " << status.ToString();
    (void)RetryLater(std::move(block));
  }

  --inflight_;
}

Future<Status> Uploader::SendToStorage(BlockHandle handle) {
  char* buffer = AlignedAlloc(handle.size);
  if (buffer == nullptr) {
    co_return Status::OutOfMemory("no memory for upload scratch");
  }

  Status status = co_await store_->Load(handle, 0, handle.size, buffer);
  if (status.ok()) {
    const BufferView block(buffer, handle.size);
    status = co_await storage_->Put(
        handle, {&block, 1}, {.max_tries = FLAGS_upload_stage_max_tries});
  }
  std::free(buffer);
  co_return status;
}

Future<> Uploader::RetryLater(UploadingBlock block) {
  co_await SleepWhile([this] { return running_; },
                      uint64_t{FLAGS_upload_stage_retry_delay_s} * 1000);
  Enqueue(block.handle);
}

}  // namespace blockcache
}  // namespace dingofs
