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
 * Created Date: 2024-09-25
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/block_cache_uploader.h"

#include <bthread/bthread.h>

#include <atomic>

#include "cache/blockcache/block_cache_upload_queue.h"
#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/config/blockcache.h"
#include "cache/utils/bthread.h"
#include "cache/utils/context.h"
#include "cache/utils/infight_throttle.h"
#include "cache/utils/step_timer.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(
    upload_stage_max_inflights, 128,
    "Maximum inflight requests for uploading stage blocks to storage");

static const std::string kModule = kBlockCacheUploaderMoudule;

BlockCacheUploader::BlockCacheUploader(StoragePoolSPtr storage_pool,
                                       CacheStoreSPtr store)
    : running_(false),
      storage_pool_(storage_pool),
      store_(store),
      pending_queue_(std::make_unique<PendingQueue>()),
      thread_pool_(std::make_unique<TaskThreadPool>("upload_stage")),
      inflights_throttle_(
          std::make_unique<InflightThrottle>(FLAGS_upload_stage_max_inflights)),
      uploading_count_(0) {}

BlockCacheUploader::~BlockCacheUploader() { Shutdown(); }

void BlockCacheUploader::Start() {
  CHECK_NOTNULL(storage_pool_);
  CHECK_NOTNULL(store_);
  CHECK_NOTNULL(pending_queue_);
  CHECK_NOTNULL(thread_pool_);
  CHECK_NOTNULL(inflights_throttle_);

  if (running_) {
    return;
  }

  LOG(INFO) << "Block cache uploader is starting...";

  CHECK_EQ(thread_pool_->Start(1), 0) << "Failed to start thread pool.";
  thread_pool_->Enqueue(&BlockCacheUploader::UploadingWorker, this);

  running_.store(true);

  LOG(INFO) << "Block cache uploader is up.";

  CHECK_RUNNING("Block cache uploader");
}

void BlockCacheUploader::Shutdown() {
  if (!running_) {  // Already shutdown
    return;
  }

  LOG(INFO) << "Block cache uploader is shutting down...";

  thread_pool_->Stop();
  uploading_count_.reset(0);

  running_.store(false);

  LOG(INFO) << "Block cache uploader is down.";

  CHECK_DOWN("Block cache uploader");
}

void BlockCacheUploader::AddStagingBlock(const StagingBlock& block) {
  DCHECK_RUNNING("Block cache uploader");

  uploading_count_.add_count(1);
  pending_queue_->Push(block);
}

void BlockCacheUploader::WaitAllUploaded() { uploading_count_.wait(); }

void BlockCacheUploader::UploadingWorker() {
  while (IsRunning()) {
    auto blocks = pending_queue_->Pop();
    if (blocks.empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    for (const auto& staging_block : blocks) {
      AsyncUploading(staging_block);
    }
  }
}

void BlockCacheUploader::AsyncUploading(const StagingBlock& staging_block) {
  inflights_throttle_->Increment(1);

  auto* self = GetSelfPtr();
  RunInBthread([self, staging_block]() {
    auto status = self->Uploading(staging_block);
    self->inflights_throttle_->Decrement(1);

    if (status.ok() || status.IsNotFound()) {
      self->uploading_count_.signal(1);
    } else {
      bthread_usleep(100 * 1000);
      self->AddStagingBlock(staging_block);  // retry
    }
  });
}

Status BlockCacheUploader::Uploading(const StagingBlock& staging_block) {
  Status status;
  IOBuffer buffer;
  auto ctx = staging_block.ctx;
  TracingGuard tracing(ctx, status, kModule, "uploading(%s,%zu)",
                       staging_block.key.Filename(), staging_block.length);

  NEXT_STEP(kLoad);
  status = Load(staging_block, &buffer);
  if (!status.ok()) {
    return status;
  }

  NEXT_STEP(kS3Put);
  status = Upload(staging_block, buffer);
  if (!status.ok()) {
    return status;
  }

  NEXT_STEP(kS3Put);
  status = RemoveStage(staging_block);

  return status;
}

Status BlockCacheUploader::Load(const StagingBlock& staging_block,
                                IOBuffer* buffer) {
  const auto& key = staging_block.key;
  auto status =
      store_->Load(staging_block.ctx, key, 0, staging_block.length, buffer);
  if (status.IsNotFound()) {
    LOG(ERROR) << "Load staging block failed which already deleted, abort "
                  "upload: key = "
               << key.Filename() << ", status = " << status.ToString();
    return status;
  } else if (!status.ok()) {
    LOG(ERROR) << "Load block failed: key = " << key.Filename()
               << ", status = " << status.ToString();
    return status;
  }

  return Status::OK();
}

Status BlockCacheUploader::Upload(const StagingBlock& staging_block,
                                  const IOBuffer& buffer) {
  const auto& key = staging_block.key;

  StorageSPtr storage;
  auto status = storage_pool_->GetStorage(key.fs_id, storage);
  if (!status.ok()) {
    LOG(ERROR) << "Get storage failed: key = " << key.Filename()
               << ", status = " << status.ToString();
    return status;
  }

  status = storage->Put(staging_block.ctx, key, Block(buffer));
  if (!status.ok()) {
    LOG(ERROR) << "Upload staging block failed: key = " << key.Filename()
               << ", status = " << status.ToString();
    return status;
  }

  return Status::OK();
}

Status BlockCacheUploader::RemoveStage(const StagingBlock& staging_block) {
  const auto& key = staging_block.key;
  CacheStore::RemoveStageOption option;
  option.block_ctx = staging_block.block_ctx;
  auto status = store_->RemoveStage(staging_block.ctx, key, option);
  if (!status.ok()) {
    LOG(WARNING) << "Remove staging block failed: key = " << key.Filename()
                 << ", status = " << status.ToString();
    status = Status::OK();  // ignore removestage error
  }

  return status;
}

bool BlockCacheUploader::IsRunning() const {
  return running_.load(std::memory_order_relaxed);
}

}  // namespace cache
}  // namespace dingofs
