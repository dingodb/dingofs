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
#include <memory>

#include "cache/blockcache/block_cache_upload_queue.h"
#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/utils/bthread.h"
#include "cache/utils/context.h"
#include "cache/utils/infight_throttle.h"
#include "options/cache/option.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(
    upload_stage_max_inflights, 32,
    "Maximum inflight requests for uploading stage blocks to storage");

static const std::string kModule = "uploader";

BlockCacheUploader::BlockCacheUploader(CacheStoreSPtr store,
                                       StoragePoolSPtr storage_pool)
    : running_(false),
      store_(store),
      storage_pool_(storage_pool),
      pending_queue_(std::make_unique<PendingQueue>()),
      inflights_(
          std::make_unique<InflightThrottle>(FLAGS_upload_stage_max_inflights)),
      thread_pool_(std::make_unique<TaskThreadPool>("upload_stage")),
      joiner_(std::make_unique<BthreadJoiner>()) {}

BlockCacheUploader::~BlockCacheUploader() { Shutdown(); }

void BlockCacheUploader::Start() {
  CHECK_NOTNULL(store_);
  CHECK_NOTNULL(storage_pool_);
  CHECK_NOTNULL(pending_queue_);
  CHECK_NOTNULL(inflights_);
  CHECK_NOTNULL(thread_pool_);
  CHECK_NOTNULL(joiner_);

  if (running_) {
    return;
  }

  LOG(INFO) << "Block cache uploader is starting...";

  auto status = joiner_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Failed to start bthread joiner: " << status.ToString();
    return;
  }

  running_ = true;

  CHECK_EQ(thread_pool_->Start(1), 0) << "Failed to start thread pool.";
  thread_pool_->Enqueue(&BlockCacheUploader::UploadingWorker, this);

  LOG(INFO) << "Block cache uploader is up.";

  CHECK_RUNNING("Block cache uploader");
}

void BlockCacheUploader::Shutdown() {
  if (!running_.exchange(false)) {
    LOG(INFO) << "Block cache uploader is already shutting down.";
    return;
  }

  LOG(INFO) << "Block cache uploader is shutting down...";

  thread_pool_->Stop();
  joiner_->Shutdown();

  LOG(INFO) << "Block cache uploader is down.";

  CHECK_DOWN("Block cache uploader");
}

void BlockCacheUploader::AddStagingBlock(const StagingBlock& block) {
  DCHECK_RUNNING("Block cache uploader");

  VLOG(9) << "Add staging block to pending queue: key = "
          << block.key.Filename() << ", length = " << block.length
          << ", from = " << static_cast<int>(block.block_ctx.from);

  pending_queue_->Push(block);
}

void BlockCacheUploader::UploadingWorker() {
  CHECK_RUNNING("Block cache uploader");

  WaitStoreUp();

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
  inflights_->Increment(1);

  auto* self = GetSelfPtr();
  auto tid = RunInBthread([self, staging_block]() {
    auto status = self->Uploading(staging_block);
    self->PostUploading(staging_block, status);
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void BlockCacheUploader::PostUploading(const StagingBlock& staging_block,
                                       Status status) {
  inflights_->Decrement(1);

  auto ctx = staging_block.ctx;
  auto key = staging_block.key;
  if (status.ok() || status.IsNotFound()) {
    return;
  } else if (status.IsCacheDown()) {
    LOG_CTX(ERROR)
        << "Uploading staging block failed for cache is down, it will "
           "re-upload after cache restart if the block still exist: key = "
        << key.Filename();
    return;
  }

  // error
  static const int sleep_ms = 100;
  LOG_CTX(ERROR) << "Uploading staging block failed, it will retry in "
                 << sleep_ms << " millisecond: key =" << key.Filename()
                 << ", status = " << status.ToString();

  if (IsRunning()) {
    bthread_usleep(sleep_ms * 1000);
    AddStagingBlock(staging_block);
  }
}

Status BlockCacheUploader::Uploading(const StagingBlock& staging_block) {
  Status status;
  auto ctx = staging_block.ctx;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "upload(%s,%zu)",
                    staging_block.key.Filename(), staging_block.length);
  StepTimerGuard guard(timer);

  NEXT_STEP("load");
  IOBuffer buffer;
  status = Load(staging_block, &buffer);  // FIXME
  if (!status.ok()) {
    return status;
  }

  NEXT_STEP("s3_put");
  status = Upload(staging_block, buffer);
  if (!status.ok()) {
    return status;
  }

  NEXT_STEP("remove_stage");
  status = RemoveStage(staging_block);
  return status;
}

Status BlockCacheUploader::Load(const StagingBlock& staging_block,
                                IOBuffer* buffer) {
  auto ctx = staging_block.ctx;
  const auto& key = staging_block.key;

  auto status = store_->Load(ctx, key, 0, staging_block.length, buffer);
  if (status.IsNotFound()) {
    LOG_CTX(ERROR) << "Load staging block failed which already deleted, "
                      "abort upload: key = "
                   << key.Filename() << ", status = " << status.ToString();
  } else if (!status.ok()) {
    LOG_CTX(ERROR) << "Load staging block failed: key = " << key.Filename()
                   << ", status = " << status.ToString();
  }
  return status;
}

Status BlockCacheUploader::Upload(const StagingBlock& staging_block,
                                  const IOBuffer& buffer) {
  auto ctx = staging_block.ctx;
  const auto& key = staging_block.key;

  StorageSPtr storage;
  auto status = storage_pool_->GetStorage(key.fs_id, storage);
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Get storage failed: key = " << key.Filename()
                   << ", status = " << status.ToString();
    return status;
  }

  status = storage->Put(staging_block.ctx, key, Block(buffer));
  if (!status.ok()) {
    LOG_CTX(ERROR) << "Upload staging block failed: key = " << key.Filename()
                   << ", status = " << status.ToString();
    return status;
  }

  return Status::OK();
}

Status BlockCacheUploader::RemoveStage(const StagingBlock& staging_block) {
  auto ctx = staging_block.ctx;
  const auto& key = staging_block.key;

  CacheStore::RemoveStageOption option;
  option.block_ctx = staging_block.block_ctx;
  auto status = store_->RemoveStage(staging_block.ctx, key, option);
  if (!status.ok()) {
    LOG_CTX(WARNING) << "Remove staging block failed: key = " << key.Filename()
                     << ", status = " << status.ToString();
    status = Status::OK();  // ignore removestage error
  }

  return status;
}

void BlockCacheUploader::WaitStoreUp() {
  while (!store_->IsRunning()) {
    bthread_usleep(1000 * 1000);
  }
}

}  // namespace cache
}  // namespace dingofs
