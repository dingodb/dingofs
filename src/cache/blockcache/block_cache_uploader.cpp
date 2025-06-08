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

#include "cache/config/block_cache.h"
#include "cache/utils/access_log.h"
#include "cache/utils/bthread.h"
#include "cache/utils/infight_throttle.h"
#include "cache/utils/phase_timer.h"

namespace dingofs {
namespace cache {

BlockCacheUploader::BlockCacheUploader(StoragePoolSPtr storage_pool,
                                       CacheStoreSPtr store)
    : running_(false),
      storage_pool_(storage_pool),
      store_(store),
      pending_queue_(std::make_unique<PendingQueue>()),
      upload_stage_thread_pool_(
          std::make_unique<TaskThreadPool>("upload_stage_worker")),
      inflights_throttle_(
          std::make_unique<InflightThrottle>(FLAGS_upload_stage_max_inflights)),
      uploading_count_(0) {}

BlockCacheUploader::~BlockCacheUploader() { Shutdown(); }

void BlockCacheUploader::Init() {
  if (!running_.exchange(true)) {
    LOG(INFO) << "Block cache uploader starting...";

    CHECK(upload_stage_thread_pool_->Start(1) == 0);
    upload_stage_thread_pool_->Enqueue(&BlockCacheUploader::UploadingWorker,
                                       this);

    LOG(INFO) << "Block cache uploader started.";
  }
}

void BlockCacheUploader::Shutdown() {
  if (running_.exchange(false)) {
    LOG(INFO) << "Block cache uploader shutdowning...";

    upload_stage_thread_pool_->Stop();
    uploading_count_.reset(0);

    LOG(INFO) << "Block cache uploader stopped.";
  }
}

void BlockCacheUploader::AddStageBlock(const StageBlock& block) {
  pending_queue_->Push(block);
  uploading_count_.add_count(1);
}

void BlockCacheUploader::WaitAllUploaded() { uploading_count_.wait(); }

void BlockCacheUploader::UploadingWorker() {
  while (running_.load(std::memory_order_relaxed)) {
    auto stage_blocks = pending_queue_->Pop();
    if (stage_blocks.empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    for (const auto& block : stage_blocks) {
      UploadBlock(block);
    }
  }
}

void BlockCacheUploader::UploadBlock(const StageBlock& block) {
  inflights_throttle_->Increment(1);

  auto self = GetSelfSPtr();
  RunInBthread([self, block]() {
    auto status = self->DoUpload(block);
    self->inflights_throttle_->Decrement(1);
    self->uploading_count_.signal(1);

    if (!status.ok() && !status.IsNotFound()) {
      bthread_usleep(100 * 1000);
      self->AddStageBlock(block);  // retry
    }
  });
}

Status BlockCacheUploader::DoUpload(const StageBlock& block) {
  Status status;
  PhaseTimer timer;
  LogGuard log([&]() {
    return absl::StrFormat("[local] upload_stage(%s,%zu): %s%s",
                           block.key.Filename(), block.length,
                           status.ToString(), timer.ToString());
  });

  // load block
  IOBuffer buffer;
  const auto& key = block.key;
  status = store_->Load(key, 0, block.length, &buffer);
  if (status.IsNotFound()) {
    LOG(ERROR) << "Stage block (key=" << key.Filename()
               << ") already deleted, abort upload!";
    return status;
  } else if (!status.ok()) {
    LOG(ERROR) << "Read stage block (key=" << key.Filename()
               << ") failed: " << status.ToString() << ", abort upload!";
    return status;
  }

  // put to storage
  status = StoragePut(key, buffer);
  if (!status.ok()) {
    LOG(ERROR) << "Upload stage block (key=" << key.Filename()
               << ") to storage failed: " << status.ToString();
    return status;
  }

  // remove stage block
  status = store_->RemoveStage(key, CacheStore::RemoveStageOption(block.ctx));
  if (!status.ok()) {
    LOG(WARNING) << "Remove stage block (path=" << key.Filename()
                 << ") after upload failed: " << status.ToString();
    status = Status::OK();  // ignore removestage error
  }

  return status;
}

Status BlockCacheUploader::StoragePut(const BlockKey& key,
                                      const IOBuffer& buffer) {
  StorageSPtr storage;
  auto status = storage_pool_->GetStorage(key.fs_id, storage);
  if (status.ok()) {
    status = storage->Put(key.StoreKey(), buffer);
  }
  return status;
}

}  // namespace cache
}  // namespace dingofs
