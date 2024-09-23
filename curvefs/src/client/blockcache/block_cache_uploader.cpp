/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#include "curvefs/src/client/blockcache/block_cache_uploader.h"

#include <chrono>
#include <memory>
#include <mutex>

#include "curvefs/src/client/blockcache/error.h"
#include "curvefs/src/client/blockcache/local_filesystem.h"
#include "curvefs/src/client/blockcache/phase_timer.h"
#include "curvefs/src/client/common/dynamic_config.h"

namespace curvefs {
namespace client {
namespace blockcache {

USING_FLAG(drop_page_cache);

BlockCacheUploader::BlockCacheUploader(std::shared_ptr<CacheStore> store,
                                       std::shared_ptr<S3Client> s3,
                                       std::shared_ptr<Countdown> stage_count)
    : store_(store), s3_(s3), stage_count_(stage_count) {
  scan_stage_thread_pool_ =
      std::make_unique<TaskThreadPool<>>("scan_stage_worker");
  upload_stage_thread_pool_ =
      std::make_shared<TaskThreadPool<>>("upload_stage_worker");
}

void BlockCacheUploader::Init(uint64_t upload_workers,
                              uint64_t upload_queue_size) {
  if (!running_.exchange(true)) {
    CHECK(scan_stage_thread_pool_->Start(1) == 0);
    CHECK(upload_stage_thread_pool_->Start(upload_workers, upload_queue_size) ==
          0);
    scan_stage_thread_pool_->Enqueue(&BlockCacheUploader::ScanStageBlock, this);
  }
}

void BlockCacheUploader::Shutdown() {
  if (running_.exchange(false)) {
    scan_stage_thread_pool_->Stop();
    upload_stage_thread_pool_->Stop();
  }
}

void BlockCacheUploader::AddStageBlock(const BlockKey& key,
                                       const std::string& stage_path,
                                       bool from_reload) {
  std::unique_lock<std::mutex> lk(mutex_);
  PreUpload(key, from_reload);
  if (!from_reload) {
    fast_queue_.emplace_back(PendingItem(key, stage_path, from_reload));
  } else {
    slow_queue_.emplace_back(PendingItem(key, stage_path, from_reload));
  }
}

size_t BlockCacheUploader::GetPendingSize() {
  std::unique_lock<std::mutex> lk(mutex_);
  return fast_queue_.size() + slow_queue_.size();
}

void BlockCacheUploader::ScanStageBlock() {
  while (running_.load(std::memory_order_relaxed)) {
    std::vector<PendingItem> queue;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      if (!fast_queue_.empty()) {
        queue.swap(fast_queue_);
      } else {
        queue.swap(slow_queue_);
      }
    }

    for (const auto& item : queue) {
      upload_stage_thread_pool_->Enqueue(&BlockCacheUploader::UploadStageBlock,
                                         this, item);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

BCACHE_ERROR BlockCacheUploader::ReadBlock(const BlockKey& key,
                                           const std::string& stage_path,
                                           std::shared_ptr<char>& buffer,
                                           size_t* length) {
  auto fs = NewTempLocalFileSystem();
  auto rc = fs->ReadFile(stage_path, buffer, length, FLAGS_drop_page_cache);
  if (rc == BCACHE_ERROR::NOT_FOUND) {
    LOG(WARNING) << "Stage block (" << key.Filename()
                 << ") already deleted, drop it!";
  } else if (rc != BCACHE_ERROR::OK) {
    LOG(ERROR) << "Read stage block (" << key.Filename()
               << ") failed: " << StrErr(rc);
  }
  return rc;
}

void BlockCacheUploader::UploadBlock(const BlockKey& key, bool from_reload,
                                     std::shared_ptr<char> buffer,
                                     size_t length,
                                     std::shared_ptr<LogGuard> log_guard) {
  auto callback = [key, from_reload, buffer, log_guard, this](int code) {
    if (code != 0) {
      LOG(ERROR) << "Object " << key.Filename()
                 << " upload failed, retCode=" << code;
      return true;  // retry
    }

    PostUpload(key, from_reload, true);
    auto rc = store_->RemoveStage(key);
    if (rc != BCACHE_ERROR::OK) {
      LOG(ERROR) << "Remove stage block (" << key.Filename()
                 << ") failed: " << StrErr(rc);
    }
    return false;
  };
  s3_->AsyncPut(key.StoreKey(), buffer.get(), length, callback);
}

void BlockCacheUploader::PreUpload(const BlockKey& key, bool from_reload) {
  if (!from_reload) {
    stage_count_->Add(key.ino, 1);
  }
}

void BlockCacheUploader::PostUpload(const BlockKey& key, bool from_reload,
                                    bool success) {
  if (!from_reload) {
    stage_count_->Add(key.ino, -1);
  }
}

void BlockCacheUploader::UploadStageBlock(const PendingItem& item) {
  size_t length;
  std::shared_ptr<char> buffer;
  BlockKey key = item.key;
  std::string stage_path = item.stage_path;
  bool from_reload = item.from_reload;

  BCACHE_ERROR rc;
  auto timer = std::make_shared<PhaseTimer>();
  auto log_guard = std::make_shared<LogGuard>([&, timer]() {
    return StrFormat("upload_stage(%s,%d): %s%s", key.Filename(), length,
                     StrErr(rc), timer->ToString());
  });

  timer->NextPhase(Phase::READ_BLOCK);
  rc = ReadBlock(key, stage_path, buffer, &length);
  if (rc == BCACHE_ERROR::OK) {
    timer->NextPhase(Phase::S3_PUT);
    UploadBlock(key, from_reload, buffer, length, log_guard);
  } else if (rc == BCACHE_ERROR::NOT_FOUND) {
    PostUpload(key, from_reload, false);
  } else {  // retry
    timer->NextPhase(Phase::ENQUEUE_UPLOAD);
    upload_stage_thread_pool_->Enqueue(&BlockCacheUploader::UploadStageBlock,
                                       this, item);
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
