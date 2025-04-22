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
 * Created Date: 2024-09-24
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/disk_cache_watcher.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <ostream>
#include <thread>

#include "base/string/string.h"
#include "cache/blockcache/disk_cache.h"
#include "cache/blockcache/disk_cache_group.h"
#include "cache/common/common.h"
#include "cache/common/local_filesystem.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using base::string::TrimSpace;

DiskCacheWatcher::DiskCacheWatcher()
    : running_(false),
      task_pool_(std::make_unique<TaskThreadPool<>>("disk_cache_watcher")) {}

void DiskCacheWatcher::Add(const std::string& root_dir,
                           std::shared_ptr<DiskCache> store) {
  watch_stores_.emplace_back(WatchStore{root_dir, store, CacheStatus::kUP});
}

void DiskCacheWatcher::Start(UploadFunc uploader) {
  if (!running_.exchange(true)) {
    uploader_ = uploader;
    CHECK(task_pool_->Start(1) == 0);
    task_pool_->Enqueue(&DiskCacheWatcher::WatchingWorker, this);
  }
}

void DiskCacheWatcher::Stop() {
  if (running_.exchange(false)) {
    task_pool_->Stop();
  }
}

void DiskCacheWatcher::WatchingWorker() {
  while (running_.load(std::memory_order_relaxed)) {
    for (auto& watch_store : watch_stores_) {
      CheckLockFile(&watch_store);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void DiskCacheWatcher::CheckLockFile(WatchStore* watch_store) {
  auto fs = LocalFileSystem();
  auto root_dir = watch_store->root_dir;
  auto layout = DiskCacheLayout(root_dir);
  std::string lock_path = layout.GetLockPath();
  auto store = watch_store->store;

  if (!fs.FileExists(lock_path)) {  // cache is down
    LOG(INFO) << "Lock file (" << lock_path
              << ") not found, we will shutdown the specified store.";
    Shutdown(watch_store);
  } else if (watch_store->status == CacheStatus::kUP) {  // cache already up
    // do nothing
  } else if (CheckUuId(lock_path, store->Id())) {  // recover to up
    Restart(watch_store);
  }
}

bool DiskCacheWatcher::CheckUuId(const std::string& lock_path,
                                 const std::string& uuid) {
  size_t length;
  std::shared_ptr<char> buffer;
  auto fs = LocalFileSystem();
  auto status = fs.ReadFile(lock_path, buffer, &length);
  if (!status.ok()) {
    LOG(ERROR) << "Read lock file (" << lock_path
               << ") failed: " << status.ToString();
    return false;
  }

  auto content = TrimSpace(std::string(buffer.get(), length));
  if (uuid != content) {
    LOG(ERROR) << "Disk cache uuid mismatch: " << uuid << " != " << content;
    return false;
  }
  return true;
}

void DiskCacheWatcher::Shutdown(WatchStore* watch_store) {
  if (watch_store->status == CacheStatus::kDOWN) {
    return;
  }

  auto root_dir = watch_store->root_dir;
  auto status = watch_store->store->Shutdown();
  if (status.ok()) {
    LOG(INFO) << "Shutdown disk cache (dir=" << root_dir
              << ") success for disk maybe broken.";
  } else {
    LOG(ERROR) << "Try to shutdown cache store (" << root_dir
               << ") failed: " << status.ToString();
  }
  watch_store->status = CacheStatus::kDOWN;
}

void DiskCacheWatcher::Restart(WatchStore* watch_store) {
  auto root_dir = watch_store->root_dir;
  auto status = watch_store->store->Init(uploader_);
  if (status.ok()) {
    watch_store->status = CacheStatus::kUP;
    LOG(INFO) << "Restart disk cache (dir=" << root_dir << ") success.";
  } else {
    LOG(ERROR) << "Try to restart cache store (" << root_dir
               << ") failed: " << status.ToString();
  }
}

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs
