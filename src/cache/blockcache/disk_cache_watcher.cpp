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

#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {

DiskCacheWatcher::DiskCacheWatcher()
    : running_(false), timer_(std::make_unique<TimerImpl>()) {}

void DiskCacheWatcher::Add(DiskCacheSPtr store,
                           CacheStore::UploadFunc uploader) {
  targets_.emplace_back(Target(store, uploader));
}

void DiskCacheWatcher::Start() {
  if (!running_.exchange(true, std::memory_order_acq_rel)) {
    LOG(INFO) << "Disk cache watcher starting...";

    CHECK(timer_->Start());
    timer_->Add([this] { WatchingWorker(); }, 100);

    LOG(INFO) << "Disk cache watcher started.";
  }
}

void DiskCacheWatcher::Stop() {
  if (running_.exchange(false, std::memory_order_acq_rel)) {
    LOG(INFO) << "Disk cache watcher stopping...";

    timer_->Stop();

    LOG(INFO) << "Disk cache watcher stopped.";
  }
}

void DiskCacheWatcher::WatchingWorker() {
  if (running_.load(std::memory_order_acquire)) {
    for (auto& target : targets_) {
      auto should = CheckTarget(&target);
      if (should == Should::kShutdown) {
        Shutdown(&target);
      } else if (should == Should::kRestart) {
        Restart(&target);
      }
    }

    timer_->Add([this] { WatchingWorker(); }, 100);
  }
}

DiskCacheWatcher::Should DiskCacheWatcher::CheckTarget(Target* target) {
  std::string lock_path = target->GetLockPath();

  if (!Helper::FileExists(lock_path)) {  // cache is down
    return Should::kShutdown;
  } else if (target->IsRunning()) {  // cache already up
    return Should::kDoNothing;
  } else if (CheckUuid(lock_path, target->Id())) {  // recover to up
    return Should::kRestart;
  }
  return Should::kDoNothing;
}

bool DiskCacheWatcher::CheckUuid(const std::string& lock_path,
                                 const std::string& uuid) {
  std::string content;
  auto status = Helper::ReadFile(lock_path, &content);
  if (!status.ok()) {
    LOG(ERROR) << "Read lock file (" << lock_path
               << ") failed: " << status.ToString();
    return false;
  }

  content = base::string::TrimSpace(content);
  if (uuid != content) {
    LOG(ERROR) << "Disk cache uuid mismatch: " << uuid << " != " << content;
    return false;
  }
  return true;
}

void DiskCacheWatcher::Shutdown(Target* target) {
  if (target->IsRunning()) {  // running
    auto root_dir = target->GetRootDir();
    auto status = target->Shutdown();
    if (status.ok()) {
      LOG(INFO) << "Shutdown disk cache (dir=" << root_dir
                << ") success for disk maybe broken.";
    } else {
      LOG(ERROR) << "Try to shutdown cache store (dir=" << root_dir
                 << ") failed: " << status.ToString();
    }
  }
}

void DiskCacheWatcher::Restart(Target* target) {
  if (!target->IsRunning()) {
    auto root_dir = target->GetRootDir();
    auto status = target->Restart(target->uploader);
    if (status.ok()) {
      LOG(INFO) << "Restart disk cache (dir=" << root_dir << ") success.";
    } else {
      LOG(ERROR) << "Try to restart cache store (dir=" << root_dir
                 << ") failed: " << status.ToString();
    }
  }
}

}  // namespace cache
}  // namespace dingofs
