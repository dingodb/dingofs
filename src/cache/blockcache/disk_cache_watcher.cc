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

#include "cache/common/macro.h"
#include "cache/iutil/file_util.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DiskCacheWatcher::DiskCacheWatcher()
    : running_(false), executor_(std::make_unique<BthreadExecutor>()) {}

void DiskCacheWatcher::Add(DiskCacheSPtr store,
                           CacheStore::UploadFunc uploader) {
  CHECK(!running_) << "MUST add targets before watcher started.";

  targets_.emplace_back(Target(store, uploader));
}

void DiskCacheWatcher::Start() {
  CHECK(!targets_.empty());
  CHECK_NOTNULL(executor_);

  if (running_) {
    return;
  }

  LOG(INFO) << "Disk cache watcher is starting...";

  CHECK(executor_->Start());
  executor_->Schedule([this] { WatchingWorker(); }, 100);

  running_ = true;

  LOG(INFO) << "Disk cache watcher is up.";

  CHECK_RUNNING("Disk cache watcher");
}

void DiskCacheWatcher::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  LOG(INFO) << "Disk cache watcher is shutting down...";

  executor_->Stop();

  LOG(INFO) << "Disk cache watcher is down.";
}

void DiskCacheWatcher::WatchingWorker() {
  for (auto& target : targets_) {
    auto should = CheckTarget(&target);
    if (should == Should::kShutdown) {
      Shutdown(&target);
    } else if (should == Should::kRestart) {
      Restart(&target);
    }
  }

  executor_->Schedule([this] { WatchingWorker(); }, 100);
}

DiskCacheWatcher::Should DiskCacheWatcher::CheckTarget(Target* target) {
  std::string lock_path = target->GetLockPath();

  if (!iutil::FileIsExist(lock_path)) {  // cache is down
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
  auto status = iutil::ReadFile(lock_path, &content);
  if (!status.ok()) {
    LOG(ERROR) << "Read lock file failed: path = " << lock_path
               << ", status = " << status.ToString();
    return false;
  }

  content = utils::TrimSpace(content);
  if (uuid != content) {
    LOG(ERROR) << "Disk cache uuid mismatch, please rewrite it: got(" << content
               << ") != expect(" << uuid << ").";
    return false;
  }
  return true;
}

void DiskCacheWatcher::Shutdown(Target* target) {
  if (!target->IsRunning()) {
    LOG(INFO) << "Disk cache (dir=" << target->GetRootDir()
              << ") is already shutdown, no need to shutdown again.";
    return;
  }

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

void DiskCacheWatcher::Restart(Target* target) {
  if (target->IsRunning()) {  // Already running
    LOG(INFO) << "Disk cache (dir=" << target->GetRootDir()
              << ") is already running, no need to restart again.";
    return;
  }

  auto root_dir = target->GetRootDir();
  auto status = target->Restart(target->uploader);
  if (status.ok()) {
    LOG(INFO) << "Restart disk cache (dir=" << root_dir << ") success.";
  } else {
    LOG(ERROR) << "Try to restart cache store (dir=" << root_dir
               << ") failed: " << status.ToString();
  }
}

}  // namespace cache
}  // namespace dingofs
