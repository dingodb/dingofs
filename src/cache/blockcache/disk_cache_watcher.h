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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_WATCHER_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_WATCHER_H_

#include <memory>
#include "cache/blockcache/disk_cache.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {

struct Target {  // watched target
  Target(DiskCacheSPtr store, CacheStore::UploadFunc uploader)
      : store(store), uploader(uploader) {}

  std::string Id() const { return store->Id(); }
  std::string GetRootDir() const { return store->GetRootDir(); }
  std::string GetLockPath() const { return store->GetLockPath(); }
  bool IsRunning() const { return store->IsRunning(); }
  Status Shutdown() { return store->Shutdown(); }
  Status Restart(CacheStore::UploadFunc) { return store->Init(uploader); }

  CacheStore::UploadFunc uploader;
  DiskCacheSPtr store;
};

class DiskCacheWatcher {
 public:
  DiskCacheWatcher();
  virtual ~DiskCacheWatcher() = default;

  void Add(DiskCacheSPtr store, CacheStore::UploadFunc uploader);
  void Start();
  void Stop();

 private:
  enum class Should : uint8_t {
    kDoNothing = 0,
    kRestart = 1,
    kShutdown = 2,
  };

  void WatchingWorker();

  Should CheckTarget(Target* target);
  static bool CheckUuid(const std::string& lock_path, const std::string& uuid);

  static void Shutdown(Target* target);
  void Restart(Target* target);

  std::atomic<bool> running_;
  std::vector<Target> targets_;
  std::unique_ptr<Executor> executor_;
};

using DiskCacheWatcherUPtr = std::unique_ptr<DiskCacheWatcher>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_WATCHER_H_
