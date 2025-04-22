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
#include <string>
#include <vector>

#include "cache/blockcache/disk_cache.h"
#include "cache/blockcache/disk_cache_loader.h"
#include "cache/blockcache/disk_state_machine_impl.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace cache {
namespace blockcache {

class DiskCacheWatcher {
  enum class CacheStatus : uint8_t {
    kUP = 0,
    kDOWN = 1,
  };

  struct WatchStore {
    std::string root_dir;
    std::shared_ptr<DiskCache> store;
    CacheStatus status;
  };

 public:
  DiskCacheWatcher();

  virtual ~DiskCacheWatcher() = default;

  void Add(const std::string& root_dir, std::shared_ptr<DiskCache> store);

  void Start(UploadFunc uploader);

  void Stop();

 private:
  void WatchingWorker();

  void CheckLockFile(WatchStore* watch_store);

  static bool CheckUuId(const std::string& lock_path, const std::string& uuid);

  static void Shutdown(WatchStore* watch_store);

  void Restart(WatchStore* watch_store);

 private:
  UploadFunc uploader_;
  std::atomic<bool> running_;
  std::vector<WatchStore> watch_stores_;
  std::unique_ptr<TaskThreadPool<>> task_pool_;
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_WATCHER_H_
