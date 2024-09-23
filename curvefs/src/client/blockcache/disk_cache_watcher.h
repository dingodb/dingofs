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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_WATCHER_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_WATCHER_H_

#include <memory>
#include <string>
#include <vector>

#include "curvefs/src/client/blockcache/disk_cache.h"
#include "curvefs/src/client/blockcache/disk_cache_loader.h"
#include "curvefs/src/client/blockcache/disk_state_machine_impl.h"
#include "src/common/concurrent/task_thread_pool.h"

namespace curvefs {
namespace client {
namespace blockcache {

class DiskCacheWatcher {
  enum class CacheStatus {
    UP,
    DOWN,
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
  void CheckLockFile();

  void CheckOne(WatchStore* watch_store);

  bool CheckUuId(const std::string& lock_path, const std::string& uuid);

  void Shutdown(WatchStore* watch_store);

  void Restart(WatchStore* watch_store);

 private:
  UploadFunc uploader_;
  std::atomic<bool> running_;
  std::vector<WatchStore> watch_stores_;
  std::unique_ptr<TaskThreadPool<>> task_pool_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_WATCHER_H_
