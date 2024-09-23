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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/blockcache/countdown.h"
#include "curvefs/src/client/blockcache/log.h"
#include "curvefs/src/client/blockcache/s3_client.h"
#include "src/common/concurrent/task_thread_pool.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curve::common::TaskThreadPool;

class BlockCacheMetric;

class BlockCacheUploader {
  struct PendingItem {
    PendingItem(const BlockKey& key, const std::string& stage_path,
                bool from_reload)
        : key(key), stage_path(stage_path), from_reload(from_reload) {}

    BlockKey key;
    std::string stage_path;
    bool from_reload;
  };

 public:
  BlockCacheUploader(std::shared_ptr<CacheStore> store,
                     std::shared_ptr<S3Client> s3,
                     std::shared_ptr<Countdown> stage_count);

  virtual ~BlockCacheUploader() = default;

  void Init(uint64_t upload_workers, uint64_t upload_queue_size);

  void Shutdown();

  void AddStageBlock(const BlockKey& key, const std::string& stage_path,
                     bool from_reload);

  size_t GetPendingSize();

 private:
  friend class BlockCacheMetric;

 private:
  void ScanStageBlock();

  void UploadStageBlock(const PendingItem& item);

  BCACHE_ERROR ReadBlock(const BlockKey& key, const std::string& stage_path,
                         std::shared_ptr<char>& buffer, size_t* length);

  void UploadBlock(const BlockKey& key, bool from_reload,
                   std::shared_ptr<char> buffer, size_t length,
                   std::shared_ptr<LogGuard> log_guard);

  void PreUpload(const BlockKey& key, bool from_reload);

  void PostUpload(const BlockKey& key, bool from_reload, bool success);

 private:
  std::mutex mutex_;
  std::atomic<bool> running_;
  std::vector<PendingItem> slow_queue_;
  std::vector<PendingItem> fast_queue_;
  std::shared_ptr<CacheStore> store_;
  std::shared_ptr<S3Client> s3_;
  std::shared_ptr<Countdown> stage_count_;
  std::unique_ptr<TaskThreadPool<>> scan_stage_thread_pool_;
  std::shared_ptr<TaskThreadPool<>> upload_stage_thread_pool_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_
