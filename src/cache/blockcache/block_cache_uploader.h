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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_

#include "cache/blockcache/block_cache_throttle.h"
#include "cache/blockcache/block_cache_upload_queue.h"
#include "cache/blockcache/cache_store.h"
#include "cache/storage/storage_pool.h"
#include "cache/utils/infight_throttle.h"

namespace dingofs {
namespace cache {

class BlockCacheUploader;
using BlockCacheUploaderSPtr = std::shared_ptr<BlockCacheUploader>;

class BlockCacheUploader
    : public std::enable_shared_from_this<BlockCacheUploader> {
 public:
  BlockCacheUploader(StoragePoolSPtr storage_pool, CacheStoreSPtr store);
  virtual ~BlockCacheUploader();

  void Init();
  void Shutdown();

  void AddStageBlock(const StageBlock& block);
  void WaitAllUploaded();

 private:
  void UploadingWorker();
  void UploadBlock(const StageBlock& block);
  Status DoUpload(const StageBlock& block);

  Status StoragePut(const BlockKey& key, const IOBuffer& buffer);

  BlockCacheUploaderSPtr GetSelfSPtr() { return shared_from_this(); }

  std::atomic<bool> running_;
  BthreadMutex mutex_;
  StoragePoolSPtr storage_pool_;
  CacheStoreSPtr store_;
  PendingQueueUPtr pending_queue_;
  TaskThreadPoolUPtr upload_stage_thread_pool_;
  InflightThrottleUPtr inflights_throttle_;
  BthreadCountdownEvent uploading_count_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_
