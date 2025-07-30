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

#include <memory>

#include "cache/blockcache/block_cache_upload_queue.h"
#include "cache/blockcache/cache_store.h"
#include "cache/storage/storage_pool.h"
#include "cache/utils/bthread.h"
#include "cache/utils/infight_throttle.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

class BlockCacheUploader;
using BlockCacheUploaderPtr = BlockCacheUploader*;
using BlockCacheUploaderUPtr = std::unique_ptr<BlockCacheUploader>;
using BlockCacheUploaderSPtr = std::shared_ptr<BlockCacheUploader>;

class BlockCacheUploader {
 public:
  BlockCacheUploader(CacheStoreSPtr store, StoragePoolSPtr storage_pool);
  virtual ~BlockCacheUploader();

  void Start();
  void Shutdown();

  void AddStagingBlock(const StagingBlock& staging_block);

 private:
  BlockCacheUploaderPtr GetSelfPtr() { return this; }

  void UploadingWorker();
  void AsyncUploading(const StagingBlock& staging_block);
  void PostUploading(const StagingBlock& staging_block, Status status);
  Status Uploading(const StagingBlock& staging_block);

  Status Load(const StagingBlock& staging_block, IOBuffer* buffer);
  Status Upload(const StagingBlock& staging_block, const IOBuffer& buffer);
  Status RemoveStage(const StagingBlock& staging_block);

  bool IsRunning();
  void WaitStoreUp();

  std::atomic<bool> running_;
  BthreadMutex mutex_;
  CacheStoreSPtr store_;
  StoragePoolSPtr storage_pool_;
  PendingQueueUPtr pending_queue_;
  InflightThrottleUPtr inflights_;
  TaskThreadPoolUPtr thread_pool_;  // uploading worker
  BthreadJoinerUPtr joiner_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_
