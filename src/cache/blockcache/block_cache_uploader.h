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

#include <bthread/mutex.h>

#include <memory>
#include <ostream>

#include "cache/blockcache/cache_store.h"
#include "cache/common/storage_client_pool.h"
#include "cache/iutil/bthread.h"
#include "cache/iutil/inflight_tracker.h"

namespace dingofs {
namespace cache {

struct StageBlock {
  StageBlock(ContextSPtr ctx, const BlockKey& key, size_t length,
             BlockAttr block_attr)
      : ctx(ctx), key(key), length(length), block_attr(block_attr) {}

  ContextSPtr ctx;
  BlockKey key;
  size_t length;
  BlockAttr block_attr;
};

struct StageBlockStat {
  StageBlockStat(uint64_t num_total, uint64_t num_from_writeback,
                 uint64_t num_from_reload)
      : num_total(num_total),
        num_from_writeback(num_from_writeback),
        num_from_reload(num_from_reload) {}

  uint64_t num_total;
  uint64_t num_from_writeback;
  uint64_t num_from_reload;
};

class PendingQueue;
using PendingQueueUPtr = std::unique_ptr<PendingQueue>;

class BlockCacheUploader {
 public:
  BlockCacheUploader(CacheStoreSPtr store,
                     StorageClientPoolSPtr storage_client_pool);
  ~BlockCacheUploader();
  void Start();
  void Shutdown();

  void EnterUploadQueue(const StageBlock& sblock);

 private:
  BlockCacheUploader* GetSelfPtr() { return this; }
  bool IsRunning() { return running_.load(std::memory_order_relaxed); }

  void WaitStoreReady() {
    while (!store_->IsRunning()) {
      bthread_usleep(1000);  // 1 second
    }
  }

  void UploadWorker();
  void AsyncUpload(const StageBlock& sblock);
  Status DoUpload(const StageBlock& sblock);
  void OnComplete(const StageBlock& sblock, Status status);

  std::atomic<bool> running_;
  bthread::Mutex mutex_;
  CacheStoreSPtr store_;
  StorageClientPoolSPtr storage_client_pool_;
  PendingQueueUPtr pending_queue_;
  iutil::InflightTrackerUPtr tracker_;
  std::thread thread_;
  iutil::BthreadJoinerUPtr joiner_;
};

using BlockCacheUploaderUPtr = std::unique_ptr<BlockCacheUploader>;
using BlockCacheUploaderSPtr = std::shared_ptr<BlockCacheUploader>;

std::ostream& operator<<(std::ostream& os, const StageBlock& sblock);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_
