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

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_

#include <bthread/mutex.h>

#include <atomic>
#include <memory>

#include "cache/blockcache/block_cache_upload_queue.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/countdown.h"
#include "cache/utils/phase_timer.h"
#include "dataaccess/accesser.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace cache {
namespace blockcache {

// How it works:
//               add                   scan                     put
// [stage block]----> [pending queue] -----> [uploading queue] ----> [s3]
class BlockCacheUploader {
 public:
  BlockCacheUploader(dataaccess::DataAccesserPtr data_accesser,
                     std::shared_ptr<CacheStore> store,
                     std::shared_ptr<Countdown> stage_count);
  virtual ~BlockCacheUploader();

  void Init(uint64_t upload_workers, uint64_t upload_queue_size);
  void Shutdown();

  void AddStageBlock(const BlockKey& key, const std::string& stage_path,
                     BlockContext ctx);
  void WaitAllUploaded();

 private:
  friend class BlockCacheMetricHelper;

  bool CanUpload(const std::vector<StageBlock>& blocks);

  void ScaningWorker();
  void UploadingWorker();

  void UploadStageBlock(const StageBlock& stage_block);
  static Status ReadBlock(const StageBlock& stage_block,
                          std::shared_ptr<char>& buffer, size_t* length);
  void UploadBlock(const StageBlock& stage_block, std::shared_ptr<char> buffer,
                   size_t length, utils::PhaseTimer timer);
  void RemoveBlock(const StageBlock& stage_block);

  void Staging(const StageBlock& stage_block);
  void Uploaded(const StageBlock& stage_block, bool success);
  static bool NeedCount(const StageBlock& stage_block);

 private:
  bthread::Mutex mutex_;
  std::atomic<bool> running_;
  dataaccess::DataAccesserPtr data_accesser_;
  std::shared_ptr<CacheStore> store_;
  std::shared_ptr<Countdown> stage_count_;
  std::shared_ptr<PendingQueue> pending_queue_;
  std::shared_ptr<UploadingQueue> uploading_queue_;
  std::unique_ptr<TaskThreadPool<>> scan_stage_thread_pool_;
  std::unique_ptr<TaskThreadPool<>> upload_stage_thread_pool_;
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_
