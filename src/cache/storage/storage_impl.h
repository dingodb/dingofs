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
 * Created Date: 2025-05-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_STORAGE_STORAGE_IMPL_H_
#define DINGOFS_SRC_CACHE_STORAGE_STORAGE_IMPL_H_

#include <bthread/execution_queue.h>
#include <bvar/passive_status.h>
#include <bvar/reducer.h>

#include <cstdint>

#include "blockaccess/block_accesser.h"
#include "cache/blockcache/cache_store.h"
#include "cache/storage/storage.h"
#include "cache/storage/storage_closure.h"
#include "cache/utils/execution_queue.h"

namespace dingofs {
namespace cache {

// Why use bthread::ExecutionQueue?
//  bthread -> Put(...) -> BlockAccesser::AsyncGet(...)
//  maybe there is pthread synchronization semantics in function
//  BlockAccesser::AsyncGet.
class StorageImpl final : public Storage {
 public:
  explicit StorageImpl(blockaccess::BlockAccesser* block_accesser);

  Status Start() override;
  Status Shutdown() override;

  Status Upload(ContextSPtr ctx, const BlockKey& key, const Block& block,
                UploadOption option = UploadOption()) override;
  Status Download(ContextSPtr ctx, const BlockKey& key, off_t offset,
                  size_t length, IOBuffer* buffer,
                  DownloadOption option = DownloadOption()) override;

 private:
  static int HandleClosure(void* meta,
                           bthread::TaskIterator<StorageClosure*>& iter);

  std::atomic<bool> running_;
  blockaccess::BlockAccesser* block_accesser_;
  bthread::ExecutionQueueId<StorageClosure*> queue_id_;
  ExecutionQueueSPtr upload_retry_queue_;
  ExecutionQueueSPtr download_retry_queue_;
  bvar::PassiveStatus<int64_t> metric_upload_retry_count_;
  bvar::PassiveStatus<int64_t> metric_download_retry_count_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_STORAGE_IMPL_H_
