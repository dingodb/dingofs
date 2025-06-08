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

#include "blockaccess/block_accesser.h"
#include "cache/storage/storage.h"
#include "cache/storage/storage_operator.h"

namespace dingofs {
namespace cache {

// Why use bthread::ExecutionQueue?
//  bthread -> Put(...) -> BlockAccesser::AsyncGet(...)
//  maybe there is pthread synchronization semantics in function
//  BlockAccesser::AsyncGet.
class StorageImpl : public Storage {
 public:
  explicit StorageImpl(blockaccess::BlockAccesserSPtr block_accesser);

  Status Init() override;
  Status Shutdown() override;

  Status Put(const std::string& key, const IOBuffer& buffer) override;
  Status Range(const std::string& key, off_t offset, size_t length,
               IOBuffer* buffer) override;

 private:
  static int Executor(void* meta, bthread::TaskIterator<StorageClosure*>& iter);
  void Execute(StorageClosure* closure);
  StorageOperator* NewOperator(StorageClosure* closure);

  std::atomic<bool> running_;
  blockaccess::BlockAccesserSPtr block_accesser_;
  TaskThreadPoolUPtr task_thread_pool_;
  bthread::ExecutionQueueId<StorageClosure*> submit_queue_id_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_STORAGE_IMPL_H_
