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

#ifndef DINGOFS_SRC_CACHE_STORAGE_STORAGE_H_
#define DINGOFS_SRC_CACHE_STORAGE_STORAGE_H_

#include <bthread/execution_queue.h>

#include <cstdint>
#include <memory>
#include <string>

#include "cache/common/common.h"
#include "cache/storage/buffer.h"
#include "cache/storage/closure.h"
#include "common/status.h"
#include "dataaccess/accesser.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace cache {
namespace storage {

// Why use bthread::ExecutionQueue?
//  bthread -> Storage::Put(...) -> DataAccesser::AsyncGet(...)
//  maybe there is pthread synchronization semantics in function AsyncGet.
class Storage {
 public:
  virtual ~Storage() = default;

  virtual Status Init() = 0;
  virtual Status Shutdown() = 0;

  virtual Status Put(const std::string& key, IOBuffer* buffer) = 0;
  virtual Status Get(const std::string& key, off_t offset, size_t length,
                     IOBuffer* buffer) = 0;
};

class StorageImpl : public Storage {
  class StorageClosure;

 public:
  explicit StorageImpl(dataaccess::DataAccesserPtr data_accesser);

  Status Init() override;
  Status Shutdown() override;

  Status Put(const std::string& key, IOBuffer* buffer) override;
  Status Get(const std::string& key, off_t offset, size_t length,
             IOBuffer* buffer) override;

 private:
  static int Executor(void* meta, bthread::TaskIterator<StorageClosure*>& iter);

  void Put(StorageClosure* closure);
  void Get(StorageClosure* closure);

 private:
  std::atomic<bool> running_;
  dataaccess::DataAccesserPtr data_accesser_;
  std::unique_ptr<dingofs::utils::TaskThreadPool<>> task_thread_pool_;
  bthread::ExecutionQueueId<StorageClosure*> submit_queue_id_;
};

}  // namespace storage
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_STORAGE_H_
