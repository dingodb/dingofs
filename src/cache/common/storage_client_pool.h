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
 * Created Date: 2025-03-17
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_STORAGE_CLIENT_POOL_H_
#define DINGOFS_SRC_CACHE_COMMON_STORAGE_CLIENT_POOL_H_

#include <bthread/rwlock.h>

#include "cache/common/mds_client.h"
#include "cache/common/storage_client.h"
#include "common/blockaccess/block_accesser.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class StorageClientPool {
 public:
  virtual ~StorageClientPool() = default;

  virtual Status GetStorageClient(uint32_t fs_id, StorageClient** storage) = 0;
};

using StorageClientPoolSPtr = std::shared_ptr<StorageClientPool>;
using StorageClientPoolUPtr = std::unique_ptr<StorageClientPool>;

class SingletonStorageClient final : public StorageClientPool {
 public:
  explicit SingletonStorageClient(StorageClient* storage_client)
      : storage_client_(storage_client) {}

  Status GetStorageClient(uint32_t /*fs_id*/,
                          StorageClient** storage_client) override {
    *storage_client = storage_client_;
    return Status::OK();
  }

 private:
  StorageClient* storage_client_;
};

class StorageClientPoolImpl final : public StorageClientPool {
 public:
  explicit StorageClientPoolImpl(MDSClientSPtr mds_client);

  Status GetStorageClient(uint32_t fs_id,
                          StorageClient** storage_client) override;

 private:
  Status CreateStorageClient(uint32_t fs_id, StorageClient** storage_client);

  bthread::RWLock rwlock_;
  MDSClientSPtr mds_client_;
  std::unordered_map<uint32_t, blockaccess::BlockAccesserUPtr> accesseres_;
  std::unordered_map<uint32_t, StorageClientUPtr> clients_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_STORAGE_CLIENT_POOL_H_
