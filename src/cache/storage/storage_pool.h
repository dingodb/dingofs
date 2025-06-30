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

#ifndef DINGOFS_SRC_CACHE_STORAGE_STORAGE_POOL_H_
#define DINGOFS_SRC_CACHE_STORAGE_STORAGE_POOL_H_

#include "blockaccess/block_accesser.h"
#include "cache/common/type.h"
#include "cache/storage/storage.h"
#include "common/status.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {

class StoragePool {
 public:
  virtual ~StoragePool() = default;

  virtual Status GetStorage(uint32_t fs_id, StorageSPtr& storage) = 0;
};

using StoragePoolSPtr = std::shared_ptr<StoragePool>;
using StoragePoolUPtr = std::unique_ptr<StoragePool>;

class SingleStorage final : public StoragePool {
 public:
  explicit SingleStorage(StorageSPtr storage);

  Status GetStorage(uint32_t /*fs_id*/, StorageSPtr& storage) override;

 private:
  StorageSPtr storage_;
};

class StoragePoolImpl final : public StoragePool {
 public:
  explicit StoragePoolImpl(
      std::shared_ptr<stub::rpcclient::MdsClient> mds_client);

  Status GetStorage(uint32_t fs_id, StorageSPtr& storage) override;

 private:
  bool Get(uint32_t fs_id, StorageSPtr& storage);
  Status Create(uint32_t fs_id, StorageSPtr& storage);
  void Insert(uint32_t fs_id, StorageSPtr storage);

  BthreadMutex mutex_;
  std::shared_ptr<stub::rpcclient::MdsClient> mds_client_;
  std::unordered_map<uint32_t, blockaccess::BlockAccesserUPtr>
      block_accesseres_;
  std::unordered_map<uint32_t, StorageSPtr> storages_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_STORAGE_POOL_H_
