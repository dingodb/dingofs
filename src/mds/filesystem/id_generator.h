// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDS_ID_GENERATOR_H_
#define DINGOFS_MDS_ID_GENERATOR_H_

#include <cstdint>
#include <memory>
#include <string>

#include "bthread/types.h"
#include "mds/common/status.h"
#include "mds/coordinator/coordinator_client.h"
#include "mds/storage/storage.h"

namespace dingofs {
namespace mds {

class IdGenerator {
 public:
  IdGenerator() = default;
  virtual ~IdGenerator() = default;

  virtual bool Init() = 0;
  virtual bool Destroy() = 0;

  virtual bool GenID(uint32_t num, uint64_t& id) = 0;
  virtual bool GenID(uint32_t num, uint64_t min_slice_id, uint64_t& id) = 0;

  virtual std::string Describe() const = 0;
};

using IdGeneratorUPtr = std::unique_ptr<IdGenerator>;
using IdGeneratorSPtr = std::shared_ptr<IdGenerator>;

class CoorAutoIncrementIdGenerator : public IdGenerator {
 public:
  CoorAutoIncrementIdGenerator(CoordinatorClientSPtr client, const std::string& name, int64_t table_id,
                               uint64_t start_id, uint32_t batch_size);
  ~CoorAutoIncrementIdGenerator() override;

  static IdGeneratorUPtr New(CoordinatorClientSPtr client, const std::string& name, int64_t table_id, uint64_t start_id,
                             uint32_t batch_size) {
    return std::make_unique<CoorAutoIncrementIdGenerator>(client, name, table_id, start_id, batch_size);
  }
  static IdGeneratorSPtr NewShare(CoordinatorClientSPtr client, const std::string& name, int64_t table_id,
                                  uint64_t start_id, uint32_t batch_size) {
    return std::make_shared<CoorAutoIncrementIdGenerator>(client, name, table_id, start_id, batch_size);
  }

  bool Init() override;
  bool Destroy() override;

  bool GenID(uint32_t num, uint64_t& id) override;
  bool GenID(uint32_t num, uint64_t min_slice_id, uint64_t& id) override;

  std::string Describe() const override;

 private:
  Status IsExistAutoIncrement();
  Status CreateAutoIncrement();
  Status DeleteAutoIncrement();

  Status AllocateIds(uint32_t num);

  const std::string name_;

  const int64_t table_id_{0};
  const uint64_t start_id_{0};
  const uint32_t batch_size_{0};

  CoordinatorClientSPtr client_;

  bthread_mutex_t mutex_;

  // [bundle, bundle_end)
  uint64_t bundle_{0};
  uint64_t bundle_end_{0};
  uint64_t next_id_{0};

  bool is_destroyed_{false};
};

class StoreAutoIncrementIdGenerator : public IdGenerator {
 public:
  StoreAutoIncrementIdGenerator(KVStorageSPtr kv_storage, const std::string& name, int64_t start_id, int batch_size);
  ~StoreAutoIncrementIdGenerator() override;

  static IdGeneratorUPtr New(KVStorageSPtr kv_storage, const std::string& name, int64_t start_id, int batch_size) {
    return std::make_unique<StoreAutoIncrementIdGenerator>(kv_storage, name, start_id, batch_size);
  }

  static IdGeneratorSPtr NewShare(KVStorageSPtr kv_storage, const std::string& name, int64_t start_id, int batch_size) {
    return std::make_shared<StoreAutoIncrementIdGenerator>(kv_storage, name, start_id, batch_size);
  }

  bool Init() override;
  bool Destroy() override;

  bool GenID(uint32_t num, uint64_t& id) override;
  bool GenID(uint32_t num, uint64_t min_slice_id, uint64_t& id) override;

  std::string Describe() const override;

 private:
  Status GetOrPutAllocId(uint64_t& alloc_id);
  Status AllocateIds(uint32_t bundle_size);
  Status DestroyId();

  KVStorageSPtr kv_storage_;

  bthread_mutex_t mutex_;

  const std::string name_;
  // the key of id
  const std::string key_;

  // the next id can be allocated in this bunlde
  uint64_t next_id_;
  // the last id can be allocated in this bunlde
  uint64_t last_alloc_id_;
  // get the numnber of id at a time
  const uint32_t batch_size_;

  bool is_destroyed_{false};
};

IdGeneratorUPtr NewFsIdGenerator(CoordinatorClientSPtr coordinator_client, KVStorageSPtr kv_storage);
IdGeneratorUPtr NewInodeIdGenerator(uint32_t fs_id, CoordinatorClientSPtr coordinator_client, KVStorageSPtr kv_storage);
IdGeneratorSPtr NewSliceIdGenerator(CoordinatorClientSPtr coordinator_client, KVStorageSPtr kv_storage);

void DestroyInodeIdGenerator(uint32_t fs_id, CoordinatorClientSPtr coordinator_client, KVStorageSPtr kv_storage);

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_ID_GENERATOR_H_