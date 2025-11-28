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

#include "mds/filesystem/id_generator.h"

#include <gflags/gflags_declare.h>

#include <algorithm>
#include <cstdint>
#include <string>

#include "bthread/mutex.h"
#include "dingofs/error.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/codec.h"
#include "mds/common/logging.h"
#include "mds/common/status.h"
#include "mds/common/time.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_txn_max_retry_times);

const std::string kFsAutoIncrementIdName = "dingofs-fs-id";
static const int64_t kFsTableId = 1000;
static const int64_t kFsIdBatchSize = 2;
static const int64_t kFsIdStartId = 1e4;  // 10 thousand

// all file systems share the same slice id generator
const std::string kSliceAutoIncrementIdName = "dingofs-slice-id";
static const int64_t kSliceTableId = 1002;
static const int64_t kSliceIdBatchSize = 32;
static const int64_t kSliceIdStartId = 1e10;  // 10 billion

// each file system has its own inode id generator
const std::string kInoAutoIncrementIdName = "dingofs-inode-id";
static const int64_t kInoBatchSize = 64;
static const int64_t kInoStartId = 2e10;  // 20 billion

CoorAutoIncrementIdGenerator::CoorAutoIncrementIdGenerator(CoordinatorClientSPtr client, const std::string& name,
                                                           int64_t table_id, uint64_t start_id, uint32_t batch_size)
    : client_(client), name_(name), table_id_(table_id), start_id_(start_id), batch_size_(batch_size) {
  next_id_ = start_id;
  bundle_ = start_id;
  bundle_end_ = start_id;

  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

CoorAutoIncrementIdGenerator::~CoorAutoIncrementIdGenerator() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destory mutex fail.";
}

bool CoorAutoIncrementIdGenerator::Init() {
  auto status = IsExistAutoIncrement();
  if (status.ok()) {
    DINGO_LOG(INFO) << fmt::format("[idalloc.{}] autoincrement table exist.", name_);
    return true;
  }

  if (status.error_code() != pb::error::ENOT_FOUND) {
    DINGO_LOG(ERROR) << fmt::format("[idalloc.{}] check autoincrement table fail, status({}).", name_,
                                    status.error_str());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("[idalloc.{}] autoincrement table not exist.", name_);

  status = CreateAutoIncrement();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[idalloc.{}] create autoincrement table fail, error({}).", name_,
                                    status.error_str());
    return false;
  }

  return true;
}

bool CoorAutoIncrementIdGenerator::Destroy() {
  BAIDU_SCOPED_LOCK(mutex_);

  auto status = DeleteAutoIncrement();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[idalloc.{}] destroy autoincrement table fail, status({}).", name_,
                                    status.error_str());
    return false;
  }

  is_destroyed_ = true;

  return true;
}

bool CoorAutoIncrementIdGenerator::GenID(uint32_t num, uint64_t& id) { return GenID(num, 0, id); }

bool CoorAutoIncrementIdGenerator::GenID(uint32_t num, uint64_t min_slice_id, uint64_t& id) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (is_destroyed_) {
    DINGO_LOG(ERROR) << fmt::format("[idalloc.{}] id generator is destroyed.", name_);
    return false;
  }

  next_id_ = std::max(next_id_, min_slice_id);

  if (next_id_ + num > bundle_end_) {
    auto status = AllocateIds(std::max(num, batch_size_));
    if (!status.ok()) {
      return false;
    }
  }

  id = next_id_;
  next_id_ += num;

  DINGO_LOG(INFO) << fmt::format("[idalloc.{}] alloc id {},{} bundle[{}, {}).", name_, id, num, bundle_, bundle_end_);

  return true;
}

std::string CoorAutoIncrementIdGenerator::Describe() const {
  return fmt::format("[coordinator] name({}) start_id({}) batch_size({}) range[{}, {}) next_id({})", name_, start_id_,
                     batch_size_, bundle_, bundle_end_, next_id_);
}

Status CoorAutoIncrementIdGenerator::IsExistAutoIncrement() {
  int64_t start_id = -1;
  return client_->GetAutoIncrement(table_id_, start_id);
}

Status CoorAutoIncrementIdGenerator::CreateAutoIncrement() {
  DINGO_LOG(INFO) << fmt::format("[idalloc.{}] create autoincrement table, start_id({}).", name_, start_id_);
  return client_->CreateAutoIncrement(table_id_, start_id_);
}

Status CoorAutoIncrementIdGenerator::DeleteAutoIncrement() {
  DINGO_LOG(INFO) << fmt::format("[idalloc.{}] delete autoincrement table, start_id({}).", name_, start_id_);
  return client_->DeleteAutoIncrement(table_id_);
}

Status CoorAutoIncrementIdGenerator::AllocateIds(uint32_t num) {
  Status status;
  Duration duration;
  int64_t bundle = 0;
  int64_t bundle_end = 0;
  do {
    status = client_->GenerateAutoIncrement(table_id_, num, bundle, bundle_end);
    if (!status.ok()) {
      break;
    }

    CHECK(bundle >= 0 && bundle_end >= 0) << "bundle id is negative.";
  } while (static_cast<uint64_t>(bundle) < next_id_);

  if (status.ok()) {
    bundle_ = static_cast<uint64_t>(bundle);
    next_id_ = static_cast<uint64_t>(bundle);
    bundle_end_ = static_cast<uint64_t>(bundle_end);
  }

  DINGO_LOG(INFO) << fmt::format("[idalloc.{}][{}us] take bundle id, bundle[{},{}) num({}) status({}).", name_,
                                 duration.ElapsedUs(), bundle_, bundle_end_, num, status.error_str());

  return status;
}

StoreAutoIncrementIdGenerator::StoreAutoIncrementIdGenerator(KVStorageSPtr kv_storage, const std::string& name,
                                                             int64_t start_id, int batch_size)
    : kv_storage_(kv_storage),
      name_(name),
      key_(MetaCodec::EncodeAutoIncrementIDKey(name)),
      next_id_(start_id),
      last_alloc_id_(start_id),
      batch_size_(batch_size) {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

StoreAutoIncrementIdGenerator::~StoreAutoIncrementIdGenerator() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destory mutex fail.";
}

bool StoreAutoIncrementIdGenerator::Init() {
  uint64_t alloc_id = 0;
  auto status = GetOrPutAllocId(alloc_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[idalloc.{}] init get alloc id fail, status({}).", name_, status.error_cstr());
    return false;
  }

  next_id_ = alloc_id;
  last_alloc_id_ = alloc_id;

  return true;
}

bool StoreAutoIncrementIdGenerator::Destroy() {
  BAIDU_SCOPED_LOCK(mutex_);

  auto status = DestroyId();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[idalloc.{}] destroy autoincrement table fail, status({}).", name_,
                                    status.error_str());
    return false;
  }

  return true;
}

bool StoreAutoIncrementIdGenerator::GenID(uint32_t num, uint64_t& id) { return GenID(num, 0, id); }

bool StoreAutoIncrementIdGenerator::GenID(uint32_t num, uint64_t min_slice_id, uint64_t& id) {
  if (num == 0) {
    DINGO_LOG(ERROR) << fmt::format("[idalloc.{}] num cant not 0.", name_);
    return -1;
  }

  BAIDU_SCOPED_LOCK(mutex_);

  next_id_ = std::max(next_id_, min_slice_id);

  if (next_id_ + num > last_alloc_id_) {
    auto status = AllocateIds(std::max(num, batch_size_));
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[idalloc.{}] allocate id fail, {}.", name_, status.error_str());
      return -1;
    }
  }

  // allocate id
  id = next_id_;
  next_id_ += num;

  DINGO_LOG(INFO) << fmt::format("[idalloc.{}] alloc id({}) num({}).", name_, id, num);

  return true;
}

std::string StoreAutoIncrementIdGenerator::Describe() const {
  return fmt::format("[store] name({}) batch_size({}) last_alloc_id({}) next_id({})", name_, batch_size_,
                     last_alloc_id_, next_id_);
}

Status StoreAutoIncrementIdGenerator::GetOrPutAllocId(uint64_t& alloc_id) {
  Status status;
  uint32_t retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }

    std::string value;
    status = txn->Get(key_, value);
    if (!status.ok()) {
      if (status.error_code() != pb::error::ENOT_FOUND) {
        break;
      }
      alloc_id = 0;

    } else {
      MetaCodec::DecodeAutoIncrementIDValue(value, alloc_id);
    }

    if (alloc_id < last_alloc_id_) {
      alloc_id = last_alloc_id_;
      txn->Put(key_, MetaCodec::EncodeAutoIncrementIDValue(alloc_id));
    }

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

  } while (++retry < FLAGS_mds_txn_max_retry_times);

  return status;
}

Status StoreAutoIncrementIdGenerator::AllocateIds(uint32_t size) {
  Duration duration;
  Status status;
  uint32_t retry = 0;
  uint64_t start_alloc_id = std::max(next_id_, last_alloc_id_);
  do {
    auto txn = kv_storage_->NewTxn();
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }

    uint64_t alloced_id = 0;
    std::string value;
    status = txn->Get(key_, value);
    if (!status.ok()) {
      if (status.error_code() != pb::error::ENOT_FOUND) {
        break;
      }

    } else {
      MetaCodec::DecodeAutoIncrementIDValue(value, alloced_id);
    }

    start_alloc_id = std::max(alloced_id, start_alloc_id);
    txn->Put(key_, MetaCodec::EncodeAutoIncrementIDValue(start_alloc_id + size));

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

  } while (++retry < FLAGS_mds_txn_max_retry_times);

  if (status.ok()) {
    last_alloc_id_ = start_alloc_id + size;
    next_id_ = start_alloc_id;
  }

  DINGO_LOG(INFO) << fmt::format("[idalloc.{}][{}us] take bundle id, bundle[{},{}) size({}) status({}).", name_,
                                 duration.ElapsedUs(), next_id_, last_alloc_id_, size, status.error_str());

  return status;
}

Status StoreAutoIncrementIdGenerator::DestroyId() {
  Status status;
  uint32_t retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }

    txn->Delete(key_);

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

  } while (++retry < FLAGS_mds_txn_max_retry_times);

  return status;
}

IdGeneratorUPtr NewFsIdGenerator(CoordinatorClientSPtr coordinator_client) {
  CHECK(coordinator_client != nullptr) << "coordinator_client is nullptr.";

  return CoorAutoIncrementIdGenerator::New(coordinator_client, kFsAutoIncrementIdName, kFsTableId, kFsIdStartId,
                                           kFsIdBatchSize);
}

IdGeneratorUPtr NewFsIdGenerator(KVStorageSPtr kv_storage) {
  CHECK(kv_storage != nullptr) << "kv_storage is nullptr.";
  return StoreAutoIncrementIdGenerator::New(kv_storage, kFsAutoIncrementIdName, kFsIdStartId, kFsIdBatchSize);
}

IdGeneratorUPtr NewInodeIdGenerator(uint32_t fs_id, CoordinatorClientSPtr coordinator_client) {
  CHECK(coordinator_client != nullptr) << "coordinator_client is nullptr.";

  std::string name = fmt::format("{}-{}", kInoAutoIncrementIdName, fs_id);

  return CoorAutoIncrementIdGenerator::New(coordinator_client, name, fs_id, kInoStartId, kInoBatchSize);
}

IdGeneratorUPtr NewInodeIdGenerator(uint32_t fs_id, KVStorageSPtr kv_storage) {
  CHECK(kv_storage != nullptr) << "kv_storage is nullptr.";
  std::string name = fmt::format("{}-{}", kInoAutoIncrementIdName, fs_id);

  return StoreAutoIncrementIdGenerator::New(kv_storage, name, kInoStartId, kInoBatchSize);
}

IdGeneratorSPtr NewSliceIdGenerator(CoordinatorClientSPtr coordinator_client) {
  CHECK(coordinator_client != nullptr) << "coordinator_client is nullptr.";
  return CoorAutoIncrementIdGenerator::NewShare(coordinator_client, kSliceAutoIncrementIdName, kSliceTableId,
                                                kSliceIdStartId, kSliceIdBatchSize);
}

IdGeneratorSPtr NewSliceIdGenerator(KVStorageSPtr kv_storage) {
  CHECK(kv_storage != nullptr) << "kv_storage is nullptr.";
  return StoreAutoIncrementIdGenerator::NewShare(kv_storage, kSliceAutoIncrementIdName, kSliceIdStartId,
                                                 kSliceIdBatchSize);
}

void DestroyInodeIdGenerator(uint32_t fs_id, CoordinatorClientSPtr coordinator_client) {
  CHECK(coordinator_client != nullptr) << "coordinator_client is nullptr.";
  std::string name = fmt::format("{}-{}", kInoAutoIncrementIdName, fs_id);

  auto id_generator = CoorAutoIncrementIdGenerator::New(coordinator_client, name, fs_id, kInoStartId, kInoBatchSize);
  id_generator->Destroy();
}

void DestroyInodeIdGenerator(uint32_t fs_id, KVStorageSPtr kv_storage) {
  CHECK(kv_storage != nullptr) << "kv_storage is nullptr.";
  std::string name = fmt::format("{}-{}", kInoAutoIncrementIdName, fs_id);

  auto id_generator = StoreAutoIncrementIdGenerator::New(kv_storage, name, kInoStartId, kInoBatchSize);
  id_generator->Destroy();
}

}  // namespace mds
}  // namespace dingofs