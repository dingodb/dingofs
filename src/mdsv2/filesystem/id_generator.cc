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

#include "mdsv2/filesystem/id_generator.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdint>

#include "bthread/mutex.h"
#include "dingofs/error.pb.h"
#include "fmt/core.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_uint32(txn_max_retry_times);

AutoIncrementIdGenerator::AutoIncrementIdGenerator(CoordinatorClientSPtr client, int64_t table_id, uint64_t start_id,
                                                   uint32_t batch_size)
    : client_(client), table_id_(table_id), start_id_(start_id), batch_size_(batch_size) {
  next_id_ = start_id;
  bundle_ = start_id;
  bundle_end_ = start_id;

  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

AutoIncrementIdGenerator::~AutoIncrementIdGenerator() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destory mutex fail.";
}

bool AutoIncrementIdGenerator::Init() {
  auto status = IsExistAutoIncrement();
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      return false;
    }

    DINGO_LOG(INFO) << fmt::format("[idalloc.{}] auto increment table not exist.", table_id_);
  } else {
    DINGO_LOG(INFO) << fmt::format("[idalloc.{}] auto increment table exist.", table_id_);
    return true;
  }

  status = CreateAutoIncrement();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Create auto increment table fail, error: " << status.error_str();
    return false;
  }

  return true;
}

bool AutoIncrementIdGenerator::GenID(uint32_t num, uint64_t& id) { return GenID(num, 0, id); }

bool AutoIncrementIdGenerator::GenID(uint32_t num, uint64_t min_slice_id, uint64_t& id) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (next_id_ < min_slice_id) {
    next_id_ = min_slice_id;
  }

  if (next_id_ + num > bundle_end_) {
    auto status = AllocateIds(std::max(num, batch_size_));
    if (!status.ok()) {
      return false;
    }
  }

  id = next_id_;
  next_id_ += num;

  DINGO_LOG(INFO) << fmt::format("[idalloc.{}] alloc id {},{} bundle[{}, {}).", table_id_, id, num, bundle_,
                                 bundle_end_);

  return true;
}

Status AutoIncrementIdGenerator::IsExistAutoIncrement() {
  int64_t start_id = -1;
  return client_->GetAutoIncrement(table_id_, start_id);
}

Status AutoIncrementIdGenerator::CreateAutoIncrement() {
  DINGO_LOG(INFO) << fmt::format("[idalloc.{}] create auto increment table, start_id({}).", table_id_, start_id_);
  return client_->CreateAutoIncrement(table_id_, start_id_);
}

Status AutoIncrementIdGenerator::AllocateIds(uint32_t num) {
  int64_t bundle = 0;
  int64_t bundle_end = 0;
  do {
    auto status = client_->GenerateAutoIncrement(table_id_, num, bundle, bundle_end);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[idalloc.{}] take bundle id fail, {}.", table_id_, status.error_str());
      return status;
    }

    CHECK(bundle >= 0 && bundle_end >= 0) << "bundle id is negative.";
  } while (bundle < next_id_);

  bundle_ = bundle;
  next_id_ = bundle;
  bundle_end_ = bundle_end;

  DINGO_LOG(INFO) << fmt::format("[idalloc.{}] take bundle id, bundle[{}, {}).", table_id_, bundle_, bundle_end_);

  return Status::OK();
}

StoreAutoIncrementIdGenerator::StoreAutoIncrementIdGenerator(KVStorageSPtr kv_storage, const std::string& name,
                                                             int64_t start_id, int batch_size)
    : kv_storage_(kv_storage),
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
    DINGO_LOG(ERROR) << fmt::format("[idalloc.{}] init get alloc id fail.", key_);
  }

  next_id_ = alloc_id;
  last_alloc_id_ = alloc_id;

  return true;
}

bool StoreAutoIncrementIdGenerator::GenID(uint32_t num, uint64_t& id) { return GenID(num, 0, id); }

bool StoreAutoIncrementIdGenerator::GenID(uint32_t num, uint64_t min_slice_id, uint64_t& id) {
  if (num == 0) {
    DINGO_LOG(ERROR) << fmt::format("[idalloc.{}] num cant not 0.", key_);
    return -1;
  }

  BAIDU_SCOPED_LOCK(mutex_);

  if (next_id_ < min_slice_id) {
    next_id_ = min_slice_id;
  }

  if (next_id_ + num > last_alloc_id_) {
    auto status = AllocateIds(std::max(num, batch_size_));
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[idalloc.{}] allocate id fail, {}.", key_, status.error_str());
      return -1;
    }
  }

  // allocate id
  id = next_id_;
  next_id_ += num;

  DINGO_LOG(DEBUG) << fmt::format("[idalloc.{}] alloc id({}) num({}).", key_, id, num);

  return true;
}

Status StoreAutoIncrementIdGenerator::GetOrPutAllocId(uint64_t& alloc_id) {
  Status status;
  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

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

  } while (++retry < FLAGS_txn_max_retry_times);

  return status;
}

Status StoreAutoIncrementIdGenerator::AllocateIds(uint32_t size) {
  Status status;
  int retry = 0;
  uint64_t start_alloc_id = std::max(next_id_, last_alloc_id_);
  do {
    auto txn = kv_storage_->NewTxn();

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

  } while (++retry < FLAGS_txn_max_retry_times);

  if (status.ok()) {
    last_alloc_id_ = start_alloc_id + size;
    next_id_ = start_alloc_id;
  }

  return status;
}

}  // namespace mdsv2
}  // namespace dingofs