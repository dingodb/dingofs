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

#include "client/vfs/metasystem/local/id_generator.h"

#include "bthread/mutex.h"
#include "fmt/format.h"
#include "mds/common/codec.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace local {

static const uint32_t kMaxRetryTimes = 5;

LevelIdGenerator::LevelIdGenerator(leveldb::DB* db, const std::string& name,
                                   int64_t start_id, int batch_size)
    : db_(db),
      name_(name),
      key_(mds::MetaCodec::EncodeAutoIncrementIDKey(name)),
      next_id_(start_id),
      last_alloc_id_(start_id),
      batch_size_(batch_size) {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

LevelIdGenerator::~LevelIdGenerator() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destory mutex fail.";
}

bool LevelIdGenerator::Init() {
  uint64_t alloc_id = 0;
  auto status = GetOrPutAllocId(alloc_id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[idalloc.{}] init get alloc id fail, status({}).", name_,
        status.ToString());
    return false;
  }

  next_id_ = alloc_id;
  last_alloc_id_ = alloc_id;

  return true;
}

bool LevelIdGenerator::Destroy() {
  BAIDU_SCOPED_LOCK(mutex_);

  auto status = DestroyId();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[idalloc.{}] destroy autoincrement table fail, status({}).", name_,
        status.ToString());
    return false;
  }

  return true;
}

bool LevelIdGenerator::GenID(uint32_t num, uint64_t& id) {
  return GenID(num, 0, id);
}

bool LevelIdGenerator::GenID(uint32_t num, uint64_t min_slice_id,
                             uint64_t& id) {
  if (num == 0) {
    LOG(ERROR) << fmt::format("[idalloc.{}] num cant not 0.", name_);
    return false;
  }

  BAIDU_SCOPED_LOCK(mutex_);

  next_id_ = std::max(next_id_, min_slice_id);

  if (next_id_ + num > last_alloc_id_) {
    auto status = AllocateIds(std::max(num, batch_size_));
    if (!status.ok()) {
      LOG(ERROR) << fmt::format("[idalloc.{}] allocate id fail, {}.", name_,
                                status.ToString());
      return false;
    }
  }

  // allocate id
  id = next_id_;
  next_id_ += num;

  LOG(INFO) << fmt::format("[idalloc.{}] alloc id({}) num({}).", name_, id,
                           num);

  return true;
}

std::string LevelIdGenerator::Describe() const {
  return fmt::format(
      "[store] name({}) batch_size({}) last_alloc_id({}) next_id({})", name_,
      batch_size_, last_alloc_id_, next_id_);
}

Status LevelIdGenerator::GetOrPutAllocId(uint64_t& alloc_id) {
  std::string value;
  auto status = Get(key_, value);
  if (!status.ok()) {
    if (!status.IsNotFound()) return status;

    alloc_id = 0;

  } else {
    mds::MetaCodec::DecodeAutoIncrementIDValue(value, alloc_id);
  }

  if (alloc_id < last_alloc_id_) {
    alloc_id = last_alloc_id_;
    auto status =
        Put(key_, mds::MetaCodec::EncodeAutoIncrementIDValue(alloc_id));
    if (!status.ok()) return status;
  }

  return Status::OK();
}

Status LevelIdGenerator::AllocateIds(uint32_t size) {
  utils::Duration duration;
  Status status;
  uint32_t retry = 0;
  uint64_t start_alloc_id = std::max(next_id_, last_alloc_id_);
  do {
    uint64_t alloced_id = 0;
    std::string value;
    status = Get(key_, value);
    if (!status.ok()) {
      if (!status.IsNotFound()) break;

    } else {
      mds::MetaCodec::DecodeAutoIncrementIDValue(value, alloced_id);
    }

    start_alloc_id = std::max(alloced_id, start_alloc_id);
    status =
        Put(key_,
            mds::MetaCodec::EncodeAutoIncrementIDValue(start_alloc_id + size));
    if (status.ok()) break;

  } while (++retry < kMaxRetryTimes);

  if (status.ok()) {
    last_alloc_id_ = start_alloc_id + size;
    next_id_ = start_alloc_id;
  }

  LOG(INFO) << fmt::format(
      "[idalloc.{}][{}us] take bundle id, bundle[{},{}) size({}) status({}).",
      name_, duration.ElapsedUs(), next_id_, last_alloc_id_, size,
      status.ToString());

  return status;
}

Status LevelIdGenerator::DestroyId() {
  auto status = db_->Delete(leveldb::WriteOptions(), key_);
  if (!status.ok()) {
    return Status::Internal(status.ToString());
  }

  return Status::OK();
}

Status LevelIdGenerator::Get(const std::string& key, std::string& value) {
  auto status = db_->Get(leveldb::ReadOptions(), key, &value);
  if (!status.ok()) {
    if (status.IsNotFound()) return Status::NotFound("not found");
    return Status::Internal(fmt::format("get fail, {}.", status.ToString()));
  }

  return Status::OK();
}

Status LevelIdGenerator::Put(const std::string& key, const std::string& value) {
  auto status = db_->Put(leveldb::WriteOptions(), key, value);
  if (!status.ok()) {
    return Status::Internal(fmt::format("put fail, {}.", status.ToString()));
  }

  return Status::OK();
}

}  // namespace local
}  // namespace vfs
}  // namespace client
}  // namespace dingofs