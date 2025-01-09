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

#include "mdsv2/storage/dummy_storage.h"

#include "bthread/mutex.h"
#include "dingofs/error.pb.h"

namespace dingofs {
namespace mdsv2 {

DummyStorage::DummyStorage() { bthread_mutex_init(&mutex_, nullptr); }
DummyStorage::~DummyStorage() { bthread_mutex_destroy(&mutex_); }

bool DummyStorage::Init(const std::string&) { return true; }

bool DummyStorage::Destroy() { return true; }

Status DummyStorage::CreateTable(const std::string& name, const TableOption& option, int64_t& table_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  tables_[++next_table_id_] = Table{name, option.start_key, option.end_key};
  table_id = next_table_id_;

  return Status::OK();
}

Status DummyStorage::DropTable(int64_t table_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  tables_.erase(table_id);

  return Status::OK();
}

Status DummyStorage::IsExistTable(const std::string& start_key, const std::string& end_key) {
  BAIDU_SCOPED_LOCK(mutex_);

  for (const auto& [table_id, table] : tables_) {
    if (table.start_key == start_key && table.end_key == end_key) {
      return Status::OK();
    }
  }

  return Status(pb::error::ENOT_FOUND, "table not exist");
}

Status DummyStorage::Put(WriteOption option, const std::string& key, const std::string& value) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (option.is_if_absent) {
    auto it = data_.find(key);
    if (it != data_.end()) {
      return Status(pb::error::EEXISTED, "key already exist");
    }
  }

  data_[key] = value;

  return Status::OK();
}

Status DummyStorage::Put(WriteOption option, KeyValue& kv) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (option.is_if_absent) {
    auto it = data_.find(kv.key);
    if (it != data_.end()) {
      return Status(pb::error::EEXISTED, "key already exist");
    }
  }

  data_[kv.key] = kv.value;

  return Status::OK();
}

Status DummyStorage::Put(WriteOption option, const std::vector<KeyValue>& kvs) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (option.is_if_absent) {
    for (const auto& kv : kvs) {
      auto it = data_.find(kv.key);
      if (it != data_.end()) {
        return Status(pb::error::EEXISTED, "key already exist");
      }
    }
  }

  for (const auto& kv : kvs) {
    data_[kv.key] = kv.value;
  }

  return Status::OK();
}

Status DummyStorage::Get(const std::string& key, std::string& value) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = data_.find(key);
  if (it == data_.end()) {
    return Status(pb::error::ENOT_FOUND, "key not found");
  }

  value = it->second;

  return Status::OK();
}

Status DummyStorage::Delete(const std::string& key) {
  BAIDU_SCOPED_LOCK(mutex_);
  data_.erase(key);
  return Status::OK();
}

}  // namespace mdsv2
}  // namespace dingofs