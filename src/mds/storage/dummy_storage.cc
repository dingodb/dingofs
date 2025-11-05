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

#include "mds/storage/dummy_storage.h"

#include "bthread/mutex.h"
#include "dingofs/error.pb.h"
#include "mds/common/helper.h"

namespace dingofs {
namespace mds {

DummyStorage::DummyStorage() { CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail."; }
DummyStorage::~DummyStorage() { CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destory mutex fail."; }

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

Status DummyStorage::DropTable(const Range& range) { return Status(pb::error::ENOT_SUPPORT, "not implemented"); }

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

  if (kv.opt_type == KeyValue::OpType::kDelete) {
    data_.erase(kv.key);
  } else if (kv.opt_type == KeyValue::OpType::kPut) {
    data_[kv.key] = kv.value;
  }

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
    if (kv.opt_type == KeyValue::OpType::kPut) {
      data_[kv.key] = kv.value;
    } else if (kv.opt_type == KeyValue::OpType::kDelete) {
      data_.erase(kv.key);
    }
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

Status DummyStorage::BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) {
  BAIDU_SCOPED_LOCK(mutex_);

  for (const auto& key : keys) {
    auto it = data_.find(key);
    if (it != data_.end()) {
      kvs.push_back(KeyValue{KeyValue::OpType::kPut, key, it->second});
    }
  }

  return Status::OK();
}

Status DummyStorage::Scan(const Range& range, std::vector<KeyValue>& kvs) { return Status::OK(); }

Status DummyStorage::Delete(const std::string& key) {
  BAIDU_SCOPED_LOCK(mutex_);

  data_.erase(key);

  return Status::OK();
}

Status DummyStorage::Delete(const std::vector<std::string>& keys) {
  BAIDU_SCOPED_LOCK(mutex_);

  for (const auto& key : keys) {
    data_.erase(key);
  }

  return Status::OK();
}

TxnUPtr DummyStorage::NewTxn(Txn::IsolationLevel isolation_level) {
  return std::make_unique<DummyTxn>(this, isolation_level);
}

DummyTxn::DummyTxn(DummyStorage* storage, Txn::IsolationLevel isolation_level)
    : storage_(storage), isolation_level_(isolation_level) {
  txn_id_ = Helper::TimestampNs();
}

int64_t DummyTxn::ID() const { return txn_id_; }

Status DummyTxn::Put(const std::string& key, const std::string& value) {
  return storage_->Put(KVStorage::WriteOption{}, key, value);
}

Status DummyTxn::PutIfAbsent(const std::string& key, const std::string& value) {
  KVStorage::WriteOption option;
  option.is_if_absent = true;

  return storage_->Put(option, key, value);
}

Status DummyTxn::Delete(const std::string& key) { return storage_->Delete(key); }

Status DummyTxn::Get(const std::string& key, std::string& value) { return storage_->Get(key, value); }

Status DummyTxn::BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) {
  return storage_->BatchGet(keys, kvs);
}

Status DummyTxn::Scan(const Range& range, uint64_t limit, std::vector<KeyValue>& kvs) {
  return storage_->Scan(range, kvs);
}

Status DummyTxn::Scan(const Range& range, ScanHandlerType handler) {
  std::vector<KeyValue> kvs;
  auto status = storage_->Scan(range, kvs);
  if (!status.ok()) {
    return status;
  }

  for (const auto& kv : kvs) {
    if (!handler(kv.key, kv.value)) {
      break;
    }
  }

  return status;
}

Status DummyTxn::Scan(const Range& range, std::function<bool(KeyValue&)> handler) {
  std::vector<KeyValue> kvs;
  auto status = storage_->Scan(range, kvs);
  if (!status.ok()) {
    return status;
  }

  for (auto& kv : kvs) {
    if (!handler(kv)) {
      break;
    }
  }

  return status;
}

Status DummyTxn::Commit() { return Status::OK(); }

Trace::Txn DummyTxn::GetTrace() { return Trace::Txn(); }

}  // namespace mds
}  // namespace dingofs