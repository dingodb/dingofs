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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <set>
#include <utility>
#include <vector>

#include "dingofs/error.pb.h"

namespace dingofs {
namespace mds {

bool DummyStorage::Init(const std::string&) { return true; }

bool DummyStorage::Destroy() { return true; }

Status DummyStorage::CreateTable(const std::string& name, const TableOption& option, int64_t& table_id) {
  utils::WriteLockGuard lock(lock_);

  tables_[++next_table_id_] = Table{name, option.start_key, option.end_key};
  table_id = next_table_id_;

  return Status::OK();
}

Status DummyStorage::DropTable(int64_t table_id) {
  utils::WriteLockGuard lock(lock_);

  auto it = tables_.find(table_id);
  if (it == tables_.end()) return Status::OK();

  // Erase all data belonging to this table's key range.
  const auto& tbl = it->second;
  if (!tbl.start_key.empty() || !tbl.end_key.empty()) {
    auto first = data_.lower_bound(tbl.start_key);
    auto last = data_.lower_bound(tbl.end_key);
    data_.erase(first, last);
  }
  tables_.erase(it);

  return Status::OK();
}

Status DummyStorage::DropTable(const Range& range) {
  utils::WriteLockGuard lock(lock_);

  for (auto it = tables_.begin(); it != tables_.end();) {
    if (it->second.start_key == range.start && it->second.end_key == range.end) {
      it = tables_.erase(it);
    } else {
      ++it;
    }
  }

  // Erase data in the requested range, regardless of whether a matching
  // table descriptor existed.
  auto first = data_.lower_bound(range.start);
  auto last = data_.lower_bound(range.end);
  data_.erase(first, last);

  return Status::OK();
}

Status DummyStorage::IsExistTable(const std::string& start_key, const std::string& end_key) {  // NOLINT
  return Status::OK();
}

Status DummyStorage::Put(WriteOption option, const std::string& key, const std::string& value) {
  utils::WriteLockGuard lock(lock_);

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
  utils::WriteLockGuard lock(lock_);

  // is_if_absent only makes sense for Put operations.
  if (option.is_if_absent && kv.opt_type == KeyValue::OpType::kPut) {
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
  utils::WriteLockGuard lock(lock_);

  if (option.is_if_absent) {
    for (const auto& kv : kvs) {
      if (kv.opt_type != KeyValue::OpType::kPut) continue;
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
  utils::ReadLockGuard lock(lock_);

  auto it = data_.find(key);
  if (it == data_.end()) {
    return Status(pb::error::ENOT_FOUND, "key not found");
  }

  value = it->second;

  return Status::OK();
}

Status DummyStorage::BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) {
  utils::ReadLockGuard lock(lock_);

  for (const auto& key : keys) {
    auto it = data_.find(key);
    if (it != data_.end()) {
      kvs.push_back(KeyValue{KeyValue::OpType::kPut, key, it->second});
    }
  }

  return Status::OK();
}

Status DummyStorage::Scan(const Range& range, std::vector<KeyValue>& kvs) {
  utils::ReadLockGuard lock(lock_);

  for (auto it = data_.lower_bound(range.start); it != data_.end(); ++it) {
    if (it->first >= range.end) {
      break;
    }
    kvs.push_back(KeyValue{KeyValue::OpType::kPut, it->first, it->second});
  }

  return Status::OK();
}

Status DummyStorage::Delete(const std::string& key) {
  utils::WriteLockGuard lock(lock_);

  data_.erase(key);

  return Status::OK();
}

Status DummyStorage::Delete(const std::vector<std::string>& keys) {
  utils::WriteLockGuard lock(lock_);

  for (const auto& key : keys) {
    data_.erase(key);
  }

  return Status::OK();
}

TxnUPtr DummyStorage::NewTxn(Txn::IsolationLevel isolation_level) {
  return std::make_unique<DummyTxn>(this, isolation_level);
}

Status DummyStorage::ApplyTxn(const std::map<std::string, KeyValue>& writes,
                              const std::set<std::string>& if_absent_keys) {
  utils::WriteLockGuard lock(lock_);

  // Re-verify if-absent keys atomically with the apply.
  for (const auto& key : if_absent_keys) {
    auto wit = writes.find(key);
    // Skip if the key was overwritten to a delete after PutIfAbsent.
    if (wit == writes.end() || wit->second.opt_type != KeyValue::OpType::kPut) {
      continue;
    }
    if (data_.find(key) != data_.end()) {
      return Status(pb::error::EEXISTED, "key already exist");
    }
  }

  for (const auto& [key, kv] : writes) {
    if (kv.opt_type == KeyValue::OpType::kPut) {
      data_[key] = kv.value;
    } else if (kv.opt_type == KeyValue::OpType::kDelete) {
      data_.erase(key);
    }
  }

  return Status::OK();
}

DummyTxn::DummyTxn(DummyStorage* storage, Txn::IsolationLevel isolation_level)
    : storage_(storage), isolation_level_(isolation_level) {
  static std::atomic<int64_t> kNextTxnId{1};
  txn_id_ = kNextTxnId.fetch_add(1, std::memory_order_relaxed);
}

int64_t DummyTxn::ID() const { return txn_id_; }

Status DummyTxn::Put(const std::string& key, const std::string& value) {
  if (committed_) return Status(pb::error::EINTERNAL, "txn already committed");

  stage_writes_[key] = KeyValue{KeyValue::OpType::kPut, key, value};
  // A subsequent unconditional Put overrides any prior PutIfAbsent intent.
  if_absent_keys_.erase(key);
  return Status::OK();
}

Status DummyTxn::PutIfAbsent(const std::string& key, const std::string& value) {
  if (committed_) return Status(pb::error::EINTERNAL, "txn already committed");

  // A prior staged Put in this txn means the key is already present from the
  // txn's view -- reject immediately.
  auto sit = stage_writes_.find(key);
  if (sit != stage_writes_.end() &&
      sit->second.opt_type == KeyValue::OpType::kPut) {
    return Status(pb::error::EEXISTED, "key already exist");
  }

  // Verify the key is absent in storage. Re-checked atomically at Commit.
  std::string ignored;
  auto status = storage_->Get(key, ignored);
  if (status.ok()) {
    return Status(pb::error::EEXISTED, "key already exist");
  }
  if (status.error_code() != pb::error::ENOT_FOUND) {
    return status;
  }

  stage_writes_[key] = KeyValue{KeyValue::OpType::kPut, key, value};
  if_absent_keys_.insert(key);
  return Status::OK();
}

Status DummyTxn::Delete(const std::string& key) {
  if (committed_) return Status(pb::error::EINTERNAL, "txn already committed");

  stage_writes_[key] = KeyValue{KeyValue::OpType::kDelete, key, ""};
  if_absent_keys_.erase(key);
  return Status::OK();
}

Status DummyTxn::Get(const std::string& key, std::string& value) {
  auto it = stage_writes_.find(key);
  if (it != stage_writes_.end()) {
    if (it->second.opt_type == KeyValue::OpType::kDelete) {
      return Status(pb::error::ENOT_FOUND, "key not found");
    }
    value = it->second.value;
    return Status::OK();
  }

  return storage_->Get(key, value);
}

Status DummyTxn::BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) {
  std::vector<std::string> rest_keys;
  rest_keys.reserve(keys.size());

  for (const auto& key : keys) {
    auto it = stage_writes_.find(key);
    if (it == stage_writes_.end()) {
      rest_keys.push_back(key);
      continue;
    }
    // Staged delete masks any underlying value; staged put returns the staged value.
    if (it->second.opt_type == KeyValue::OpType::kPut) {
      kvs.push_back(it->second);
    }
  }

  if (rest_keys.empty()) return Status::OK();

  std::vector<KeyValue> rest_kvs;
  auto status = storage_->BatchGet(rest_keys, rest_kvs);
  if (!status.ok()) return status;

  kvs.insert(kvs.end(), rest_kvs.begin(), rest_kvs.end());
  return Status::OK();
}

// Merges a sorted storage scan result with the txn's staged writes for the
// given range. Iterates in ascending key order, applying staged
// puts/deletes to mask or override storage values.
static void MergeScanWithStage(const Range& range, const std::map<std::string, KeyValue>& stage_writes,
                               std::vector<KeyValue>&& storage_kvs, uint64_t limit, std::vector<KeyValue>& out) {
  auto sit = stage_writes.lower_bound(range.start);
  auto send = stage_writes.lower_bound(range.end);

  auto kit = storage_kvs.begin();
  auto kend = storage_kvs.end();

  auto emit = [&](KeyValue kv) -> bool {
    out.push_back(std::move(kv));
    return out.size() < limit;
  };

  while (out.size() < limit && (kit != kend || sit != send)) {
    bool take_stage;
    if (kit == kend) {
      take_stage = true;
    } else if (sit == send) {
      take_stage = false;
    } else if (sit->first < kit->key) {
      take_stage = true;
    } else if (sit->first == kit->key) {
      take_stage = true;
      ++kit;  // staged op overrides storage value for this key
    } else {
      take_stage = false;
    }

    if (take_stage) {
      if (sit->second.opt_type == KeyValue::OpType::kPut) {
        if (!emit(sit->second)) return;
      }
      ++sit;
    } else {
      if (!emit(*kit)) return;
      ++kit;
    }
  }
}

Status DummyTxn::Scan(const Range& range, uint64_t limit, std::vector<KeyValue>& kvs) {
  std::vector<KeyValue> storage_kvs;
  auto status = storage_->Scan(range, storage_kvs);
  if (!status.ok()) return status;

  if (limit == 0) limit = UINT64_MAX;
  MergeScanWithStage(range, stage_writes_, std::move(storage_kvs), limit, kvs);
  return Status::OK();
}

Status DummyTxn::Scan(const Range& range, ScanHandlerType handler) {
  std::vector<KeyValue> storage_kvs;
  auto status = storage_->Scan(range, storage_kvs);
  if (!status.ok()) return status;

  std::vector<KeyValue> merged;
  MergeScanWithStage(range, stage_writes_, std::move(storage_kvs), UINT64_MAX, merged);

  for (const auto& kv : merged) {
    if (!handler(kv.key, kv.value)) break;
  }
  return Status::OK();
}

Status DummyTxn::Scan(const Range& range, std::function<bool(KeyValue&)> handler) {
  std::vector<KeyValue> storage_kvs;
  auto status = storage_->Scan(range, storage_kvs);
  if (!status.ok()) return status;

  std::vector<KeyValue> merged;
  MergeScanWithStage(range, stage_writes_, std::move(storage_kvs), UINT64_MAX, merged);

  for (auto& kv : merged) {
    if (!handler(kv)) break;
  }
  return Status::OK();
}

Status DummyTxn::Commit() {
  if (committed_) return Status(pb::error::EINTERNAL, "txn already committed");
  committed_ = true;

  auto status = storage_->ApplyTxn(stage_writes_, if_absent_keys_);
  stage_writes_.clear();
  if_absent_keys_.clear();
  return status;
}

Trace::Txn DummyTxn::GetTrace() { return Trace::Txn(); }

}  // namespace mds
}  // namespace dingofs