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

#include "mdsv2/storage/dingodb_storage.h"

#include <cstdint>
#include <memory>
#include <string>

#include "dingofs/error.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/synchronization.h"

namespace dingofs {

namespace mdsv2 {

DEFINE_int32(dingodb_replica_num, 3, "backend store replicas");

DEFINE_int32(dingodb_scan_batch_size, 100000, "dingodb scan batch size");

const uint32_t kTxnKeepAliveMs = 10 * 1000;

static dingodb::sdk::KVPair ToKVPair(const KeyValue& kv) { return dingodb::sdk::KVPair{kv.key, kv.value}; }

static void KvPairToKeyValue(const dingodb::sdk::KVPair& kv_pair, KeyValue& kv) {
  kv.key = kv_pair.key;
  kv.value = kv_pair.value;
}

static void KvPairsToKeyValues(const std::vector<dingodb::sdk::KVPair>& kv_pairs, std::vector<KeyValue>& kvs) {
  kvs.reserve(kv_pairs.size());
  for (const auto& kv_pair : kv_pairs) {
    kvs.emplace_back(KeyValue{KeyValue::OpType::kPut, kv_pair.key, kv_pair.value});
  }
}

static std::vector<dingodb::sdk::KVPair> ToKVPairs(const std::vector<KeyValue>& kvs) {
  std::vector<dingodb::sdk::KVPair> kv_pairs;
  kv_pairs.reserve(kvs.size());
  for (const auto& kv : kvs) {
    kv_pairs.emplace_back(dingodb::sdk::KVPair{kv.key, kv.value});
  }

  return kv_pairs;
}

bool DingodbStorage::Init(const std::string& addr) {
  DINGO_LOG(INFO) << fmt::format("init dingo storage, addr({}).", addr);

  auto status = dingodb::sdk::Client::BuildFromAddrs(addr, &client_);
  CHECK(status.ok()) << fmt::format("build dingo sdk client fail, error: {}", status.ToString());

  return true;
}

bool DingodbStorage::Destroy() {
  DINGO_LOG(INFO) << "destroy dingo storage.";

  delete client_;

  return true;
}

Status DingodbStorage::CreateTable(const std::string& name, const TableOption& option, int64_t& table_id) {
  dingodb::sdk::RegionCreator* creator = nullptr;
  auto status = client_->NewRegionCreator(&creator);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  int64_t region_id = 0;
  status = creator->SetRegionName(name)
               .SetRange(option.start_key, option.end_key)
               .SetReplicaNum(FLAGS_dingodb_replica_num)
               .Wait(true)
               .Create(region_id);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  table_id = region_id;

  return Status::OK();
}

Status DingodbStorage::DropTable(int64_t table_id) {
  auto status = client_->DropRegion(table_id);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Status DingodbStorage::IsExistTable(const std::string& start_key, const std::string& end_key) {
  dingodb::sdk::Coordinator* coordinator{nullptr};
  auto status = client_->NewCoordinator(&coordinator);
  CHECK(status.ok()) << fmt::format("new dingo sdk coordinator fail, error: {}", status.ToString());

  std::vector<int64_t> region_ids;
  status = coordinator->ScanRegions(start_key, end_key, region_ids);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return region_ids.empty() ? Status(pb::error::ENOT_FOUND, "table not exist") : Status::OK();
}

DingodbStorage::SdkTxnUPtr DingodbStorage::NewSdkTxn() {
  dingodb::sdk::TransactionOptions options;
  options.isolation = dingodb::sdk::TransactionIsolation::kSnapshotIsolation;
  options.kind = dingodb::sdk::kOptimistic;
  options.keep_alive_ms = kTxnKeepAliveMs;

  dingodb::sdk::Transaction* txn = nullptr;
  auto status = client_->NewTransaction(options, &txn);
  CHECK(status.ok()) << fmt::format("new transaction fail, error: {}", status.ToString());

  return DingodbStorage::SdkTxnUPtr(txn);
}

Status DingodbStorage::Put(WriteOption option, const std::string& key, const std::string& value) {
  auto txn = NewSdkTxn();
  if (txn == nullptr) {
    return Status(pb::error::EBACKEND_STORE, "new transaction fail");
  }

  auto status = option.is_if_absent ? txn->PutIfAbsent(key, value) : txn->Put(key, value);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  status = txn->PreCommit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }
  status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Status DingodbStorage::Put(WriteOption option, KeyValue& kv) {
  auto txn = NewSdkTxn();
  if (txn == nullptr) {
    return Status(pb::error::EBACKEND_STORE, "new transaction fail");
  }

  if (kv.opt_type == KeyValue::OpType::kPut) {
    auto status = option.is_if_absent ? txn->PutIfAbsent(kv.key, kv.value) : txn->Put(kv.key, kv.value);
    if (!status.ok()) {
      return Status(pb::error::EBACKEND_STORE, status.ToString());
    }
  } else if (kv.opt_type == KeyValue::OpType::kDelete) {
    auto status = txn->Delete(kv.key);
    if (!status.ok()) {
      return Status(pb::error::EBACKEND_STORE, status.ToString());
    }
  }

  auto status = txn->PreCommit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }
  status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Status DingodbStorage::Put(WriteOption option, const std::vector<KeyValue>& kvs) {
  auto txn = NewSdkTxn();
  if (txn == nullptr) {
    return Status(pb::error::EBACKEND_STORE, "new transaction fail");
  }

  for (const auto& kv : kvs) {
    if (kv.opt_type == KeyValue::OpType::kPut) {
      auto status = option.is_if_absent ? txn->PutIfAbsent(kv.key, kv.value) : txn->Put(kv.key, kv.value);
      if (!status.ok()) {
        return Status(pb::error::EBACKEND_STORE, status.ToString());
      }
    } else if (kv.opt_type == KeyValue::OpType::kDelete) {
      auto status = txn->Delete(kv.key);
      if (!status.ok()) {
        return Status(pb::error::EBACKEND_STORE, status.ToString());
      }
    }
  }

  auto status = txn->PreCommit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }
  status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Status DingodbStorage::Get(const std::string& key, std::string& value) {
  auto txn = NewSdkTxn();
  if (txn == nullptr) {
    return Status(pb::error::EBACKEND_STORE, "new transaction fail");
  }

  auto status = txn->Get(key, value);
  if (!status.ok()) {
    return status.IsNotFound() ? Status(pb::error::ENOT_FOUND, status.ToString())
                               : Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  status = txn->PreCommit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }
  status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Status DingodbStorage::Scan(const Range& range, std::vector<KeyValue>& kvs) {
  auto txn = NewSdkTxn();
  if (txn == nullptr) {
    return Status(pb::error::EBACKEND_STORE, "new transaction fail");
  }

  std::vector<dingodb::sdk::KVPair> kv_pairs;
  auto status = txn->Scan(range.start_key, range.end_key, FLAGS_dingodb_scan_batch_size, kv_pairs);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  KvPairsToKeyValues(kv_pairs, kvs);

  status = txn->PreCommit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }
  status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Status DingodbStorage::Delete(const std::string& key) {
  auto txn = NewSdkTxn();
  if (txn == nullptr) {
    return Status(pb::error::EBACKEND_STORE, "new transaction fail");
  }

  auto status = txn->Delete(key);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  status = txn->PreCommit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }
  status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Status DingodbStorage::Delete(const std::vector<std::string>& keys) {
  auto txn = NewSdkTxn();
  if (txn == nullptr) {
    return Status(pb::error::EBACKEND_STORE, "new transaction fail");
  }

  for (const auto& key : keys) {
    auto status = txn->Delete(key);
    if (!status.ok()) {
      return Status(pb::error::EBACKEND_STORE, status.ToString());
    }
  }

  auto status = txn->PreCommit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }
  status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

TxnUPtr DingodbStorage::NewTxn() { return std::make_unique<DingodbTxn>(NewSdkTxn()); }

Status DingodbTxn::Put(const std::string& key, const std::string& value) {
  auto status = txn_->Put(key, value);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Status DingodbTxn::PutIfAbsent(const std::string& key, const std::string& value) {
  auto status = txn_->PutIfAbsent(key, value);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Status DingodbTxn::Delete(const std::string& key) {
  auto status = txn_->Delete(key);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Status DingodbTxn::Get(const std::string& key, std::string& value) {
  uint64_t start_time = Helper::TimestampUs();
  DEFER(txn_trace_.read_time_us += (Helper::TimestampUs() - start_time));

  auto status = txn_->Get(key, value);
  if (!status.ok()) {
    return status.IsNotFound() ? Status(pb::error::ENOT_FOUND, status.ToString())
                               : Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Status DingodbTxn::Scan(const Range& range, uint64_t limit, std::vector<KeyValue>& kvs) {
  uint64_t start_time = Helper::TimestampUs();
  DEFER(txn_trace_.read_time_us += (Helper::TimestampUs() - start_time));

  std::vector<dingodb::sdk::KVPair> kv_pairs;
  auto status = txn_->Scan(range.start_key, range.end_key, limit, kv_pairs);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  KvPairsToKeyValues(kv_pairs, kvs);

  return Status::OK();
}

void DingodbTxn::Rollback() {
  auto status = txn_->Rollback();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("rollback fail, error: {}", status.ToString());
  }
}

Status DingodbTxn::Commit() {
  uint64_t start_time = Helper::TimestampUs();
  ON_SCOPE_EXIT([&]() {
    txn_trace_.is_one_pc = txn_->IsOnePc();
    txn_trace_.write_time_us += (Helper::TimestampUs() - start_time);
  });

  auto status = txn_->PreCommit();
  if (!status.ok()) {
    Rollback();
    if (status.IsTxnWriteConflict()) {
      txn_trace_.is_conflict = true;
      return Status(pb::error::ESTORE_MAYBE_RETRY, status.ToString());
    }
    return Status(status.Errno(), status.ToString());
  }

  status = txn_->Commit();
  if (!status.ok()) {
    Rollback();
    if (status.IsTxnWriteConflict()) {
      txn_trace_.is_conflict = true;
      return Status(pb::error::ESTORE_MAYBE_RETRY, status.ToString());
    }
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Trace::Txn DingodbTxn::GetTrace() { return txn_trace_; }

}  // namespace mdsv2
}  // namespace dingofs