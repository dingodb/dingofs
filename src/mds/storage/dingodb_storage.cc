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

#include "mds/storage/dingodb_storage.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "brpc/reloadable_flags.h"
#include "common/helper.h"
#include "common/logging.h"
#include "dingofs/error.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/helper.h"
#include "mds/common/status.h"
#include "mds/common/synchronization.h"

namespace dingofs {
namespace mds {

DEFINE_int32(mds_storage_dingodb_replica_num, 3, "backend store replicas");

DEFINE_int32(mds_storage_dingodb_scan_batch_size, 100000, "dingodb scan batch size");

// dingo-store rejects a batch get whose key count exceeds its own
// max_batch_get_count (4096 by default), and the sdk splits keys by region
// only, never by count. Keep every request we send under our own cap.
DEFINE_uint32(mds_batch_get_size, 1024, "max key num of one batch get request");
DEFINE_validator(mds_batch_get_size, brpc::PositiveInteger);

DECLARE_uint32(mds_scan_batch_size);
DECLARE_uint32(mds_txn_max_retry_times);

const uint32_t kTxnKeepAliveMs = 10 * 1000;

static bvar::PerSecondEx<bvar::Adder<uint32_t>, 60> g_txn_write_conflict_per_second("txn_write_conflict_sum");
static bvar::PerSecondEx<bvar::Adder<uint32_t>, 60> g_txn_lock_conflict_per_second("txn_lock_conflict_sum");
static bvar::PerSecondEx<bvar::Adder<uint32_t>, 60> g_txn_memlock_conflict_per_second("txn_memlock_conflict_sum");
static bvar::PerSecondEx<bvar::Adder<uint32_t>, 60> g_txn_total_conflict_per_second("txn_total_conflict_sum");

static void KvPairsToKeyValues(const std::vector<dingodb::sdk::KVPair>& kv_pairs, std::vector<KeyValue>& kvs) {
  kvs.reserve(kv_pairs.size());
  for (const auto& kv_pair : kv_pairs) {
    kvs.emplace_back(KeyValue{KeyValue::OpType::kPut, kv_pair.key, kv_pair.value});
  }
}

bool DingodbStorage::Init(const std::string& addr) {
  LOG(INFO) << fmt::format("[storage] init dingo storage, addr({}).", addr);

  dingodb::sdk::ShowSdkVersion();

  auto status = dingodb::sdk::Client::BuildFromAddrs(addr, &client_);
  CHECK(status.ok()) << fmt::format("build dingo sdk client fail, error: {}", status.ToString());

  return true;
}

std::vector<std::pair<std::string, std::string>> DingodbStorage::GetSdkVersion() {
  return dingodb::sdk::GetSdkVersion();
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
               .SetReplicaNum(FLAGS_mds_storage_dingodb_replica_num)
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

Status DingodbStorage::DropTable(const Range& range) {
  auto status = client_->DropRegion(range.start, range.end);
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

DingodbStorage::SdkTxnUPtr DingodbStorage::NewSdkTxn(Txn::IsolationLevel isolation_level) {
  dingodb::sdk::TransactionOptions options;
  options.isolation = (isolation_level == Txn::kSnapshotIsolation)
                          ? dingodb::sdk::TransactionIsolation::kSnapshotIsolation
                          : dingodb::sdk::TransactionIsolation::kReadCommitted;
  options.kind = dingodb::sdk::kOptimistic;
  options.keep_alive_ms = kTxnKeepAliveMs;

  dingodb::sdk::Transaction* txn = nullptr;
  uint32_t retry = 0;
  do {
    auto status = client_->NewTransaction(options, &txn);
    if (status.ok()) break;

    LOG(ERROR) << fmt::format("[storage] new transaction fail, retry({}) error({}).", retry, status.ToString());
  } while (++retry <= FLAGS_mds_txn_max_retry_times);

  if (txn == nullptr) return nullptr;

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

  auto status = txn->Commit();
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

  auto status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

static inline Status TransformStatus(const dingodb::sdk::Status& status) {
  if (status.IsNotFound()) {
    return Status(pb::error::ENOT_FOUND, status.ToString());

  } else if (status.IsTxnWriteConflict()) {
    g_txn_total_conflict_per_second << 1;
    g_txn_write_conflict_per_second << 1;
    return Status(pb::error::ESTORE_MAYBE_RETRY, status.ToString());

  } else if (status.IsTxnLockConflict()) {
    g_txn_total_conflict_per_second << 1;
    g_txn_lock_conflict_per_second << 1;
    return Status(pb::error::ESTORE_MAYBE_RETRY, status.ToString());

  } else if (status.IsTxnMemLockConflict()) {
    g_txn_total_conflict_per_second << 1;
    g_txn_memlock_conflict_per_second << 1;
    return Status(pb::error::ESTORE_MAYBE_RETRY, status.ToString());

  } else if (status.IsPushMinCommitTs()) {
    g_txn_total_conflict_per_second << 1;
    return Status(pb::error::ESTORE_MAYBE_RETRY, status.ToString());

  } else {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }
}

// Batch get in requests of at most mds_batch_get_size keys.
// Callers size their batches by what they are reading (inodes, chunks, ...),
// which says nothing about the key count: one directory inode alone expands
// into 1 + kDirAttrMutationNum keys.
static Status SdkBatchGet(dingodb::sdk::Transaction& txn, const std::vector<std::string>& keys,
                          std::vector<KeyValue>& kvs) {
  const size_t batch_key_num = FLAGS_mds_batch_get_size;
  for (size_t offset = 0; offset < keys.size(); offset += batch_key_num) {
    const size_t end = std::min(offset + batch_key_num, keys.size());
    std::vector<std::string> sub_keys(keys.begin() + offset, keys.begin() + end);

    // sdk BatchGet assigns to its output, so collect per request and append.
    std::vector<dingodb::sdk::KVPair> kv_pairs;
    auto status = txn.BatchGet(sub_keys, kv_pairs);
    if (!status.ok()) return TransformStatus(status);

    KvPairsToKeyValues(kv_pairs, kvs);
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
    return TransformStatus(status);
  }

  status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

Status DingodbStorage::BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) {
  auto txn = NewSdkTxn();
  if (txn == nullptr) {
    return Status(pb::error::EBACKEND_STORE, "new transaction fail");
  }

  auto status = SdkBatchGet(*txn, keys, kvs);
  if (!status.ok()) return status;

  auto sdk_status = txn->Commit();
  if (!sdk_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, sdk_status.ToString());
  }

  return Status::OK();
}

Status DingodbStorage::Scan(const Range& range, std::vector<KeyValue>& kvs) {
  CHECK(range.start < range.end) << fmt::format("invalid range({}/{}).", ::dingofs::Helper::StringToHex(range.start),
                                                ::dingofs::Helper::StringToHex(range.end));

  auto txn = NewSdkTxn();
  if (txn == nullptr) {
    return Status(pb::error::EBACKEND_STORE, "new transaction fail");
  }

  std::vector<dingodb::sdk::KVPair> kv_pairs;
  auto status = txn->Scan(range.start, range.end, FLAGS_mds_storage_dingodb_scan_batch_size, kv_pairs);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  KvPairsToKeyValues(kv_pairs, kvs);

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

  auto status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }

  return Status::OK();
}

TxnUPtr DingodbStorage::NewTxn(Txn::IsolationLevel isolation_level) {
  auto sdk_txn = NewSdkTxn(isolation_level);
  if (sdk_txn == nullptr) return nullptr;

  return std::make_unique<DingodbTxn>(std::move(sdk_txn));
}

DingodbTxn::~DingodbTxn() {
  dingodb::sdk::TraceMetrics metrics;
  txn_->GetTraceMetrics(metrics);
  LOG_DEBUG << fmt::format("[storage.{}] trace {}.", ID(), metrics.ToString());
}

int64_t DingodbTxn::ID() const { return txn_->ID(); }

Status DingodbTxn::Put(const std::string& key, const std::string& value) {
  auto status = txn_->Put(key, value);
  CHECK(status.ok()) << "txn put fail, " << status.ToString();

  return Status::OK();
}

Status DingodbTxn::PutIfAbsent(const std::string& key, const std::string& value) {
  auto status = txn_->PutIfAbsent(key, value);
  CHECK(status.ok()) << "txn put fail, " << status.ToString();

  return Status::OK();
}

Status DingodbTxn::Delete(const std::string& key) {
  auto status = txn_->Delete(key);
  CHECK(status.ok()) << "txn put fail, " << status.ToString();

  return Status::OK();
}

Status DingodbTxn::Get(const std::string& key, std::string& value) {
  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  auto status = txn_->Get(key, value);
  if (!status.ok()) return TransformStatus(status);

  return Status::OK();
}

Status DingodbTxn::BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) {
  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  return SdkBatchGet(*txn_, keys, kvs);
}

Status DingodbTxn::Scan(const Range& range, uint64_t limit, std::vector<KeyValue>& kvs) {
  CHECK(range.start < range.end) << fmt::format("invalid range({}/{}).", ::dingofs::Helper::StringToHex(range.start),
                                                ::dingofs::Helper::StringToHex(range.end));

  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  std::vector<dingodb::sdk::KVPair> kv_pairs;
  auto status = txn_->Scan(range.start, range.end, limit, kv_pairs);
  if (!status.ok()) return TransformStatus(status);

  KvPairsToKeyValues(kv_pairs, kvs);

  return Status::OK();
}

Status DingodbTxn::Scan(const Range& range, ScanHandlerType handler) {
  return Scan(range, [&](KeyValue& kv) { return handler(kv.key, kv.value); });
}

Status DingodbTxn::Scan(const Range& range, std::function<bool(KeyValue&)> handler) {
  Status status;
  Range page = range;
  std::vector<KeyValue> kvs;
  do {
    kvs.clear();
    status = Scan(page, FLAGS_mds_scan_batch_size, kvs);
    if (!status.ok()) break;

    bool is_exit = false;
    for (auto& kv : kvs) {
      if (!handler(kv)) {
        is_exit = true;
        break;
      }
    }
    if (is_exit || kvs.size() < FLAGS_mds_scan_batch_size) break;

    // Scan ranges are start-inclusive. Move just past the last key so the next
    // page does not repeat the same full batch forever.
    page.start = kvs.back().key;
    page.start.push_back('\0');
  } while (page.start < page.end);

  return status;
}

Status DingodbTxn::Commit() {
  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() {
    txn_trace_.commit_type = GetCommitType();
    txn_trace_.write_time_us += (utils::TimestampUs() - start_time);
  });

  auto status = txn_->Commit();
  if (!status.ok()) {
    Rollback();
    return TransformStatus(status);
  }

  return Status::OK();
}

void DingodbTxn::Rollback() {
  auto status = txn_->Rollback();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[storage] rollback fail, status({}).", status.ToString());
  }
}

Trace::Txn DingodbTxn::GetTrace() {
  txn_trace_.txn_id = ID();
  return txn_trace_;
}

char* DingodbTxn::GetCommitType() {
  if (txn_->IsOnePc()) {
    return (char*)"1pc";

  } else if (txn_->IsAsyncCommit()) {
    return (char*)"async";

  } else {
    return (char*)"2pc";
  }
}

Status DingodbTxn::TransformStatus(const dingodb::sdk::Status& status) {
  if (status.IsNotFound()) {
    return Status(pb::error::ENOT_FOUND, status.ToString());

  } else if (status.IsTxnWriteConflict()) {
    g_txn_total_conflict_per_second << 1;
    g_txn_write_conflict_per_second << 1;
    txn_trace_.is_conflict = true;
    return Status(pb::error::ESTORE_MAYBE_RETRY, status.ToString());

  } else if (status.IsTxnLockConflict()) {
    g_txn_total_conflict_per_second << 1;
    g_txn_lock_conflict_per_second << 1;
    txn_trace_.is_conflict = true;
    return Status(pb::error::ESTORE_MAYBE_RETRY, status.ToString());

  } else if (status.IsTxnMemLockConflict()) {
    g_txn_total_conflict_per_second << 1;
    g_txn_memlock_conflict_per_second << 1;
    txn_trace_.is_conflict = true;
    return Status(pb::error::ESTORE_MAYBE_RETRY, status.ToString());

  } else if (status.IsPushMinCommitTs()) {
    g_txn_total_conflict_per_second << 1;
    txn_trace_.is_conflict = true;
    return Status(pb::error::ESTORE_MAYBE_RETRY, status.ToString());

  } else {
    return Status(pb::error::EBACKEND_STORE, status.ToString());
  }
}

}  // namespace mds
}  // namespace dingofs