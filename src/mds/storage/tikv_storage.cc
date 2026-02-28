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

#include "mds/storage/tikv_storage.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

#include "mds/common/helper.h"
#include "mds/common/synchronization.h"
#include "tikv/lib.rs.h"

namespace dingofs {
namespace mds {

bool TikvStorage::Init(const std::string& addr) {
  LOG(INFO) << fmt::format("[storage] init tikv storage, addr({}).", addr);

  std::vector<std::string> addrs;
  Helper::SplitString(addr, ',', addrs);
  client_ = new tikv_client::TransactionClient(addrs);

  LOG(INFO) << fmt::format("[storage] init tikv storage end, addr({}).", addr);

  return true;
}

bool TikvStorage::Destroy() {
  delete client_;
  client_ = nullptr;

  return true;
}

Status TikvStorage::CreateTable(const std::string& name, const TableOption& option, int64_t& table_id) {
  return Status::OK();
}

Status TikvStorage::DropTable(int64_t table_id) { return Status::OK(); }

Status TikvStorage::DropTable(const Range& range) { return Status::OK(); }

Status TikvStorage::IsExistTable(const std::string& start_key, const std::string& end_key) { return Status::OK(); }

Status TikvStorage::Put(WriteOption option, const std::string& key, const std::string& value) {
  auto txn = TikvTxn(client_, Txn::kSnapshotIsolation);

  txn.Put(key, value);

  return txn.Commit();
}

Status TikvStorage::Put(WriteOption option, KeyValue& kv) {
  auto txn = TikvTxn(client_, Txn::kSnapshotIsolation);

  txn.Put(kv.key, kv.value);

  return txn.Commit();
}

Status TikvStorage::Put(WriteOption option, const std::vector<KeyValue>& kvs) {
  auto txn = TikvTxn(client_, Txn::kSnapshotIsolation);

  for (const auto& kv : kvs) {
    txn.Put(kv.key, kv.value);
  }

  return txn.Commit();
}

Status TikvStorage::Get(const std::string& key, std::string& value) {
  auto txn = TikvTxn(client_, Txn::kSnapshotIsolation);

  auto status = txn.Get(key, value);

  txn.Commit();

  return status;
}

Status TikvStorage::BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) {
  auto txn = TikvTxn(client_, Txn::kSnapshotIsolation);

  auto status = txn.BatchGet(keys, kvs);

  txn.Commit();

  return status;
}

Status TikvStorage::Scan(const Range& range, std::vector<KeyValue>& kvs) {
  auto txn = TikvTxn(client_, Txn::kSnapshotIsolation);

  auto status = txn.Scan(range, UINT64_MAX, kvs);

  txn.Commit();

  return status;
}

Status TikvStorage::Delete(const std::string& key) {
  auto txn = TikvTxn(client_, Txn::kSnapshotIsolation);

  txn.Delete(key);

  return txn.Commit();
}

Status TikvStorage::Delete(const std::vector<std::string>& keys) {
  auto txn = TikvTxn(client_, Txn::kSnapshotIsolation);

  for (const auto& key : keys) {
    txn.Delete(key);
  }

  return txn.Commit();
}

TxnUPtr TikvStorage::NewTxn(Txn::IsolationLevel isolation_level) {
  return std::make_unique<TikvTxn>(client_, isolation_level);
}

static bool IsRetryable(const std::string& err_msg) {
  static const std::vector<std::string> kRetryableErrors = {
      "retryable:",
      "Failed to resolve lock",
  };

  for (const auto& retryable_err : kRetryableErrors) {
    if (err_msg.find(retryable_err) != std::string::npos) {
      return true;
    }
  }

  return false;
}

TikvTxn::TikvTxn(tikv_client::TransactionClient* client, Txn::IsolationLevel isolation_level)
    : client_(client), txn_(client->begin()), isolation_level_(isolation_level) {
  txn_id_ = txn_.id();
};

TikvTxn::~TikvTxn() {
  if (!committed_.load(std::memory_order_relaxed)) Commit();
  client_ = nullptr;
}

int64_t TikvTxn::ID() const { return txn_id_; }

Status TikvTxn::Put(const std::string& key, const std::string& value) {
  txn_.put(key, value);
  return Status::OK();
}

Status TikvTxn::PutIfAbsent(const std::string& key, const std::string& value) {
  return Status(pb::error::ENOT_SUPPORT, "not support yet");
}

Status TikvTxn::Delete(const std::string& key) {
  txn_.remove(key);
  return Status::OK();
}

Status TikvTxn::Get(const std::string& key, std::string& value) {
  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  try {
    auto result = txn_.get(key);

    if (!result.has_value()) {
      return Status(pb::error::ENOT_FOUND, "key not found");
    }
    value = result.value();

  } catch (const std::exception& e) {
    if (IsRetryable(e.what())) {
      return Status(pb::error::ESTORE_MAYBE_RETRY, fmt::format("get err({})", e.what()));
    } else {
      return Status(pb::error::EBACKEND_STORE, fmt::format("get err({})", e.what()));
    }
  }

  return Status::OK();
}

Status TikvTxn::BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) {
  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  try {
    auto kv_pairs = txn_.batch_get(keys);

    for (auto& kv_pair : kv_pairs) {
      KeyValue kv;
      kv.key = std::move(kv_pair.key);
      kv.value = kv_pair.value;

      kvs.push_back(std::move(kv));
    }

  } catch (const std::exception& e) {
    if (IsRetryable(e.what())) {
      return Status(pb::error::ESTORE_MAYBE_RETRY, fmt::format("batchget err({})", e.what()));
    } else {
      return Status(pb::error::EBACKEND_STORE, fmt::format("batchget err({})", e.what()));
    }
  }

  return Status::OK();
}

Status TikvTxn::Scan(const Range& range, uint64_t limit, std::vector<KeyValue>& kvs) {
  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  try {
    auto kv_pairs = txn_.scan(range.start, Bound::Included, range.end, Bound::Included, limit);

    for (auto& kv_pair : kv_pairs) {
      KeyValue kv;
      kv.key = std::move(kv_pair.key);
      kv.value = kv_pair.value;

      kvs.push_back(std::move(kv));
    }

  } catch (const std::exception& e) {
    if (IsRetryable(e.what())) {
      return Status(pb::error::ESTORE_MAYBE_RETRY, fmt::format("scan err({})", e.what()));
    } else {
      return Status(pb::error::EBACKEND_STORE, fmt::format("scan err({})", e.what()));
    }
  }

  return Status::OK();
}

Status TikvTxn::Scan(const Range& range, ScanHandlerType handler) {
  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  try {
    auto kv_pairs = txn_.scan(range.start, Bound::Included, range.end, Bound::Included, UINT32_MAX);

    for (auto& kv_pair : kv_pairs) {
      if (!handler(kv_pair.key, kv_pair.value)) {
        break;
      }
    }

  } catch (const std::exception& e) {
    if (IsRetryable(e.what())) {
      return Status(pb::error::ESTORE_MAYBE_RETRY, fmt::format("scan err({})", e.what()));
    } else {
      return Status(pb::error::EBACKEND_STORE, fmt::format("scan err({})", e.what()));
    }
  }

  return Status::OK();
}

Status TikvTxn::Scan(const Range& range, std::function<bool(KeyValue&)> handler) {
  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  try {
    auto kv_pairs = txn_.scan(range.start, Bound::Included, range.end, Bound::Included, UINT32_MAX);

    for (auto& kv_pair : kv_pairs) {
      KeyValue kv;
      kv.key = std::move(kv_pair.key);
      kv.value = std::move(kv_pair.value);
      if (!handler(kv)) {
        break;
      }
    }

  } catch (const std::exception& e) {
    if (IsRetryable(e.what())) {
      return Status(pb::error::ESTORE_MAYBE_RETRY, fmt::format("scan err({})", e.what()));
    } else {
      return Status(pb::error::EBACKEND_STORE, fmt::format("scan err({})", e.what()));
    }
  }

  return Status::OK();
}

Status TikvTxn::Commit() {
  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.write_time_us += (utils::TimestampUs() - start_time); });

  Status status;
  try {
    txn_.commit();

  } catch (const std::exception& e) {
    if (IsRetryable(e.what())) {
      status = Status(pb::error::ESTORE_MAYBE_RETRY, fmt::format("commit err({})", e.what()));
    } else {
      status = Status(pb::error::EBACKEND_STORE, fmt::format("commit err({})", e.what()));
    }

    txn_.rollback();
  }

  committed_.store(true, std::memory_order_relaxed);

  return status;
}

Trace::Txn TikvTxn::GetTrace() {
  txn_trace_.txn_id = ID();
  return txn_trace_;
}

}  // namespace mds
}  // namespace dingofs