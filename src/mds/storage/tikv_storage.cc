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

#include <bthread/countdown_event.h>
#include <glog/logging.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "mds/common/helper.h"
#include "mds/common/synchronization.h"
#include "tikv/lib.rs.h"

namespace dingofs {
namespace mds {

static const uint32_t kScanLimit = 4096;

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
  return std::make_unique<TikvTxnAsync>(client_, isolation_level);
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

static bool IsOutOfRange(const std::string& err_msg) { return err_msg.find("OutOfRange") != std::string::npos; }

TikvTxn::TikvTxn(tikv_client::TransactionClient* client, Txn::IsolationLevel isolation_level)
    : client_(client), txn_(client->begin()), isolation_level_(isolation_level) {
  txn_id_ = txn_.id();
}

TikvTxn::~TikvTxn() {
  if (!committed_.load(std::memory_order_relaxed)) Commit();
  client_ = nullptr;
}

int64_t TikvTxn::ID() const { return txn_id_; }

Status TikvTxn::Put(const std::string& key, const std::string& value) {
  txn_.put(key, value);
  return Status::OK();
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

Status TikvTxn::DoScan(const Range& range, uint32_t limit, std::vector<KeyValue>& kvs) {
  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  uint32_t actual_limit = std::min(kScanLimit, limit);
  kvs.reserve(actual_limit);

  std::string start_key = range.start;
  do {
    try {
      LOG_DEBUG << fmt::format("[storage.{}] scan range[{}, {}), start_key: {}, limit: {}", txn_id_,
                               Helper::StringToHex(range.start), Helper::StringToHex(range.end),
                               Helper::StringToHex(start_key), actual_limit);

      auto kv_pairs = txn_.scan(start_key, Bound::Included, range.end, Bound::Excluded, actual_limit);
      for (auto& kv_pair : kv_pairs) {
        KeyValue kv;
        kv.key = std::move(kv_pair.key);
        kv.value = std::move(kv_pair.value);
        kvs.push_back(std::move(kv));
      }

      if (kvs.size() >= limit || kv_pairs.size() < actual_limit) return Status::OK();

      if (!kvs.empty()) start_key = Helper::PrefixNext(kvs.back().key);

    } catch (const std::exception& e) {
      if (IsOutOfRange(e.what())) {
        LOG(WARNING) << fmt::format("[storage.{}] scan limit({}) err({}).", txn_id_, actual_limit, e.what());
        actual_limit = std::max(actual_limit / 2, static_cast<uint32_t>(1));
        continue;

      } else if (IsRetryable(e.what())) {
        return Status(pb::error::ESTORE_MAYBE_RETRY, fmt::format("scan err({})", e.what()));

      } else {
        return Status(pb::error::EBACKEND_STORE, fmt::format("scan err({})", e.what()));
      }
    }

  } while (true);

  return Status::OK();
}

Status TikvTxn::Scan(const Range& range, uint64_t limit, std::vector<KeyValue>& kvs) {
  return DoScan(range, limit, kvs);
}

Status TikvTxn::Scan(const Range& range, ScanHandlerType handler) {
  std::vector<KeyValue> kvs;
  auto status = DoScan(range, UINT32_MAX, kvs);
  if (!status.ok()) return status;

  for (auto& kv : kvs) {
    if (!handler(kv.key, kv.value)) break;
  }

  return Status::OK();
}

Status TikvTxn::Scan(const Range& range, std::function<bool(KeyValue&)> handler) {
  std::vector<KeyValue> kvs;
  auto status = DoScan(range, UINT32_MAX, kvs);
  if (!status.ok()) return status;

  for (auto& kv : kvs) {
    if (!handler(kv)) break;
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

TikvTxnAsync::TikvTxnAsync(tikv_client::TransactionClient* client, Txn::IsolationLevel isolation_level)
    : client_(client), txn_(client->begin()), isolation_level_(isolation_level) {
  txn_id_ = txn_.id();
}

TikvTxnAsync::~TikvTxnAsync() {
  if (!committed_.load(std::memory_order_relaxed)) Commit();
  client_ = nullptr;
}

int64_t TikvTxnAsync::ID() const { return txn_id_; }

Status TikvTxnAsync::Put(const std::string& key, const std::string& value) {
  struct Param {
    bthread::CountdownEvent event{1};
    std::string error;
  };

  Param param;
  try {
    txn_.put_async(
        key, value,
        [](const std::string* error, void* ctx) {
          CHECK(ctx != nullptr) << "callback context is null.";

          Param* param = reinterpret_cast<Param*>(ctx);
          if (error != nullptr) param->error = *error;

          param->event.signal();
        },
        &param);

    param.event.wait();

  } catch (const std::exception& e) {
    param.error = e.what();
  }

  CHECK(param.error.empty()) << fmt::format("put_async err({}).", param.error);

  return Status::OK();
}

Status TikvTxnAsync::Delete(const std::string& key) {
  struct Param {
    bthread::CountdownEvent event{1};
    std::string error;
  };

  Param param;
  try {
    txn_.remove_async(
        key,
        [](const std::string* error, void* ctx) {
          CHECK(ctx != nullptr) << "callback context is null.";

          Param* param = reinterpret_cast<Param*>(ctx);
          if (error != nullptr) param->error = *error;

          CHECK(param != nullptr) << "callback param is null.";
          param->event.signal();
        },
        &param);

    param.event.wait();

  } catch (const std::exception& e) {
    param.error = e.what();
  }

  CHECK(param.error.empty()) << fmt::format("delete_async err({}).", param.error);

  return Status::OK();
}

Status TikvTxnAsync::Get(const std::string& key, std::string& value) {
  struct Param {
    bthread::CountdownEvent event{1};
    std::string value;
    std::string error;
  };

  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  Param param;

  try {
    txn_.get_async(
        key,
        [](const std::optional<std::string>* value, const std::string* error, void* ctx) {
          CHECK(ctx != nullptr) << "callback context is null.";

          Param* param = reinterpret_cast<Param*>(ctx);
          if (error != nullptr) {
            param->error = fmt::format("get_async err({}).", *error);

          } else if (value != nullptr && value->has_value()) {
            param->value = value->value();
          }

          CHECK(param != nullptr) << "callback param is null.";
          param->event.signal();
        },
        &param);

    param.event.wait();

    value = std::move(param.value);

  } catch (const std::exception& e) {
    param.error = e.what();
  }

  if (!param.error.empty()) {
    if (IsRetryable(param.error)) {
      return Status(pb::error::ESTORE_MAYBE_RETRY, fmt::format("get_async err({})", param.error));
    } else {
      return Status(pb::error::EBACKEND_STORE, fmt::format("get_async err({})", param.error));
    }
  }

  return value.empty() ? Status(pb::error::ENOT_FOUND, "key not found") : Status::OK();
}

Status TikvTxnAsync::BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) {
  struct Param {
    bthread::CountdownEvent event{1};
    std::string error;
    std::vector<KeyValue>& kvs;

    Param(std::vector<KeyValue>& kvs_ref) : kvs(kvs_ref) {}
  };

  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  Param param(kvs);
  try {
    txn_.batch_get_async(
        keys,
        [](const std::vector<tikv_client::KvPair>* pairs, const std::string* error, void* ctx) {
          CHECK(ctx != nullptr) << "callback context is null.";

          Param* param = reinterpret_cast<Param*>(ctx);
          if (error != nullptr) {
            param->error = *error;

          } else if (pairs != nullptr) {
            for (const auto& kv_pair : *pairs) {
              KeyValue kv;
              kv.key = kv_pair.key;
              kv.value = kv_pair.value;
              param->kvs.push_back(std::move(kv));
            }
          }

          CHECK(param != nullptr) << "callback param is null.";
          param->event.signal();
        },
        &param);

    param.event.wait();

  } catch (const std::exception& e) {
    param.error = e.what();
  }

  if (!param.error.empty()) {
    if (IsRetryable(param.error)) {
      return Status(pb::error::ESTORE_MAYBE_RETRY, fmt::format("batchget_async err({})", param.error));
    } else {
      return Status(pb::error::EBACKEND_STORE, fmt::format("batchget_async err({})", param.error));
    }
  }

  return Status::OK();
}

Status TikvTxnAsync::DoScan(const Range& range, uint32_t limit, std::vector<KeyValue>& kvs) {
  struct Param {
    bthread::CountdownEvent event{1};
    std::string error;
    std::vector<KeyValue>& kvs;
    uint32_t count{0};

    Param(std::vector<KeyValue>& kvs_ref) : kvs(kvs_ref) {}
  };

  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  uint32_t actual_limit = std::min(kScanLimit, limit);
  kvs.reserve(actual_limit);

  std::string start_key = range.start;
  do {
    Param param(kvs);
    try {
      LOG_DEBUG << fmt::format("[storage.{}] scan range[{}, {}), start_key: {}, limit: {}", txn_id_,
                               Helper::StringToHex(range.start), Helper::StringToHex(range.end),
                               Helper::StringToHex(start_key), actual_limit);

      txn_.scan_async(
          start_key, Bound::Included, range.end, Bound::Excluded, actual_limit,
          [](const std::vector<tikv_client::KvPair>* pairs, const std::string* error, void* ctx) {
            CHECK(ctx != nullptr) << "callback context is null.";

            Param* param = reinterpret_cast<Param*>(ctx);
            if (error != nullptr) {
              param->error = *error;

            } else if (pairs != nullptr) {
              param->count = pairs->size();
              for (const auto& kv_pair : *pairs) {
                KeyValue kv;
                kv.key = kv_pair.key;
                kv.value = kv_pair.value;
                param->kvs.push_back(std::move(kv));
              }
            }

            CHECK(param != nullptr) << "callback param is null.";
            param->event.signal();
          },
          &param);

      param.event.wait();

    } catch (const std::exception& e) {
      param.error = e.what();
    }

    if (param.error.empty()) {
      if (param.kvs.size() >= limit || param.count < actual_limit) return Status::OK();
      if (!kvs.empty()) start_key = Helper::PrefixNext(kvs.back().key);

    } else {
      if (IsOutOfRange(param.error)) {
        LOG(WARNING) << fmt::format("[storage.{}] scan_async limit({}) err({}).", txn_id_, actual_limit, param.error);
        actual_limit = std::max(actual_limit / 2, static_cast<uint32_t>(1));
        continue;

      } else if (IsRetryable(param.error)) {
        return Status(pb::error::ESTORE_MAYBE_RETRY, fmt::format("scan_async err({})", param.error));

      } else {
        return Status(pb::error::EBACKEND_STORE, fmt::format("scan_async err({})", param.error));
      }
    }

  } while (true);

  return Status::OK();
}

Status TikvTxnAsync::Scan(const Range& range, uint64_t limit, std::vector<KeyValue>& kvs) {
  return DoScan(range, limit, kvs);
}

Status TikvTxnAsync::Scan(const Range& range, ScanHandlerType handler) {
  std::vector<KeyValue> kvs;
  auto status = DoScan(range, UINT32_MAX, kvs);
  if (!status.ok()) return status;

  for (auto& kv : kvs) {
    if (!handler(kv.key, kv.value)) break;
  }

  return Status::OK();
}

Status TikvTxnAsync::Scan(const Range& range, std::function<bool(KeyValue&)> handler) {
  std::vector<KeyValue> kvs;
  auto status = DoScan(range, UINT32_MAX, kvs);
  if (!status.ok()) return status;

  for (auto& kv : kvs) {
    if (!handler(kv)) break;
  }

  return Status::OK();
}

Status TikvTxnAsync::Commit() {
  struct Param {
    bthread::CountdownEvent event{1};
    std::string error;
  };

  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.write_time_us += (utils::TimestampUs() - start_time); });

  Param param;
  try {
    txn_.commit_async(
        [](const std::string* error, void* ctx) {
          CHECK(ctx != nullptr) << "callback context is null.";

          Param* param = reinterpret_cast<Param*>(ctx);
          if (error != nullptr) param->error = *error;

          CHECK(param != nullptr) << "callback param is null.";
          param->event.signal();
        },
        &param);

    param.event.wait();

  } catch (const std::exception& e) {
    param.error = e.what();
  }

  Status status;
  if (!param.error.empty()) {
    if (IsRetryable(param.error)) {
      status = Status(pb::error::ESTORE_MAYBE_RETRY, fmt::format("commit_async err({})", param.error));
    } else {
      status = Status(pb::error::EBACKEND_STORE, fmt::format("commit_async err({})", param.error));
    }

    txn_.rollback();
  }

  committed_.store(true, std::memory_order_relaxed);

  return status;
}

Trace::Txn TikvTxnAsync::GetTrace() {
  txn_trace_.txn_id = ID();
  return txn_trace_;
}

}  // namespace mds
}  // namespace dingofs