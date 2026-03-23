// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "mds/storage/tikv_go_storage.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "fmt/core.h"
#include "glog/logging.h"
#include "mds/common/helper.h"
#include "mds/common/tracing.h"
#include "utils/time.h"

// CGO-generated header (produced by `go build -buildmode=c-archive`).
// The path is added to include dirs by CMake after tikvgo_build runs.
#include "tikv_go.h"

namespace dingofs {
namespace mds {

// ---------------------------------------------------------------------------
// Callback stubs – invoked by go goroutines on arbitrary pthreads.
// ---------------------------------------------------------------------------

void TikvGoTxn::OnAsyncComplete(size_t ctx_ptr, void* result) {
  auto* ctx = reinterpret_cast<AsyncContext*>(ctx_ptr);
  ctx->result = result;
  ctx->sem.Release(1);
}

void TikvGoTxn::OnAsyncKVComplete(size_t ctx_ptr, void* result) {
  auto* ctx = reinterpret_cast<AsyncContext*>(ctx_ptr);
  ctx->kv_result = result;
  ctx->sem.Release(1);
}

// ---------------------------------------------------------------------------
// Result parsing helpers
// ---------------------------------------------------------------------------

static Status ParseAsyncResult(void* vr, std::string* out_value = nullptr) {
  auto* res = static_cast<CAsyncResult*>(vr);
  if (res == nullptr) {
    return Status(pb::error::EBACKEND_STORE, "null result from go bridge");
  }

  if (res->error != nullptr && res->error_len > 0) {
    std::string err_msg(res->error, res->error_len);
    if (err_msg.find("retryable:") != std::string::npos) {
      return Status(pb::error::ESTORE_MAYBE_RETRY, fmt::format("tikv-go err({})", err_msg));
    }
    return Status(pb::error::EBACKEND_STORE, fmt::format("tikv-go err({})", err_msg));
  }

  if (out_value != nullptr) {
    if (res->data == nullptr || res->data_len == 0) {
      // key not found.
      return Status(pb::error::ENOT_FOUND, "key not found");
    }
    out_value->assign(res->data, res->data_len);
  }

  return Status::OK();
}

static Status ParseAsyncKVResult(void* vr, std::vector<KeyValue>& kvs) {
  auto* res = static_cast<CAsyncKVResult*>(vr);
  if (res == nullptr) {
    return Status(pb::error::EBACKEND_STORE, "null kv result from go bridge");
  }

  if (res->error != nullptr && res->error_len > 0) {
    std::string err_msg(res->error, res->error_len);
    return Status(pb::error::EBACKEND_STORE, fmt::format("tikv-go err({})", err_msg));
  }

  int n = res->count;
  kvs.reserve(kvs.size() + n);
  for (int i = 0; i < n; ++i) {
    CKVPair* p = res->pairs + i;

    KeyValue kv;
    kv.key.assign(p->key, p->key_len);
    if (p->value != nullptr && p->value_len > 0) {
      kv.value.assign(p->value, p->value_len);
    }
    kvs.push_back(std::move(kv));
  }

  return Status::OK();
}

// ---------------------------------------------------------------------------
// TikvGoStorage
// ---------------------------------------------------------------------------

bool TikvGoStorage::Init(const std::string& addr) {
  LOG(INFO) << fmt::format("[storage] init tikv-go storage, addr({}).", addr);

  std::vector<std::string> addr_list;
  Helper::SplitString(addr, ',', addr_list);

  // build c array of char* for the go function.
  std::vector<const char*> c_addrs;
  c_addrs.reserve(addr_list.size());
  for (const auto& a : addr_list) {
    c_addrs.push_back(a.c_str());
  }

  char* err_str = nullptr;
  int err_len = 0;
  uint64_t handle =
      tikv_go_client_new(const_cast<char**>(c_addrs.data()), static_cast<int>(c_addrs.size()), &err_str, &err_len);

  if (handle == 0) {
    std::string err_msg = (err_str && err_len > 0) ? std::string(err_str, err_len) : "unknown error";
    if (err_str) tikv_go_free_string(err_str);
    LOG(ERROR) << fmt::format("[storage] tikv-go client_new fail: {}", err_msg);
    return false;
  }
  if (err_str) tikv_go_free_string(err_str);

  client_handle_ = handle;
  LOG(INFO) << fmt::format("[storage] init tikv-go storage end, addr({}).", addr);

  return true;
}

bool TikvGoStorage::Destroy() {
  if (client_handle_ != 0) {
    tikv_go_client_destroy(static_cast<GoUint64>(client_handle_));
    client_handle_ = 0;
  }

  return true;
}

Status TikvGoStorage::Put(WriteOption /*option*/, const std::string& key, const std::string& value) {
  auto txn = TikvGoTxn(client_handle_, Txn::kSnapshotIsolation);
  txn.Put(key, value);

  return txn.Commit();
}

Status TikvGoStorage::Put(WriteOption option, KeyValue& kv) { return Put(option, kv.key, kv.value); }

Status TikvGoStorage::Put(WriteOption /*option*/, const std::vector<KeyValue>& kvs) {
  auto txn = TikvGoTxn(client_handle_, Txn::kSnapshotIsolation);
  for (const auto& kv : kvs) {
    txn.Put(kv.key, kv.value);
  }

  return txn.Commit();
}

Status TikvGoStorage::Get(const std::string& key, std::string& value) {
  auto txn = TikvGoTxn(client_handle_, Txn::kSnapshotIsolation);
  auto status = txn.Get(key, value);

  {
    auto status = txn.Commit();
    CHECK(status.ok()) << fmt::format("unexpected commit fail: {}", status.error_str());
  }

  return status;
}

Status TikvGoStorage::BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) {
  auto txn = TikvGoTxn(client_handle_, Txn::kSnapshotIsolation);

  auto status = txn.BatchGet(keys, kvs);
  {
    auto status = txn.Commit();
    CHECK(status.ok()) << fmt::format("unexpected commit fail: {}", status.error_str());
  }

  return status;
}

Status TikvGoStorage::Scan(const Range& range, std::vector<KeyValue>& kvs) {
  auto txn = TikvGoTxn(client_handle_, Txn::kSnapshotIsolation);
  auto status = txn.Scan(range, UINT64_MAX, kvs);

  txn.Commit();

  return status;
}

Status TikvGoStorage::Delete(const std::string& key) {
  auto txn = TikvGoTxn(client_handle_, Txn::kSnapshotIsolation);
  txn.Delete(key);

  return txn.Commit();
}

Status TikvGoStorage::Delete(const std::vector<std::string>& keys) {
  auto txn = TikvGoTxn(client_handle_, Txn::kSnapshotIsolation);
  for (const auto& key : keys) {
    txn.Delete(key);
  }

  return txn.Commit();
}

TxnUPtr TikvGoStorage::NewTxn(Txn::IsolationLevel isolation_level) {
  return std::make_unique<TikvGoTxn>(client_handle_, isolation_level);
}

// ---------------------------------------------------------------------------
// TikvGoTxn
// ---------------------------------------------------------------------------

TikvGoTxn::TikvGoTxn(uint64_t client_handle, Txn::IsolationLevel isolation_level) : isolation_level_(isolation_level) {
  char* err_str = nullptr;
  int err_len = 0;
  uint64_t handle =
      tikv_go_txn_begin(static_cast<GoUint64>(client_handle), static_cast<int>(isolation_level), &err_str, &err_len);
  if (handle == 0) {
    std::string err_msg = (err_str && err_len > 0) ? std::string(err_str, err_len) : "unknown";
    if (err_str) tikv_go_free_string(err_str);
    LOG(ERROR) << fmt::format("[tikv-go] txn_begin fail: {}", err_msg);
    // txn_handle_ stays 0; all subsequent ops will return errors.
    return;
  }

  if (err_str) tikv_go_free_string(err_str);

  txn_handle_ = handle;
  txn_id_ = static_cast<int64_t>(tikv_go_txn_id(static_cast<GoUint64>(txn_handle_)));
}

TikvGoTxn::~TikvGoTxn() {
  if (!committed_.load(std::memory_order_relaxed) && txn_handle_ != 0) {
    Commit();
  }

  if (txn_handle_ != 0) {
    tikv_go_txn_destroy(static_cast<GoUint64>(txn_handle_));
    txn_handle_ = 0;
  }
}

int64_t TikvGoTxn::ID() const { return txn_id_; }

Status TikvGoTxn::Put(const std::string& key, const std::string& value) {
  CHECK(txn_handle_ != 0) << "txn not initialized";

  AsyncContext ctx;
  tikv_go_txn_put_async(static_cast<GoUint64>(txn_handle_), const_cast<char*>(key.data()), static_cast<int>(key.size()),
                        const_cast<char*>(value.data()), static_cast<int>(value.size()),
                        reinterpret_cast<GoUintptr>(OnAsyncComplete), reinterpret_cast<GoUintptr>(&ctx));
  ctx.sem.Acquire();

  Status status = ParseAsyncResult(ctx.result);
  tikv_go_free_async_result(static_cast<CAsyncResult*>(ctx.result));

  return status;
}

Status TikvGoTxn::PutIfAbsent(const std::string& /*key*/, const std::string& /*value*/) {
  return Status(pb::error::ENOT_SUPPORT, "not support yet");
}

Status TikvGoTxn::Delete(const std::string& key) {
  CHECK(txn_handle_ != 0) << "txn not initialized";

  AsyncContext ctx;
  tikv_go_txn_delete_async(static_cast<GoUint64>(txn_handle_), const_cast<char*>(key.data()),
                           static_cast<int>(key.size()), reinterpret_cast<GoUintptr>(OnAsyncComplete),
                           reinterpret_cast<GoUintptr>(&ctx));
  ctx.sem.Acquire();

  Status status = ParseAsyncResult(ctx.result);
  tikv_go_free_async_result(static_cast<CAsyncResult*>(ctx.result));

  return status;
}

Status TikvGoTxn::Get(const std::string& key, std::string& value) {
  CHECK(txn_handle_ != 0) << "txn not initialized";

  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  AsyncContext ctx;
  tikv_go_txn_get_async(static_cast<GoUint64>(txn_handle_), const_cast<char*>(key.data()), static_cast<int>(key.size()),
                        reinterpret_cast<GoUintptr>(OnAsyncComplete), reinterpret_cast<GoUintptr>(&ctx));
  ctx.sem.Acquire();

  Status status = ParseAsyncResult(ctx.result, &value);
  tikv_go_free_async_result(static_cast<CAsyncResult*>(ctx.result));

  return status;
}

Status TikvGoTxn::BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) {
  CHECK(txn_handle_ != 0) << "txn not initialized";

  if (keys.empty()) return Status::OK();

  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  // Build C arrays.
  std::vector<const char*> c_keys(keys.size());
  std::vector<int> c_lens(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    c_keys[i] = keys[i].data();
    c_lens[i] = static_cast<int>(keys[i].size());
  }

  AsyncContext ctx;
  tikv_go_txn_batch_get_async(static_cast<GoUint64>(txn_handle_), const_cast<char**>(c_keys.data()), c_lens.data(),
                              static_cast<int>(keys.size()), reinterpret_cast<GoUintptr>(OnAsyncKVComplete),
                              reinterpret_cast<GoUintptr>(&ctx));
  ctx.sem.Acquire();

  Status status = ParseAsyncKVResult(ctx.kv_result, kvs);
  tikv_go_free_kv_result(static_cast<CAsyncKVResult*>(ctx.kv_result));

  return status;
}

Status TikvGoTxn::DoScan(const Range& range, uint64_t limit,
                         std::function<bool(const std::string&, const std::string&)> handler) {
  CHECK(txn_handle_ != 0) << "txn not initialized";

  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.read_time_us += (utils::TimestampUs() - start_time); });

  AsyncContext ctx;
  tikv_go_txn_scan_async(static_cast<GoUint64>(txn_handle_), const_cast<char*>(range.start.data()),
                         static_cast<int>(range.start.size()), const_cast<char*>(range.end.data()),
                         static_cast<int>(range.end.size()), static_cast<GoUint64>(limit),
                         reinterpret_cast<GoUintptr>(OnAsyncKVComplete), reinterpret_cast<GoUintptr>(&ctx));
  ctx.sem.Acquire();

  CAsyncKVResult* kv_result = static_cast<CAsyncKVResult*>(ctx.kv_result);
  if (kv_result == nullptr) {
    return Status(pb::error::EBACKEND_STORE, "kv result is null");
  }
  if (kv_result->error != nullptr && kv_result->error_len > 0) {
    std::string err_msg(kv_result->error, kv_result->error_len);
    tikv_go_free_kv_result(kv_result);
    return Status(pb::error::EBACKEND_STORE, fmt::format("tikv-go scan err({})", err_msg));
  }

  for (int i = 0; i < kv_result->count; ++i) {
    CKVPair* p = kv_result->pairs + i;
    std::string k(p->key, p->key_len);
    std::string v = (p->value && p->value_len > 0) ? std::string(p->value, p->value_len) : std::string();
    if (!handler(k, v)) break;
  }

  tikv_go_free_kv_result(kv_result);

  return Status::OK();
}

Status TikvGoTxn::Scan(const Range& range, uint64_t limit, std::vector<KeyValue>& kvs) {
  return DoScan(range, limit, [&](const std::string& k, const std::string& v) {
    KeyValue kv;
    kv.key = k;
    kv.value = v;
    kvs.push_back(std::move(kv));
    return true;
  });
}

Status TikvGoTxn::Scan(const Range& range, ScanHandlerType handler) {
  return DoScan(range, 0, [&](const std::string& k, const std::string& v) { return handler(k, v); });
}

Status TikvGoTxn::Scan(const Range& range, std::function<bool(KeyValue&)> handler) {
  return DoScan(range, 0, [&](const std::string& k, const std::string& v) {
    KeyValue kv;
    kv.key = k;
    kv.value = v;
    return handler(kv);
  });
}

Status TikvGoTxn::Commit() {
  CHECK(txn_handle_ != 0) << "txn not initialized";

  uint64_t start_time = utils::TimestampUs();
  ON_SCOPE_EXIT([&]() { txn_trace_.write_time_us += (utils::TimestampUs() - start_time); });

  AsyncContext ctx;
  tikv_go_txn_commit_async(static_cast<GoUint64>(txn_handle_), reinterpret_cast<GoUintptr>(OnAsyncComplete),
                           reinterpret_cast<GoUintptr>(&ctx));
  ctx.sem.Acquire();

  Status status = ParseAsyncResult(ctx.result);
  tikv_go_free_async_result(static_cast<CAsyncResult*>(ctx.result));

  if (!status.ok()) {
    // Async rollback on commit failure.
    Rollback();
  }

  committed_.store(true, std::memory_order_relaxed);

  return status;
}

void TikvGoTxn::Rollback() {  // NOLINT
  CHECK(txn_handle_ != 0) << "txn not initialized";

  AsyncContext rb_ctx;
  tikv_go_txn_rollback_async(static_cast<GoUint64>(txn_handle_), reinterpret_cast<GoUintptr>(OnAsyncComplete),
                             reinterpret_cast<GoUintptr>(&rb_ctx));
  rb_ctx.sem.Acquire();
  tikv_go_free_async_result(static_cast<CAsyncResult*>(rb_ctx.result));
}

Trace::Txn TikvGoTxn::GetTrace() {
  txn_trace_.txn_id = ID();
  return txn_trace_;
}

}  // namespace mds
}  // namespace dingofs
