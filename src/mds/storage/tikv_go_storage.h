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

#ifndef DINGOFS_MDS_TIKV_GO_STORAGE_H_
#define DINGOFS_MDS_TIKV_GO_STORAGE_H_

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "mds/common/synchronization.h"
#include "mds/storage/storage.h"

namespace dingofs {
namespace mds {

// ---------------------------------------------------------------------------
// AsyncContext – lives on the calling bthread's stack.
// result/kv_result are typed void* to avoid depending on cgo-generated
// libtikvgo.h at header inclusion time; cast in the .cc implementation.
// ---------------------------------------------------------------------------
struct AsyncContext {
  BthreadSemaphore sem{0};
  void* result{nullptr};     // CAsyncResult*
  void* kv_result{nullptr};  // CAsyncKVResult*
};

// ---------------------------------------------------------------------------
// TikvGoStorage – KVStorage implementation backed by the Go TiKV client.
// ---------------------------------------------------------------------------
class TikvGoStorage : public KVStorage {
 public:
  TikvGoStorage() = default;
  ~TikvGoStorage() override = default;

  static KVStorageSPtr New() { return std::make_shared<TikvGoStorage>(); }

  bool Init(const std::string& addr) override;
  bool Destroy() override;

  Status CreateTable(const std::string&, const TableOption&, int64_t&) override { return Status::OK(); }
  Status DropTable(int64_t) override { return Status::OK(); }
  Status DropTable(const Range&) override { return Status::OK(); }
  Status IsExistTable(const std::string&, const std::string&) override { return Status::OK(); }

  Status Put(WriteOption option, const std::string& key, const std::string& value) override;
  Status Put(WriteOption option, KeyValue& kv) override;
  Status Put(WriteOption option, const std::vector<KeyValue>& kvs) override;

  Status Get(const std::string& key, std::string& value) override;
  Status BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) override;

  Status Scan(const Range& range, std::vector<KeyValue>& kvs) override;

  Status Delete(const std::string& key) override;
  Status Delete(const std::vector<std::string>& keys) override;

  TxnUPtr NewTxn(Txn::IsolationLevel isolation_level = Txn::kSnapshotIsolation) override;

 private:
  uint64_t client_handle_{0};
};

// ---------------------------------------------------------------------------
// TikvGoTxn – Txn implementation using async CGO calls + butex waiting.
// ---------------------------------------------------------------------------
class TikvGoTxn : public Txn {
 public:
  TikvGoTxn(uint64_t client_handle, Txn::IsolationLevel isolation_level);
  ~TikvGoTxn() override;

  int64_t ID() const override;
  Status Put(const std::string& key, const std::string& value) override;
  Status PutIfAbsent(const std::string& key, const std::string& value) override;
  Status Delete(const std::string& key) override;

  Status Get(const std::string& key, std::string& value) override;
  Status BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) override;
  Status Scan(const Range& range, uint64_t limit, std::vector<KeyValue>& kvs) override;
  Status Scan(const Range& range, ScanHandlerType handler) override;
  Status Scan(const Range& range, std::function<bool(KeyValue&)> handler) override;

  Status Commit() override;

  Trace::Txn GetTrace() override;

  // C callbacks – called by Go goroutines from arbitrary pthreads.
  // Signatures use void* to avoid pulling in libtikvgo.h from this header;
  // the .cc implementation casts to the proper cgo types.
  static void OnAsyncComplete(size_t ctx_ptr, void* result);
  static void OnAsyncKVComplete(size_t ctx_ptr, void* result);

 private:
  // Shared helper: run a scan and feed results through a generic handler.
  Status DoScan(const Range& range, uint64_t limit,
                std::function<bool(const std::string&, const std::string&)> handler);
  void Rollback();

  uint64_t txn_handle_{0};
  int64_t txn_id_{0};
  Txn::IsolationLevel isolation_level_;
  Trace::Txn txn_trace_;
  std::atomic<bool> committed_{false};
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_TIKV_GO_STORAGE_H_
