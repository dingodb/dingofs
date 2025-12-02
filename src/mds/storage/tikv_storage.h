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

#ifndef DINGOFS_MDS_TIKV_STORAGE_H_
#define DINGOFS_MDS_TIKV_STORAGE_H_

#include <atomic>

#include "mds/storage/storage.h"
#include "tikv/tikv_client.h"

namespace dingofs {
namespace mds {

class TikvStorage : public KVStorage {
 public:
  TikvStorage() = default;
  ~TikvStorage() override = default;

  static KVStorageSPtr New() { return std::make_shared<TikvStorage>(); }

  bool Init(const std::string& addr) override;
  bool Destroy() override;

  Status CreateTable(const std::string& name, const TableOption& option, int64_t& table_id) override;
  Status DropTable(int64_t table_id) override;
  Status DropTable(const Range& range) override;
  Status IsExistTable(const std::string& start_key, const std::string& end_key) override;

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
  tikv_client::TransactionClient* client_{nullptr};
};

class TikvTxn : public Txn {
 public:
  TikvTxn(tikv_client::TransactionClient* client, Txn::IsolationLevel isolation_level);
  ~TikvTxn() override;

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

 private:
  int64_t txn_id_{0};

  tikv_client::TransactionClient* client_{nullptr};
  tikv_client::Transaction txn_;

  Txn::IsolationLevel isolation_level_;
  Trace::Txn txn_trace_;

  std::atomic<bool> committed_{false};
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_TIKV_STORAGE_H_