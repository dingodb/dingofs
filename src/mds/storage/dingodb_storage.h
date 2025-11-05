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

#ifndef DINGOFS_MDS_DINGODB_STORAGE_H_
#define DINGOFS_MDS_DINGODB_STORAGE_H_

#include <memory>

#include "dingosdk/client.h"
#include "mds/common/status.h"
#include "mds/storage/storage.h"

namespace dingofs {
namespace mds {

class DingodbStorage : public KVStorage {
 public:
  DingodbStorage() = default;
  ~DingodbStorage() override = default;

  static KVStorageSPtr New() { return std::make_shared<DingodbStorage>(); }

  bool Init(const std::string& addr) override;
  bool Destroy() override;

  static std::vector<std::pair<std::string, std::string>> GetSdkVersion();

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
  using SdkTxnUPtr = std::unique_ptr<dingodb::sdk::Transaction>;

  SdkTxnUPtr NewSdkTxn(Txn::IsolationLevel isolation_level = Txn::kSnapshotIsolation);

  dingodb::sdk::Client* client_{nullptr};
};

class DingodbTxn : public Txn {
 public:
  using SdkTxnUPtr = std::unique_ptr<dingodb::sdk::Transaction>;

  DingodbTxn(SdkTxnUPtr txn) : txn_(std::move(txn)) {};
  ~DingodbTxn() override = default;

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
  using TxnUPtr = std::unique_ptr<dingodb::sdk::Transaction>;

  Status TransformStatus(const dingodb::sdk::Status& status);

  void Rollback();

  SdkTxnUPtr txn_;
  Trace::Txn txn_trace_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_DINGODB_STORAGE_H_