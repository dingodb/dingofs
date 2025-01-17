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

#ifndef DINGOFS_MDV2_DINGODB_STORAGE_H_
#define DINGOFS_MDV2_DINGODB_STORAGE_H_

#include <memory>

#include "dingosdk/client.h"
#include "mdsv2/common/status.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {

namespace mdsv2 {

class DingodbStorage : public KVStorage {
 public:
  DingodbStorage() = default;

  ~DingodbStorage() override = default;

  static KVStoragePtr New() { return std::make_shared<DingodbStorage>(); }

  bool Init(const std::string& addr) override;
  bool Destroy() override;

  Status CreateTable(const std::string& name, const TableOption& option, int64_t& table_id) override;
  Status DropTable(int64_t table_id) override;
  Status IsExistTable(const std::string& start_key, const std::string& end_key) override;

  Status Put(WriteOption option, const std::string& key, const std::string& value) override;
  Status Put(WriteOption option, KeyValue& kv) override;
  Status Put(WriteOption option, const std::vector<KeyValue>& kvs) override;

  Status Get(const std::string& key, std::string& value) override;

  Status Scan(const Range& range, std::vector<KeyValue>& kvs) override;

  Status Delete(const std::string& key) override;
  Status Delete(const std::vector<std::string>& keys) override;

 private:
  using TxnPtr = std::unique_ptr<dingodb::sdk::Transaction>;

  TxnPtr NewTxn();

  dingodb::sdk::Client* client_{nullptr};
};

}  // namespace mdsv2

}  // namespace dingofs

#endif  // DINGOFS_MDV2_DINGODB_STORAGE_H_