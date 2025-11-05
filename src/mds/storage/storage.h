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

#ifndef DINGOFS_MDS_STORAGE_H_
#define DINGOFS_MDS_STORAGE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "mds/common/status.h"
#include "mds/common/tracing.h"
#include "mds/common/type.h"

namespace dingofs {
namespace mds {

struct KeyValue {
  enum class OpType : uint8_t {
    kPut = 0,
    kDelete = 1,
  };

  static std::string OpTypeName(OpType op_type) {
    switch (op_type) {
      case OpType::kPut:
        return "Put";
      case OpType::kDelete:
        return "Delete";
      default:
        return "Unknown";
    }
  }

  OpType opt_type{OpType::kPut};
  std::string key;
  std::string value;
};

class Txn;
using TxnPtr = std::shared_ptr<Txn>;
using TxnUPtr = std::unique_ptr<Txn>;

class Txn {
 public:
  enum IsolationLevel : uint8_t {
    kReadCommitted = 0,
    kSnapshotIsolation = 1,
  };

  virtual ~Txn() = default;

  virtual int64_t ID() const = 0;
  virtual Status Put(const std::string& key, const std::string& value) = 0;
  virtual Status PutIfAbsent(const std::string& key, const std::string& value) = 0;
  virtual Status Delete(const std::string& key) = 0;

  virtual Status Get(const std::string& key, std::string& value) = 0;
  virtual Status BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) = 0;
  virtual Status Scan(const Range& range, uint64_t limit, std::vector<KeyValue>& kvs) = 0;

  using ScanHandlerType = std::function<bool(const std::string& key, const std::string& value)>;
  virtual Status Scan(const Range& range, ScanHandlerType handler) = 0;

  virtual Status Scan(const Range& range, std::function<bool(KeyValue&)> handler) = 0;

  virtual Status Commit() = 0;

  virtual Trace::Txn GetTrace() = 0;
};

class KVStorage {
 public:
  struct TableOption {
    std::string start_key;
    std::string end_key;
  };

  struct WriteOption {
    bool is_if_absent{false};
  };

  virtual ~KVStorage() = default;

  virtual bool Init(const std::string& addr) = 0;
  virtual bool Destroy() = 0;

  virtual Status CreateTable(const std::string& name, const TableOption& option, int64_t& table_id) = 0;
  virtual Status DropTable(int64_t table_id) = 0;
  virtual Status DropTable(const Range& range) = 0;
  virtual Status IsExistTable(const std::string& start_key, const std::string& end_key) = 0;

  virtual Status Put(WriteOption option, const std::string& key, const std::string& value) = 0;
  virtual Status Put(WriteOption option, KeyValue& kv) = 0;
  virtual Status Put(WriteOption option, const std::vector<KeyValue>& kvs) = 0;

  virtual Status Get(const std::string& key, std::string& value) = 0;
  virtual Status BatchGet(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs) = 0;

  virtual Status Scan(const Range& range, std::vector<KeyValue>& kvs) = 0;

  virtual Status Delete(const std::string& key) = 0;
  virtual Status Delete(const std::vector<std::string>& keys) = 0;

  virtual TxnUPtr NewTxn(Txn::IsolationLevel isolation_level = Txn::kSnapshotIsolation) = 0;
};
using KVStorageSPtr = std::shared_ptr<KVStorage>;

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_STORAGE_H_