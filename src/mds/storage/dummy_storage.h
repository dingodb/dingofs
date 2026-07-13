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

#ifndef DINGOFS_MDS_DUMMY_STORAGE_H_
#define DINGOFS_MDS_DUMMY_STORAGE_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "absl/container/btree_map.h"
#include "mds/storage/storage.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mds {

class DummyStorage : public KVStorage {
 public:
  DummyStorage() = default;
  ~DummyStorage() override = default;

  static KVStorageSPtr New() { return std::make_shared<DummyStorage>(); }

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

  Status Gc(uint32_t seconds) override { return Status::OK(); }  // NOLINT

  TxnUPtr NewTxn(Txn::IsolationLevel isolation_level = Txn::kSnapshotIsolation) override;

 private:
  friend class DummyTxn;

  // Atomically verifies that none of `if_absent_keys` already exist, then
  // applies `writes` (last-write-wins per key). Used by DummyTxn::Commit to
  // make PutIfAbsent semantics race-free against concurrent committers.
  Status ApplyTxn(const std::map<std::string, KeyValue>& writes, const std::set<std::string>& if_absent_keys);

  struct Table {
    std::string name;
    std::string start_key;
    std::string end_key;
  };

  utils::RWLock lock_;

  int64_t next_table_id_{0};
  std::map<int64_t, Table> tables_;

  absl::btree_map<std::string, std::string> data_;
};

class DummyTxn : public Txn {
 public:
  DummyTxn(DummyStorage* storage, Txn::IsolationLevel isolation_level);
  ~DummyTxn() override = default;

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
  DummyStorage* storage_{nullptr};

  Txn::IsolationLevel isolation_level_;

  // key -> latest staged op (last-write-wins). Map is sorted to make Scan
  // merging cheap and deterministic.
  std::map<std::string, KeyValue> stage_writes_;

  // Subset of stage_writes_ keys created via PutIfAbsent. Their absence in
  // storage must be re-verified atomically at Commit time.
  std::set<std::string> if_absent_keys_;

  bool committed_{false};
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_DUMMY_STORAGE_H_