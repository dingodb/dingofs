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
#include "absl/container/flat_hash_map.h"
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
  bool Stop() override { return true; }

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

  // Atomically applies `writes` under the storage lock. Before applying,
  // re-verifies `if_absent_keys` and, for every key the txn both read and is
  // about to write, checks that it did not change since the read. A changed key
  // yields ESTORE_MAYBE_RETRY, mirroring the optimistic concurrency of the
  // production backends: without it a stale read-modify-write silently loses an
  // update (last-write-wins).
  Status ApplyTxn(const std::map<std::string, KeyValue>& writes, const std::set<std::string>& if_absent_keys,
                  const std::map<std::string, uint64_t>& read_versions);

  // Versioned reads: the version is captured atomically with the value so a
  // later commit can tell whether the key moved under the transaction. Absent
  // keys are reported with version 0 (and ENOT_FOUND for the single-key read).
  Status GetWithVersion(const std::string& key, std::string& value, uint64_t& version);
  Status BatchGetWithVersion(const std::vector<std::string>& keys, std::vector<KeyValue>& kvs,
                             std::map<std::string, uint64_t>& versions);
  Status ScanWithVersion(const Range& range, std::vector<KeyValue>& kvs, std::map<std::string, uint64_t>& versions);

  // Caller must hold lock_. Returns 0 for a key that is absent or never written.
  uint64_t VersionNoLock(const std::string& key) const;

  struct Table {
    std::string name;
    std::string start_key;
    std::string end_key;
  };

  utils::RWLock lock_;

  int64_t next_table_id_{0};
  std::map<int64_t, Table> tables_;

  absl::btree_map<std::string, std::string> data_;

  // Last commit version that wrote each key; the basis for write-conflict
  // detection. Entries are never evicted (test-scope storage).
  absl::flat_hash_map<std::string, uint64_t> key_versions_;
  uint64_t commit_version_{0};
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

  // Version observed the last time each key was read from storage. Commit
  // rejects the txn if one of these keys changed before we wrote it.
  std::map<std::string, uint64_t> read_versions_;

  bool committed_{false};

  void RecordReadVersions(const std::map<std::string, uint64_t>& versions);
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_DUMMY_STORAGE_H_