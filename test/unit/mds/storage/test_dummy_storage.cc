// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#include "mds/storage/dummy_storage.h"

#include <algorithm>
#include <atomic>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "dingofs/error.pb.h"
#include "gtest/gtest.h"
#include "mds/storage/storage.h"

namespace dingofs {
namespace mds {
namespace unit_test {

namespace {

KeyValue MakePut(const std::string& k, const std::string& v) {
  return KeyValue{KeyValue::OpType::kPut, k, v};
}

KeyValue MakeDelete(const std::string& k) {
  return KeyValue{KeyValue::OpType::kDelete, k, ""};
}

}  // namespace

class DummyStorageTest : public testing::Test {
 protected:
  void SetUp() override { storage_ = DummyStorage::New(); }

  KVStorageSPtr storage_;
};

// ---------------------------------------------------------------------------
// KVStorage direct API
// ---------------------------------------------------------------------------

TEST_F(DummyStorageTest, PutGet) {
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "k1", "v1").ok());

  std::string value;
  ASSERT_TRUE(storage_->Get("k1", value).ok());
  EXPECT_EQ(value, "v1");
}

TEST_F(DummyStorageTest, GetNotFound) {
  std::string value;
  auto status = storage_->Get("missing", value);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_FOUND);
}

TEST_F(DummyStorageTest, PutOverwrite) {
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "k1", "v1").ok());
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "k1", "v2").ok());

  std::string value;
  ASSERT_TRUE(storage_->Get("k1", value).ok());
  EXPECT_EQ(value, "v2");
}

TEST_F(DummyStorageTest, PutIfAbsent) {
  KVStorage::WriteOption opt;
  opt.is_if_absent = true;

  ASSERT_TRUE(storage_->Put(opt, "k1", "v1").ok());

  auto status = storage_->Put(opt, "k1", "v2");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::EEXISTED);

  std::string value;
  ASSERT_TRUE(storage_->Get("k1", value).ok());
  EXPECT_EQ(value, "v1");
}

TEST_F(DummyStorageTest, PutKeyValueDeleteIgnoresIfAbsent) {
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "k1", "v1").ok());

  KVStorage::WriteOption opt;
  opt.is_if_absent = true;

  // Delete with is_if_absent should not return EEXISTED even though k1 exists.
  KeyValue kv = MakeDelete("k1");
  auto status = storage_->Put(opt, kv);
  ASSERT_TRUE(status.ok()) << status.error_str();

  std::string value;
  EXPECT_EQ(storage_->Get("k1", value).error_code(), pb::error::ENOT_FOUND);
}

TEST_F(DummyStorageTest, PutBatchMixedDelete) {
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "k1", "old").ok());

  std::vector<KeyValue> batch = {MakePut("k1", "new"), MakePut("k2", "v2"),
                                 MakeDelete("k3")};
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), batch).ok());

  std::string v;
  ASSERT_TRUE(storage_->Get("k1", v).ok());
  EXPECT_EQ(v, "new");
  ASSERT_TRUE(storage_->Get("k2", v).ok());
  EXPECT_EQ(v, "v2");
}

TEST_F(DummyStorageTest, PutBatchIfAbsentSkipsDeletes) {
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "existing", "v").ok());

  KVStorage::WriteOption opt;
  opt.is_if_absent = true;

  // Delete on an existing key must not block batch (is_if_absent only applies
  // to puts).
  std::vector<KeyValue> batch = {MakeDelete("existing"), MakePut("k_new", "v")};
  ASSERT_TRUE(storage_->Put(opt, batch).ok());

  std::string v;
  EXPECT_EQ(storage_->Get("existing", v).error_code(), pb::error::ENOT_FOUND);
  ASSERT_TRUE(storage_->Get("k_new", v).ok());
}

TEST_F(DummyStorageTest, BatchGetPartialMiss) {
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "k1", "v1").ok());
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "k3", "v3").ok());

  std::vector<KeyValue> kvs;
  ASSERT_TRUE(storage_->BatchGet({"k1", "k2", "k3"}, kvs).ok());
  ASSERT_EQ(kvs.size(), 2u);

  std::set<std::string> keys{kvs[0].key, kvs[1].key};
  EXPECT_EQ(keys, (std::set<std::string>{"k1", "k3"}));
}

TEST_F(DummyStorageTest, ScanRange) {
  for (const auto& k : {"a", "b", "c", "d", "e"}) {
    ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), k, "v").ok());
  }

  std::vector<KeyValue> kvs;
  ASSERT_TRUE(storage_->Scan(Range{"b", "d"}, kvs).ok());
  ASSERT_EQ(kvs.size(), 2u);
  EXPECT_EQ(kvs[0].key, "b");
  EXPECT_EQ(kvs[1].key, "c");
}

TEST_F(DummyStorageTest, DeleteSingleAndBatch) {
  for (const auto& k : {"k1", "k2", "k3"}) {
    ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), k, "v").ok());
  }

  ASSERT_TRUE(storage_->Delete("k1").ok());
  ASSERT_TRUE(storage_->Delete(std::vector<std::string>{"k2", "k3"}).ok());

  std::string v;
  EXPECT_EQ(storage_->Get("k1", v).error_code(), pb::error::ENOT_FOUND);
  EXPECT_EQ(storage_->Get("k2", v).error_code(), pb::error::ENOT_FOUND);
  EXPECT_EQ(storage_->Get("k3", v).error_code(), pb::error::ENOT_FOUND);
}

TEST_F(DummyStorageTest, DropTableByIdRemovesData) {
  KVStorage::TableOption opt;
  opt.start_key = "t/";
  opt.end_key = "t0";  // ASCII '0' > '/', covers "t/..."

  int64_t table_id = 0;
  ASSERT_TRUE(storage_->CreateTable("t", opt, table_id).ok());

  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "t/a", "1").ok());
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "t/b", "2").ok());
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "u/x", "3").ok());

  ASSERT_TRUE(storage_->DropTable(table_id).ok());

  std::string v;
  EXPECT_EQ(storage_->Get("t/a", v).error_code(), pb::error::ENOT_FOUND);
  EXPECT_EQ(storage_->Get("t/b", v).error_code(), pb::error::ENOT_FOUND);
  ASSERT_TRUE(storage_->Get("u/x", v).ok()) << "unrelated key must survive";
  EXPECT_EQ(v, "3");
}

TEST_F(DummyStorageTest, DropTableByRangeRemovesData) {
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "p/1", "a").ok());
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "p/2", "b").ok());
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "q/1", "c").ok());

  ASSERT_TRUE(storage_->DropTable(Range{"p/", "p0"}).ok());

  std::string v;
  EXPECT_EQ(storage_->Get("p/1", v).error_code(), pb::error::ENOT_FOUND);
  EXPECT_EQ(storage_->Get("p/2", v).error_code(), pb::error::ENOT_FOUND);
  ASSERT_TRUE(storage_->Get("q/1", v).ok());
}

// ---------------------------------------------------------------------------
// DummyTxn
// ---------------------------------------------------------------------------

TEST_F(DummyStorageTest, TxnUncommittedWritesNotVisibleToStorage) {
  auto txn = storage_->NewTxn();
  ASSERT_TRUE(txn->Put("k1", "v1").ok());

  std::string v;
  EXPECT_EQ(storage_->Get("k1", v).error_code(), pb::error::ENOT_FOUND);

  ASSERT_TRUE(txn->Commit().ok());
  ASSERT_TRUE(storage_->Get("k1", v).ok());
  EXPECT_EQ(v, "v1");
}

TEST_F(DummyStorageTest, TxnReadYourOwnWrites) {
  auto txn = storage_->NewTxn();
  ASSERT_TRUE(txn->Put("k1", "v1").ok());

  std::string v;
  ASSERT_TRUE(txn->Get("k1", v).ok());
  EXPECT_EQ(v, "v1");

  ASSERT_TRUE(txn->Put("k1", "v2").ok());
  ASSERT_TRUE(txn->Get("k1", v).ok());
  EXPECT_EQ(v, "v2");
}

TEST_F(DummyStorageTest, TxnDeleteThenGet) {
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "k1", "v1").ok());

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(txn->Delete("k1").ok());

  std::string v;
  EXPECT_EQ(txn->Get("k1", v).error_code(), pb::error::ENOT_FOUND);

  // Until commit, storage is unchanged.
  ASSERT_TRUE(storage_->Get("k1", v).ok());
  EXPECT_EQ(v, "v1");

  ASSERT_TRUE(txn->Commit().ok());
  EXPECT_EQ(storage_->Get("k1", v).error_code(), pb::error::ENOT_FOUND);
}

TEST_F(DummyStorageTest, TxnPutIfAbsentStagesAndCommits) {
  auto txn = storage_->NewTxn();
  ASSERT_TRUE(txn->PutIfAbsent("k1", "v1").ok());

  // Not yet visible to storage before commit.
  std::string v;
  EXPECT_EQ(storage_->Get("k1", v).error_code(), pb::error::ENOT_FOUND);

  // But visible inside the txn.
  ASSERT_TRUE(txn->Get("k1", v).ok());
  EXPECT_EQ(v, "v1");

  ASSERT_TRUE(txn->Commit().ok());
  ASSERT_TRUE(storage_->Get("k1", v).ok());
  EXPECT_EQ(v, "v1");
}

TEST_F(DummyStorageTest, TxnPutIfAbsentConflictExistingStorage) {
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "k1", "v1").ok());

  auto txn = storage_->NewTxn();
  auto status = txn->PutIfAbsent("k1", "v2");
  EXPECT_EQ(status.error_code(), pb::error::EEXISTED);
}

TEST_F(DummyStorageTest, TxnPutIfAbsentConflictWithStagedPut) {
  auto txn = storage_->NewTxn();
  ASSERT_TRUE(txn->Put("k1", "v1").ok());

  auto status = txn->PutIfAbsent("k1", "v2");
  EXPECT_EQ(status.error_code(), pb::error::EEXISTED);
}

TEST_F(DummyStorageTest, TxnBatchGetMergesStageAndStorage) {
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "k1", "s1").ok());
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "k2", "s2").ok());

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(txn->Put("k2", "stage2").ok());
  ASSERT_TRUE(txn->Delete("k1").ok());
  ASSERT_TRUE(txn->Put("k3", "stage3").ok());

  std::vector<KeyValue> kvs;
  ASSERT_TRUE(txn->BatchGet({"k1", "k2", "k3", "k4"}, kvs).ok());

  std::map<std::string, std::string> got;
  for (auto& kv : kvs) got[kv.key] = kv.value;

  EXPECT_EQ(got.count("k1"), 0u) << "deleted in stage";
  EXPECT_EQ(got["k2"], "stage2");
  EXPECT_EQ(got["k3"], "stage3");
  EXPECT_EQ(got.count("k4"), 0u);
}

TEST_F(DummyStorageTest, TxnScanMergesStageAndStorage) {
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "a", "sa").ok());
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "b", "sb").ok());
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "d", "sd").ok());

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(txn->Put("c", "stage_c").ok());     // new key
  ASSERT_TRUE(txn->Put("b", "stage_b").ok());     // overwrite
  ASSERT_TRUE(txn->Delete("d").ok());             // hide

  std::vector<KeyValue> kvs;
  ASSERT_TRUE(txn->Scan(Range{"a", "z"}, UINT64_MAX, kvs).ok());

  ASSERT_EQ(kvs.size(), 3u);
  EXPECT_EQ(kvs[0].key, "a");
  EXPECT_EQ(kvs[0].value, "sa");
  EXPECT_EQ(kvs[1].key, "b");
  EXPECT_EQ(kvs[1].value, "stage_b");
  EXPECT_EQ(kvs[2].key, "c");
  EXPECT_EQ(kvs[2].value, "stage_c");
}

TEST_F(DummyStorageTest, TxnScanRespectsLimit) {
  for (const auto& k : {"a", "b", "c", "d"}) {
    ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), k, "v").ok());
  }

  auto txn = storage_->NewTxn();
  std::vector<KeyValue> kvs;
  ASSERT_TRUE(txn->Scan(Range{"a", "z"}, 2, kvs).ok());
  ASSERT_EQ(kvs.size(), 2u);
  EXPECT_EQ(kvs[0].key, "a");
  EXPECT_EQ(kvs[1].key, "b");
}

TEST_F(DummyStorageTest, TxnScanWithHandlerEarlyStop) {
  for (const auto& k : {"a", "b", "c"}) {
    ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), k, "v").ok());
  }

  auto txn = storage_->NewTxn();
  std::vector<std::string> seen;
  ASSERT_TRUE(txn->Scan(Range{"a", "z"},
                        [&](const std::string& k, const std::string&) {
                          seen.push_back(k);
                          return seen.size() < 2;
                        })
                  .ok());
  ASSERT_EQ(seen.size(), 2u);
  EXPECT_EQ(seen[0], "a");
  EXPECT_EQ(seen[1], "b");
}

TEST_F(DummyStorageTest, TxnScanKVHandlerSeesStagedValues) {
  ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), "a", "old").ok());

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(txn->Put("a", "new").ok());

  std::vector<std::string> values;
  ASSERT_TRUE(txn->Scan(Range{"a", "z"},
                        [&](KeyValue& kv) {
                          values.push_back(kv.value);
                          return true;
                        })
                  .ok());
  ASSERT_EQ(values.size(), 1u);
  EXPECT_EQ(values[0], "new");
}

TEST_F(DummyStorageTest, TxnCommitClearsStageAndDoubleCommitFails) {
  auto txn = storage_->NewTxn();
  ASSERT_TRUE(txn->Put("k1", "v1").ok());
  ASSERT_TRUE(txn->Commit().ok());

  // A second commit must not silently re-apply staged writes.
  auto status = txn->Commit();
  EXPECT_FALSE(status.ok());
}

TEST_F(DummyStorageTest, TxnWriteAfterCommitFails) {
  auto txn = storage_->NewTxn();
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_FALSE(txn->Put("k1", "v1").ok());
  EXPECT_FALSE(txn->Delete("k1").ok());
  EXPECT_FALSE(txn->PutIfAbsent("k1", "v1").ok());
}

TEST_F(DummyStorageTest, TxnIdsAreUnique) {
  std::set<int64_t> ids;
  for (int i = 0; i < 100; ++i) {
    auto txn = storage_->NewTxn();
    ASSERT_TRUE(ids.insert(txn->ID()).second) << "duplicate txn id: "
                                              << txn->ID();
  }
}

TEST_F(DummyStorageTest, TxnPutIfAbsentRaceAtCommit) {
  // Two concurrent txns both see key absent and both try PutIfAbsent.
  // Exactly one must win at commit time.
  auto txn1 = storage_->NewTxn();
  auto txn2 = storage_->NewTxn();

  ASSERT_TRUE(txn1->PutIfAbsent("k1", "v1").ok());
  ASSERT_TRUE(txn2->PutIfAbsent("k1", "v2").ok());

  ASSERT_TRUE(txn1->Commit().ok());
  auto status = txn2->Commit();
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::EEXISTED);

  std::string v;
  ASSERT_TRUE(storage_->Get("k1", v).ok());
  EXPECT_EQ(v, "v1");
}

TEST_F(DummyStorageTest, ConcurrentTxnsAllCommit) {
  constexpr int kThreads = 8;
  constexpr int kPerThread = 50;

  std::vector<std::thread> threads;
  std::atomic<int> committed{0};
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([this, t, &committed] {
      for (int i = 0; i < kPerThread; ++i) {
        auto txn = storage_->NewTxn();
        std::string key = "t" + std::to_string(t) + "/k" + std::to_string(i);
        if (txn->Put(key, "v").ok() && txn->Commit().ok()) {
          ++committed;
        }
      }
    });
  }
  for (auto& th : threads) th.join();

  EXPECT_EQ(committed.load(), kThreads * kPerThread);

  std::vector<KeyValue> kvs;
  ASSERT_TRUE(storage_->Scan(Range{"t", "u"}, kvs).ok());
  EXPECT_EQ(kvs.size(),
            static_cast<size_t>(kThreads * kPerThread));
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
