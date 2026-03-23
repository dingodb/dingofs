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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "bthread/bthread.h"
#include "bthread/types.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mds/common/helper.h"
#include "mds/storage/tikv_go_storage.h"

namespace dingofs {
namespace mds {
namespace unit_test {

// skip test case, because tikv server is not ready in test environment, and the
// test case will fail.
class TikvGoStorageTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    storage_ = TikvGoStorage::New();
    ASSERT_TRUE(storage_->Init("10.220.32.42:2379"))
        << "init tikv-go storage fail.";
  }

  static void TearDownTestSuite() { storage_->Destroy(); }

 public:
  static KVStorageSPtr storage_;
};

KVStorageSPtr TikvGoStorageTest::storage_ = nullptr;

TEST_F(TikvGoStorageTest, Put) {
  GTEST_SKIP() << "Skip Put test case.";

  auto txn = storage_->NewTxn();

  for (int i = 0; i < 10; ++i) {
    auto status =
        txn->Put("unit_tikv_go_put_" + Helper::GenerateRandomString(32),
                 "unit_value_" + Helper::GenerateRandomString(64));
    ASSERT_TRUE(status.ok()) << "put fail, error: " << status.error_str();
  }

  auto status = txn->Commit();
  ASSERT_TRUE(status.ok()) << "commit fail, error: " << status.error_str();
}

TEST_F(TikvGoStorageTest, PutGet) {
  GTEST_SKIP() << "Skip PutGet test case.";

  auto txn = storage_->NewTxn();

  std::vector<std::string> keys;
  for (int i = 0; i < 10; ++i) {
    std::string key = "unit_tikv_go_putget_" + Helper::GenerateRandomString(32);
    auto status =
        txn->Put(key, "unit_value_" + Helper::GenerateRandomString(64));
    ASSERT_TRUE(status.ok()) << "put fail, error: " << status.error_str();
    keys.push_back(key);
  }

  for (const auto& key : keys) {
    std::string value;
    auto status = txn->Get(key, value);
    ASSERT_TRUE(status.ok()) << "get key fail, error: " << status.error_str();
    ASSERT_FALSE(value.empty()) << "value is empty.";
  }

  auto status = txn->Commit();
  ASSERT_TRUE(status.ok()) << "commit fail, error: " << status.error_str();
}

TEST_F(TikvGoStorageTest, PutDelete) {
  GTEST_SKIP() << "Skip PutDelete test case.";

  std::string key =
      "unit_tikv_go_putdelete_" + Helper::GenerateRandomString(32);
  std::string value = "unit_value_" + Helper::GenerateRandomString(64);

  {
    auto txn = storage_->NewTxn();
    auto status = txn->Put(key, value);
    ASSERT_TRUE(status.ok()) << "put fail, error: " << status.error_str();
    status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "commit fail, error: " << status.error_str();
  }

  {
    auto txn = storage_->NewTxn();
    auto status = txn->Delete(key);
    ASSERT_TRUE(status.ok()) << "delete fail, error: " << status.error_str();
    status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "commit fail, error: " << status.error_str();
  }

  {
    auto txn = storage_->NewTxn();
    std::string got;
    auto status = txn->Get(key, got);
    ASSERT_FALSE(status.ok()) << "key should not exist after delete.";
    txn->Commit();
  }
}

TEST_F(TikvGoStorageTest, PutBatchGet) {
  GTEST_SKIP() << "Skip PutBatchGet test case.";

  std::vector<std::string> keys;
  {
    auto txn = storage_->NewTxn();

    for (int i = 0; i < 10; ++i) {
      std::string key =
          "unit_tikv_go_batchget_" + Helper::GenerateRandomString(32);
      auto status =
          txn->Put(key, "unit_value_" + Helper::GenerateRandomString(64));
      ASSERT_TRUE(status.ok()) << "put fail, error: " << status.error_str();
      keys.push_back(key);
    }

    auto status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "commit fail, error: " << status.error_str();
  }

  {
    auto txn = storage_->NewTxn();

    std::vector<KeyValue> kvs;
    auto status = txn->BatchGet(keys, kvs);
    ASSERT_TRUE(status.ok()) << "batch get fail, error: " << status.error_str();
    ASSERT_EQ(kvs.size(), keys.size()) << "kvs size not equal.";

    std::sort(keys.begin(), keys.end());  // NOLINT
    std::sort(                            // NOLINT
        kvs.begin(), kvs.end(),
        [](const KeyValue& a, const KeyValue& b) { return a.key < b.key; });
    for (size_t i = 0; i < keys.size(); ++i) {
      ASSERT_EQ(keys[i], kvs[i].key) << "key not equal.";
      ASSERT_FALSE(kvs[i].value.empty()) << "value is empty.";
    }

    txn->Commit();
  }
}

TEST_F(TikvGoStorageTest, Scan) {
  GTEST_SKIP() << "Skip Scan test case.";

  std::string prefix =
      "unit_tikv_go_scan_" + Helper::GenerateRandomString(16) + "_";

  std::vector<std::string> keys;
  {
    auto txn = storage_->NewTxn();
    for (int i = 0; i < 10; ++i) {
      std::string key = prefix + std::to_string(i);
      auto status =
          txn->Put(key, "unit_value_" + Helper::GenerateRandomString(64));
      ASSERT_TRUE(status.ok()) << "put fail, error: " << status.error_str();
      keys.push_back(key);
    }
    auto status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "commit fail, error: " << status.error_str();
  }

  {
    auto txn = storage_->NewTxn();
    Range range;
    range.start = prefix;
    range.end = prefix + "\xFF";

    std::vector<KeyValue> kvs;
    auto status = txn->Scan(range, UINT64_MAX, kvs);
    ASSERT_TRUE(status.ok()) << "scan fail, error: " << status.error_str();
    ASSERT_EQ(kvs.size(), keys.size()) << "scan result size not equal.";
    txn->Commit();
  }
}

TEST_F(TikvGoStorageTest, ScanWithLimit) {
  GTEST_SKIP() << "Skip ScanWithLimit test case.";

  std::string prefix =
      "unit_tikv_go_scanlimit_" + Helper::GenerateRandomString(16) + "_";

  {
    auto txn = storage_->NewTxn();
    for (int i = 0; i < 10; ++i) {
      auto status = txn->Put(prefix + std::to_string(i),
                             "unit_value_" + Helper::GenerateRandomString(64));
      ASSERT_TRUE(status.ok()) << "put fail, error: " << status.error_str();
    }
    auto status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "commit fail, error: " << status.error_str();
  }

  {
    auto txn = storage_->NewTxn();
    Range range;
    range.start = prefix;
    range.end = prefix + "\xFF";

    std::vector<KeyValue> kvs;
    auto status = txn->Scan(range, 5, kvs);
    ASSERT_TRUE(status.ok()) << "scan fail, error: " << status.error_str();
    ASSERT_EQ(kvs.size(), 5) << "scan with limit result size not equal.";
    txn->Commit();
  }
}

TEST_F(TikvGoStorageTest, ScanWithHandler) {
  GTEST_SKIP() << "Skip ScanWithHandler test case.";

  std::string prefix =
      "unit_tikv_go_scanhandler_" + Helper::GenerateRandomString(16) + "_";

  {
    auto txn = storage_->NewTxn();
    for (int i = 0; i < 10; ++i) {
      auto status = txn->Put(prefix + std::to_string(i),
                             "unit_value_" + Helper::GenerateRandomString(64));
      ASSERT_TRUE(status.ok()) << "put fail, error: " << status.error_str();
    }
    auto status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "commit fail, error: " << status.error_str();
  }

  {
    auto txn = storage_->NewTxn();
    Range range;
    range.start = prefix;
    range.end = prefix + "\xFF";

    int count = 0;
    auto status = txn->Scan(
        range,
        [&](const std::string& /*key*/, const std::string& /*value*/) -> bool {
          ++count;
          return true;
        });
    ASSERT_TRUE(status.ok()) << "scan fail, error: " << status.error_str();
    ASSERT_EQ(count, 10) << "scan handler count not equal.";
    txn->Commit();
  }
}

TEST_F(TikvGoStorageTest, StoragePutGet) {
  GTEST_SKIP() << "Skip StoragePutGet test case.";

  std::string key =
      "unit_tikv_go_storage_putget_" + Helper::GenerateRandomString(32);
  std::string value = "unit_value_" + Helper::GenerateRandomString(64);

  KVStorage::WriteOption opt;
  auto status = storage_->Put(opt, key, value);
  ASSERT_TRUE(status.ok()) << "storage put fail, error: " << status.error_str();

  std::string got;
  status = storage_->Get(key, got);
  ASSERT_TRUE(status.ok()) << "storage get fail, error: " << status.error_str();
  ASSERT_EQ(got, value) << "storage get value not equal.";
}

TEST_F(TikvGoStorageTest, StorageDelete) {
  GTEST_SKIP() << "Skip StorageDelete test case.";

  std::string key =
      "unit_tikv_go_storage_delete_" + Helper::GenerateRandomString(32);
  std::string value = "unit_value_" + Helper::GenerateRandomString(64);

  KVStorage::WriteOption opt;
  auto status = storage_->Put(opt, key, value);
  ASSERT_TRUE(status.ok()) << "storage put fail, error: " << status.error_str();

  status = storage_->Delete(key);
  ASSERT_TRUE(status.ok()) << "storage delete fail, error: "
                           << status.error_str();

  std::string got;
  status = storage_->Get(key, got);
  ASSERT_FALSE(status.ok()) << "key should not exist after delete.";
}

TEST_F(TikvGoStorageTest, StorageBatchPut) {
  GTEST_SKIP() << "Skip StorageBatchPut test case.";

  std::vector<KeyValue> kvs;
  for (int i = 0; i < 10; ++i) {
    KeyValue kv;
    kv.key =
        "unit_tikv_go_storage_batchput_" + Helper::GenerateRandomString(32);
    kv.value = "unit_value_" + Helper::GenerateRandomString(64);
    kvs.push_back(std::move(kv));
  }

  KVStorage::WriteOption opt;
  auto status = storage_->Put(opt, kvs);
  ASSERT_TRUE(status.ok()) << "storage batch put fail, error: "
                           << status.error_str();

  std::vector<std::string> keys;
  keys.reserve(kvs.size());
  for (const auto& kv : kvs) {
    keys.push_back(kv.key);
  }

  std::vector<KeyValue> got_kvs;
  status = storage_->BatchGet(keys, got_kvs);
  ASSERT_TRUE(status.ok()) << "storage batch get fail, error: "
                           << status.error_str();
  ASSERT_EQ(got_kvs.size(), kvs.size()) << "batch get size not equal.";
}

TEST_F(TikvGoStorageTest, GetNotFound) {
  GTEST_SKIP() << "Skip GetNotFound test case.";

  auto txn = storage_->NewTxn();
  std::string value;
  auto status = txn->Get(
      "unit_tikv_go_nonexistent_key_" + Helper::GenerateRandomString(32),
      value);
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.error_code(), pb::error::ENOT_FOUND);
  txn->Commit();
}

TEST_F(TikvGoStorageTest, BatchGetEmpty) {
  GTEST_SKIP() << "Skip BatchGetEmpty test case.";

  auto txn = storage_->NewTxn();
  std::vector<std::string> keys;
  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(keys, kvs);
  ASSERT_TRUE(status.ok()) << "batch get empty fail: " << status.error_str();
  ASSERT_TRUE(kvs.empty());
  txn->Commit();
}

TEST_F(TikvGoStorageTest, BatchGetPartialMiss) {
  GTEST_SKIP() << "Skip BatchGetPartialMiss test case.";

  std::string existing_key =
      "unit_tikv_go_partialmiss_" + Helper::GenerateRandomString(32);
  {
    auto txn = storage_->NewTxn();
    auto status = txn->Put(existing_key, "some_value");
    ASSERT_TRUE(status.ok()) << "put fail: " << status.error_str();
    status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "commit fail: " << status.error_str();
  }

  {
    auto txn = storage_->NewTxn();
    std::vector<std::string> keys = {existing_key,
                                     "unit_tikv_go_partialmiss_nonexist_" +
                                         Helper::GenerateRandomString(32)};
    std::vector<KeyValue> kvs;
    auto status = txn->BatchGet(keys, kvs);
    ASSERT_TRUE(status.ok()) << "batch get fail: " << status.error_str();
    ASSERT_EQ(kvs.size(), 1) << "should only get existing key.";
    ASSERT_EQ(kvs[0].key, existing_key);
    txn->Commit();
  }
}

TEST_F(TikvGoStorageTest, ScanWithKVHandler) {
  GTEST_SKIP() << "Skip ScanWithKVHandler test case.";

  std::string prefix =
      "unit_tikv_go_scankvhandler_" + Helper::GenerateRandomString(16) + "_";

  {
    auto txn = storage_->NewTxn();
    for (int i = 0; i < 5; ++i) {
      auto status = txn->Put(prefix + std::to_string(i),
                             "unit_value_" + Helper::GenerateRandomString(64));
      ASSERT_TRUE(status.ok()) << "put fail: " << status.error_str();
    }
    auto status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "commit fail: " << status.error_str();
  }

  {
    auto txn = storage_->NewTxn();
    Range range;
    range.start = prefix;
    range.end = prefix + "\xFF";

    std::vector<KeyValue> collected;
    auto status = txn->Scan(range, [&](KeyValue& kv) -> bool {
      collected.push_back(std::move(kv));
      return true;
    });
    ASSERT_TRUE(status.ok()) << "scan fail: " << status.error_str();
    ASSERT_EQ(collected.size(), 5);
    txn->Commit();
  }
}

TEST_F(TikvGoStorageTest, ScanHandlerEarlyStop) {
  GTEST_SKIP() << "Skip ScanHandlerEarlyStop test case.";

  std::string prefix =
      "unit_tikv_go_earlystop_" + Helper::GenerateRandomString(16) + "_";

  {
    auto txn = storage_->NewTxn();
    for (int i = 0; i < 10; ++i) {
      auto status = txn->Put(prefix + std::to_string(i),
                             "unit_value_" + Helper::GenerateRandomString(64));
      ASSERT_TRUE(status.ok()) << "put fail: " << status.error_str();
    }
    auto status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "commit fail: " << status.error_str();
  }

  {
    auto txn = storage_->NewTxn();
    Range range;
    range.start = prefix;
    range.end = prefix + "\xFF";

    int count = 0;
    auto status = txn->Scan(
        range,
        [&](const std::string& /*key*/, const std::string& /*value*/) -> bool {
          ++count;
          return count < 3;  // stop after 3 entries
        });
    ASSERT_TRUE(status.ok()) << "scan fail: " << status.error_str();
    ASSERT_EQ(count, 3) << "handler should stop after 3 entries.";
    txn->Commit();
  }
}

TEST_F(TikvGoStorageTest, ScanEmptyRange) {
  GTEST_SKIP() << "Skip ScanEmptyRange test case.";

  auto txn = storage_->NewTxn();
  Range range;
  range.start = "unit_tikv_go_emptyrange_" + Helper::GenerateRandomString(32);
  range.end = range.start + "\xFF";

  std::vector<KeyValue> kvs;
  auto status = txn->Scan(range, UINT64_MAX, kvs);
  ASSERT_TRUE(status.ok()) << "scan fail: " << status.error_str();
  ASSERT_TRUE(kvs.empty()) << "scan empty range should return nothing.";
  txn->Commit();
}

TEST_F(TikvGoStorageTest, PutIfAbsent) {
  GTEST_SKIP() << "Skip PutIfAbsent test case.";

  auto txn = storage_->NewTxn();
  auto status = txn->PutIfAbsent("any_key", "any_value");
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
  txn->Commit();
}

TEST_F(TikvGoStorageTest, TxnID) {
  GTEST_SKIP() << "Skip TxnID test case.";

  auto txn = storage_->NewTxn();
  ASSERT_NE(txn->ID(), 0) << "txn id should not be zero.";
  txn->Commit();
}

TEST_F(TikvGoStorageTest, GetTrace) {
  GTEST_SKIP() << "Skip GetTrace test case.";

  auto txn = storage_->NewTxn();

  std::string key = "unit_tikv_go_trace_" + Helper::GenerateRandomString(32);
  auto status = txn->Put(key, "trace_value");
  ASSERT_TRUE(status.ok()) << "put fail: " << status.error_str();

  std::string value;
  status = txn->Get(key, value);
  ASSERT_TRUE(status.ok()) << "get fail: " << status.error_str();

  auto trace = txn->GetTrace();
  ASSERT_NE(trace.txn_id, 0) << "trace txn_id should not be zero.";
  ASSERT_GT(trace.read_time_us, 0) << "read_time_us should be positive.";

  status = txn->Commit();
  ASSERT_TRUE(status.ok()) << "commit fail: " << status.error_str();
}

TEST_F(TikvGoStorageTest, StoragePutKV) {
  GTEST_SKIP() << "Skip StoragePutKV test case.";

  KeyValue kv;
  kv.key = "unit_tikv_go_storage_putkv_" + Helper::GenerateRandomString(32);
  kv.value = "unit_value_" + Helper::GenerateRandomString(64);

  KVStorage::WriteOption opt;
  auto status = storage_->Put(opt, kv);
  ASSERT_TRUE(status.ok()) << "storage put kv fail: " << status.error_str();

  std::string got;
  status = storage_->Get(kv.key, got);
  ASSERT_TRUE(status.ok()) << "storage get fail: " << status.error_str();
  ASSERT_EQ(got, kv.value);
}

TEST_F(TikvGoStorageTest, StorageBatchDelete) {
  GTEST_SKIP() << "Skip StorageBatchDelete test case.";

  std::vector<std::string> keys;
  KVStorage::WriteOption opt;
  for (int i = 0; i < 5; ++i) {
    std::string key =
        "unit_tikv_go_storage_batchdel_" + Helper::GenerateRandomString(32);
    auto status = storage_->Put(opt, key, "value_" + std::to_string(i));
    ASSERT_TRUE(status.ok()) << "put fail: " << status.error_str();
    keys.push_back(key);
  }

  auto status = storage_->Delete(keys);
  ASSERT_TRUE(status.ok()) << "batch delete fail: " << status.error_str();

  for (const auto& key : keys) {
    std::string got;
    status = storage_->Get(key, got);
    ASSERT_FALSE(status.ok()) << "key should not exist after batch delete.";
  }
}

TEST_F(TikvGoStorageTest, StorageScan) {
  GTEST_SKIP() << "Skip StorageScan test case.";

  std::string prefix =
      "unit_tikv_go_storage_scan_" + Helper::GenerateRandomString(16) + "_";

  KVStorage::WriteOption opt;
  for (int i = 0; i < 5; ++i) {
    auto status = storage_->Put(opt, prefix + std::to_string(i),
                                "value_" + std::to_string(i));
    ASSERT_TRUE(status.ok()) << "put fail: " << status.error_str();
  }

  Range range;
  range.start = prefix;
  range.end = prefix + "\xFF";

  std::vector<KeyValue> kvs;
  auto status = storage_->Scan(range, kvs);
  ASSERT_TRUE(status.ok()) << "scan fail: " << status.error_str();
  ASSERT_EQ(kvs.size(), 5) << "storage scan result size not equal.";
}

TEST_F(TikvGoStorageTest, NewTxnReadCommitted) {
  GTEST_SKIP() << "Skip NewTxnReadCommitted test case.";

  auto txn = storage_->NewTxn(Txn::kReadCommitted);
  ASSERT_NE(txn, nullptr);
  ASSERT_NE(txn->ID(), 0);

  std::string key = "unit_tikv_go_rc_" + Helper::GenerateRandomString(32);
  auto status = txn->Put(key, "rc_value");
  ASSERT_TRUE(status.ok()) << "put fail: " << status.error_str();

  std::string value;
  status = txn->Get(key, value);
  ASSERT_TRUE(status.ok()) << "get fail: " << status.error_str();
  ASSERT_EQ(value, "rc_value");

  status = txn->Commit();
  ASSERT_TRUE(status.ok()) << "commit fail: " << status.error_str();
}

TEST_F(TikvGoStorageTest, PutOverwrite) {
  GTEST_SKIP() << "Skip PutOverwrite test case.";

  std::string key =
      "unit_tikv_go_overwrite_" + Helper::GenerateRandomString(32);

  {
    auto txn = storage_->NewTxn();
    auto status = txn->Put(key, "old_value");
    ASSERT_TRUE(status.ok()) << "put fail: " << status.error_str();
    status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "commit fail: " << status.error_str();
  }

  {
    auto txn = storage_->NewTxn();
    auto status = txn->Put(key, "new_value");
    ASSERT_TRUE(status.ok()) << "put fail: " << status.error_str();
    status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "commit fail: " << status.error_str();
  }

  {
    auto txn = storage_->NewTxn();
    std::string got;
    auto status = txn->Get(key, got);
    ASSERT_TRUE(status.ok()) << "get fail: " << status.error_str();
    ASSERT_EQ(got, "new_value") << "overwritten value not correct.";
    txn->Commit();
  }
}

TEST_F(TikvGoStorageTest, MultiBthreadConcurrent) {
  GTEST_SKIP() << "Skip MultiBthreadConcurrent test case.";

  struct Param {
    KVStorageSPtr storage;
    int thread_no;
  };

  const int thread_num = 4;
  std::vector<bthread_t> tids(thread_num);
  std::vector<Param*> params(thread_num);

  for (int i = 0; i < thread_num; ++i) {
    params[i] = new Param();
    params[i]->storage = storage_;
    params[i]->thread_no = i;

    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    ASSERT_EQ(0, bthread_start_background(
                     &tids[i], &attr,
                     [](void* arg) -> void* {
                       Param* p = reinterpret_cast<Param*>(arg);
                       auto storage = p->storage;
                       int no = p->thread_no;

                       for (int j = 0; j < 10; ++j) {
                         auto txn = storage->NewTxn();
                         std::string key = "unit_tikv_go_concurrent_" +
                                           std::to_string(no) + "_" +
                                           std::to_string(j);
                         txn->Put(key, "value_" + std::to_string(j));

                         std::string got;
                         txn->Get(key, got);
                         CHECK_EQ(got, "value_" + std::to_string(j));

                         txn->Commit();
                       }

                       delete p;
                       return nullptr;
                     },
                     params[i]));
  }

  for (int i = 0; i < thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

TEST_F(TikvGoStorageTest, PutBatchGetInBthread) {
  struct Param {
    KVStorageSPtr storage;
  };

  Param* param = new Param();
  param->storage = storage_;

  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  ASSERT_EQ(0, bthread_start_background(
                   &tid, &attr,
                   [](void* arg) -> void* {
                     Param* p = reinterpret_cast<Param*>(arg);
                     auto storage = p->storage;

                     std::vector<std::string> keys;
                     {
                       auto txn = storage->NewTxn();
                       for (int i = 0; i < 10; ++i) {
                         std::string key = "unit_tikv_go_bthread_" +
                                           Helper::GenerateRandomString(32);
                         txn->Put(key, "unit_value_" +
                                           Helper::GenerateRandomString(64));
                         keys.push_back(key);
                       }
                       txn->Commit();
                     }

                     {
                       auto txn = storage->NewTxn();
                       std::vector<KeyValue> kvs;
                       txn->BatchGet(keys, kvs);
                       CHECK_EQ(kvs.size(), keys.size())
                           << "kvs size not equal in bthread.";
                       txn->Commit();
                     }

                     delete p;
                     return nullptr;
                   },
                   param));

  bthread_join(tid, nullptr);
}

TEST_F(TikvGoStorageTest, LostUpdate) {
  GTEST_SKIP() << "Skip LostUpdate test case.";

  struct Param {
    KVStorageSPtr storage;
    std::atomic<uint64_t> counter{0};
  };

  Param* param = new Param();
  param->storage = storage_;

  const int thread_num = 4;
  const uint64_t loop_num = 1000;

  std::vector<std::thread> threads;
  threads.reserve(thread_num);
  for (int thread_no = 0; thread_no < thread_num; ++thread_no) {
    threads.emplace_back([param, thread_no]() {
      auto storage = param->storage;
      auto& counter = param->counter;

      const std::string key = "unit_tikv_go_lostupdate_00001";

      for (uint64_t i = 0; i < loop_num; ++i) {
        auto txn = storage->NewTxn();

        std::string value;
        auto status = txn->Get(key, value);
        if (!status.ok()) {
          if (status.error_code() == pb::error::ENOT_FOUND) {
            value = "0";
          } else {
            LOG(FATAL) << "get key fail, error: " << status.error_str();
          }
        }

        uint64_t value_int = std::stoull(value);
        value_int += 1;
        txn->Put(key, std::to_string(value_int));
        status = txn->Commit();
        if (status.ok()) {
          auto count = counter.fetch_add(1);
          EXPECT_EQ(value_int, count + 1)
              << "counter value not equal, thread_no: " << thread_no;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  LOG(INFO) << "Final counter value: " << param->counter.load();
  delete param;
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
