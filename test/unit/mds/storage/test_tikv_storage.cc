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

#include <algorithm>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "bthread/types.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mds/common/helper.h"
#include "mds/storage/tikv_storage.h"

namespace dingofs {
namespace mds {
namespace unit_test {

// skip test case, because tikv server is not ready in test environment, and the
// test case will fail.
class DISABLED_TikvStorageTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    storage_ = TikvStorage::New();
    ASSERT_TRUE(storage_->Init("10.220.32.40:2379"))
        << "init tikv storage fail.";
  }

  static void TearDownTestSuite() { storage_->Destroy(); }

  void SetUp() override {}
  void TearDown() override {}

 public:
  static KVStorageSPtr storage_;
};

KVStorageSPtr DISABLED_TikvStorageTest::storage_ = nullptr;

TEST_F(DISABLED_TikvStorageTest, Put) {
  GTEST_SKIP() << "Skip Put test case.";

  auto txn = storage_->NewTxn();

  for (int i = 0; i < 10; ++i) {
    txn->Put("unit_key_put_" + Helper::GenerateRandomString(32),
             "unit_value_" + Helper::GenerateRandomString(64));
  }

  txn->Commit();
}

TEST_F(DISABLED_TikvStorageTest, PutGet) {
  GTEST_SKIP() << "Skip PutGet test case.";

  auto txn = storage_->NewTxn();

  std::vector<std::string> keys;
  for (int i = 0; i < 10; ++i) {
    std::string key = "unit_key_putget_" + Helper::GenerateRandomString(32);
    txn->Put(key, "unit_value_" + Helper::GenerateRandomString(64));

    keys.push_back(key);
  }

  for (const auto& key : keys) {
    std::string value;
    auto status = txn->Get(key, value);
    ASSERT_TRUE(status.ok()) << "get key fail, error: " << status.error_str();
    ASSERT_FALSE(value.empty()) << "value is empty.";
  }

  txn->Commit();
}

TEST_F(DISABLED_TikvStorageTest, PutBatchGet) {
  GTEST_SKIP() << "Skip PutBatchGet test case.";

  std::vector<std::string> keys;
  {
    auto txn = storage_->NewTxn();

    for (int i = 0; i < 10; ++i) {
      std::string key =
          "unit_key_putbatchget_" + Helper::GenerateRandomString(32);
      txn->Put(key, "unit_value_" + Helper::GenerateRandomString(64));

      keys.push_back(key);
    }

    txn->Commit();
  }

  {
    auto txn = storage_->NewTxn();

    std::vector<KeyValue> kvs;
    txn->BatchGet(keys, kvs);
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

TEST_F(DISABLED_TikvStorageTest, PutBatchGetInBthread) {
  GTEST_SKIP() << "Skip PutBatchGetInBthread test case.";
  struct Param {
    KVStorageSPtr storage;
  };

  Param* param = new Param();
  param->storage = storage_;

  std::cout << "Start PutBatchGetInBthread test." << std::endl;
  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_PTHREAD;
  if (bthread_start_background(
          &tid, &attr,
          [](void* arg) -> void* {
            Param* param = reinterpret_cast<Param*>(arg);
            auto storage = param->storage;

            std::cout << "here 0001." << std::endl;
            std::vector<std::string> keys;
            {
              auto txn = storage->NewTxn();

              for (int i = 0; i < 10; ++i) {
                std::string key = "unit_key_putbatchgetbtrhead_" +
                                  Helper::GenerateRandomString(32);
                txn->Put(key, "unit_value_" + Helper::GenerateRandomString(64));

                keys.push_back(key);
              }

              std::cout << "here 0002." << std::endl;
              txn->Commit();
              std::cout << "here 0003." << std::endl;
            }

            {
              auto txn = storage->NewTxn();

              std::cout << "here 0004." << std::endl;
              std::vector<KeyValue> kvs;
              txn->BatchGet(keys, kvs);
              // ASSERT_EQ(kvs.size(), keys.size()) << "kvs size not equal.";
              std::sort(keys.begin(), keys.end());  // NOLINT
              std::sort(                            // NOLINT
                  kvs.begin(), kvs.end(),
                  [](const KeyValue& a, const KeyValue& b) {
                    return a.key < b.key;
                  });
              // for (size_t i = 0; i < keys.size(); ++i) {
              //   ASSERT_EQ(keys[i], kvs[i].key) << "key not equal.";
              //   ASSERT_FALSE(kvs[i].value.empty()) << "value is empty.";
              // }

              std::cout << "here 0005." << std::endl;
              txn->Commit();
              std::cout << "here 0006." << std::endl;
            }

            delete param;
            return nullptr;
          },
          param) != 0) {
    delete param;
    LOG(FATAL) << "[operation] start background thread fail.";
  }

  std::cout << "Wait PutBatchGetInBthread test." << std::endl;

  bthread_join(tid, nullptr);

  std::cout << "End PutBatchGetInBthread test." << std::endl;
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
