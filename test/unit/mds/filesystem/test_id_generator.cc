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

#include <cstdint>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "fmt/core.h"
#include "gtest/gtest.h"
#include "mds/coordinator/dummy_coordinator_client.h"
#include "mds/filesystem/id_generator.h"
#include "mds/storage/dummy_storage.h"

namespace dingofs {
namespace mds {
namespace unit_test {

// test CoorAutoIncrementIdGenerator
class AutoIncrementIdGeneratorTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(AutoIncrementIdGeneratorTest, GenID) {
  auto coordinator_client = DummyCoordinatorClient::New();
  ASSERT_TRUE(coordinator_client->Init("")) << "init coordinator client fail.";

  int64_t table_id = 1001;
  auto id_generator = CoorAutoIncrementIdGenerator::New(
      coordinator_client, "test", table_id, 20000, 8);
  ASSERT_TRUE(id_generator->Init()) << "init id generator fail.";

  for (int i = 0; i < 1000; ++i) {
    uint64_t id = 0;
    ASSERT_TRUE(id_generator->GenID(1, id));
    ASSERT_EQ(id, 20000 + i);
  }
}

// test StoreAutoIncrementIdGenerator (lock-free bump allocator)
class StoreAutoIncrementIdGeneratorTest : public testing::Test {
 protected:
  void SetUp() override { storage_ = DummyStorage::New(); }

  KVStorageSPtr storage_;
};

TEST_F(StoreAutoIncrementIdGeneratorTest, SequentialGenID) {
  const int64_t kStartId = 1000;
  auto id_generator =
      StoreAutoIncrementIdGenerator::New(storage_, "store-seq", kStartId, 8);
  ASSERT_TRUE(id_generator->Init()) << "init id generator fail.";

  // IDs are strictly increasing and never below start.
  uint64_t prev = 0;
  for (int i = 0; i < 1000; ++i) {
    uint64_t id = 0;
    ASSERT_TRUE(id_generator->GenID(1, id));
    ASSERT_GE(id, static_cast<uint64_t>(kStartId));
    if (i > 0) ASSERT_GT(id, prev);
    prev = id;
  }
}

TEST_F(StoreAutoIncrementIdGeneratorTest, ConcurrentNoDuplicate) {
  const int64_t kStartId = 1000;
  // Small batch size forces frequent refills, exercising the refill path.
  auto id_generator =
      StoreAutoIncrementIdGenerator::New(storage_, "store-conc", kStartId, 8);
  ASSERT_TRUE(id_generator->Init()) << "init id generator fail.";

  constexpr int kThreads = 16;
  constexpr int kPerThread = 2000;
  std::vector<std::vector<uint64_t>> per_thread(kThreads);

  std::vector<std::thread> workers;
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t]() {
      per_thread[t].reserve(kPerThread);
      for (int i = 0; i < kPerThread; ++i) {
        uint64_t id = 0;
        ASSERT_TRUE(id_generator->GenID(1, id));
        ASSERT_GE(id, static_cast<uint64_t>(kStartId));
        per_thread[t].push_back(id);
      }
    });
  }
  for (auto& w : workers) w.join();

  std::set<uint64_t> all;
  for (auto& v : per_thread) {
    for (uint64_t id : v) {
      ASSERT_TRUE(all.insert(id).second) << "duplicate id: " << id;
    }
  }
  ASSERT_EQ(all.size(), static_cast<size_t>(kThreads) * kPerThread);
}

TEST_F(StoreAutoIncrementIdGeneratorTest, ConcurrentMixedNumAndFloor) {
  const int64_t kStartId = 1000;
  auto id_generator =
      StoreAutoIncrementIdGenerator::New(storage_, "store-mixed", kStartId, 16);
  ASSERT_TRUE(id_generator->Init()) << "init id generator fail.";

  // A floor far above the start: every returned id must respect it.
  const uint64_t kFloor = 1'000'000;

  constexpr int kThreads = 12;
  constexpr int kPerThread = 1000;
  std::mutex mu;
  std::set<uint64_t> all;

  std::vector<std::thread> workers;
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t]() {
      for (int i = 0; i < kPerThread; ++i) {
        // Vary num across the bundle boundary.
        uint32_t num = (i % 3) + 1;
        uint64_t id = 0;
        ASSERT_TRUE(id_generator->GenID(num, kFloor, id));
        ASSERT_GE(id, kFloor);
        std::lock_guard<std::mutex> lk(mu);
        for (uint32_t k = 0; k < num; ++k) {
          ASSERT_TRUE(all.insert(id + k).second) << "duplicate id: " << (id + k);
        }
      }
    });
  }
  for (auto& w : workers) w.join();
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs