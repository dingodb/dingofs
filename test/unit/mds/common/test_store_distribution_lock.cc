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
#include <chrono>
#include <thread>

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "mds/common/distribution_lock.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/dummy_storage.h"

namespace dingofs {
namespace mds {

// Defined in mds/common/distribution_lock.cc. Shrinking the lease TTL lets
// these tests observe lock acquisition/expiry/hand-off within milliseconds
// instead of the production default (18s).
DECLARE_uint64(mds_distribution_lock_lease_ttl_ms);

namespace unit_test {

class StoreDistributionLockTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    saved_ttl_ms = FLAGS_mds_distribution_lock_lease_ttl_ms;
    FLAGS_mds_distribution_lock_lease_ttl_ms = 100;
  }

  static void TearDownTestSuite() { FLAGS_mds_distribution_lock_lease_ttl_ms = saved_ttl_ms; }

  static KVStorageSPtr NewStorage() {
    auto storage = DummyStorage::New();
    EXPECT_TRUE(storage->Init(""));
    return storage;
  }

  // Polls `pred` until it becomes true or `timeout` elapses. Avoids
  // hard-coded sleeps racing with the lock's background renew bthread.
  template <typename Pred>
  static bool WaitUntil(Pred pred, std::chrono::milliseconds timeout = std::chrono::milliseconds(2000)) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      if (pred()) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
  }

  static uint64_t saved_ttl_ms;
};
uint64_t StoreDistributionLockTest::saved_ttl_ms = 0;

TEST_F(StoreDistributionLockTest, SingleOwnerEventuallyLocksAndKeyIsPersisted) {
  auto storage = NewStorage();
  auto lock = StoreDistributionLock::New(storage, "test_lock_single", /*mds_id=*/1);

  ASSERT_TRUE(lock->Init());
  ASSERT_TRUE(WaitUntil([&] { return lock->IsLocked(); }));
  EXPECT_EQ(lock->LockKey(), "test_lock_single");

  lock->Destroy();
}

TEST_F(StoreDistributionLockTest, SecondOwnerCannotAcquireWhileFirstHoldsLock) {
  auto storage = NewStorage();
  auto lock1 = StoreDistributionLock::New(storage, "test_lock_mutex", /*mds_id=*/1);
  auto lock2 = StoreDistributionLock::New(storage, "test_lock_mutex", /*mds_id=*/2);

  ASSERT_TRUE(lock1->Init());
  ASSERT_TRUE(WaitUntil([&] { return lock1->IsLocked(); }));

  ASSERT_TRUE(lock2->Init());
  // While lock1 keeps renewing an unexpired lease, lock2 must never observe
  // itself as the owner: this is the core mutual-exclusion guarantee of a
  // distribution lock.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_FALSE(lock2->IsLocked());
  EXPECT_TRUE(lock1->IsLocked());

  lock1->Destroy();
  lock2->Destroy();
}

TEST_F(StoreDistributionLockTest, SecondOwnerTakesOverAfterFirstStopsRenewing) {
  auto storage = NewStorage();
  auto lock1 = StoreDistributionLock::New(storage, "test_lock_handoff", /*mds_id=*/1);
  auto lock2 = StoreDistributionLock::New(storage, "test_lock_handoff", /*mds_id=*/2);

  ASSERT_TRUE(lock1->Init());
  ASSERT_TRUE(WaitUntil([&] { return lock1->IsLocked(); }));

  // Stop lock1's renewal without deleting its key, simulating a crashed/
  // partitioned holder: the key stays in storage but its lease will expire.
  lock1->Destroy();

  ASSERT_TRUE(lock2->Init());
  ASSERT_TRUE(WaitUntil([&] { return lock2->IsLocked(); }, std::chrono::milliseconds(3000)));

  lock2->Destroy();
}

TEST_F(StoreDistributionLockTest, GetAllLockInfoReportsCurrentOwner) {
  auto storage = NewStorage();
  auto operation_processor = OperationProcessor::New(storage);
  ASSERT_TRUE(operation_processor->Init());

  auto lock = StoreDistributionLock::New(storage, "test_lock_describe", /*mds_id=*/42);
  ASSERT_TRUE(lock->Init());
  ASSERT_TRUE(WaitUntil([&] { return lock->IsLocked(); }));

  std::vector<StoreDistributionLock::LockEntry> entries;
  ASSERT_TRUE(StoreDistributionLock::GetAllLockInfo(operation_processor, entries).ok());

  auto it = std::find_if(entries.begin(), entries.end(),
                          [](const auto& entry) { return entry.name == "test_lock_describe"; });
  ASSERT_NE(it, entries.end());
  EXPECT_EQ(it->owner, 42);

  lock->Destroy();
  operation_processor->Destroy();
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
