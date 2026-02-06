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

#include <fmt/format.h>

#include <cstdint>
#include <string>
#include <vector>

#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mds/common/distribution_lock.h"
#include "mds/coordinator/dingo_coordinator_client.h"

namespace dingofs {
namespace mds {
namespace unit_test {

class CoorDistributionLockTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(CoorDistributionLockTest, Lock) {
  GTEST_SKIP() << "Skip test case.";

  auto coordinator_client = DingoCoordinatorClient::New();
  ASSERT_TRUE(coordinator_client->Init("172.20.3.17:22001"));

  std::string lock_prefix = "/lock07";

  std::vector<CoorDistributionLockSPtr> dist_locks;
  for (int i = 0; i < 2; ++i) {
    auto dist_lock =
        CoorDistributionLock::New(coordinator_client, lock_prefix, 10000 + i);
    ASSERT_TRUE(dist_lock->Init());
    dist_locks.push_back(dist_lock);
  }

  for (;;) {
    for (auto& dist_lock : dist_locks) {
      // LOG(INFO) << fmt::format("lock key: {} {}", dist_lock->LockKey(),
      // dist_lock->IsLocked());
    }

    bthread_usleep(2 * 1000 * 1000);
  }

  for (auto& dist_lock : dist_locks) {
    dist_lock->Destroy();
  }
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs