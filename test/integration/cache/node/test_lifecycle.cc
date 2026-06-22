/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2026-06-22
 * Author: AI
 */

#include <chrono>
#include <thread>

#include "test/integration/cache/deploy/fixture.h"

namespace dingofs {
namespace cache {
namespace integration {

// A freshly spawned cache node opens its RPC port and stays alive.
TEST_F(NodeTest, StartsAndStaysAlive) {
  CacheNode node;
  ASSERT_TRUE(
      node.Start(workdir_ + "/node1", mds_.Addr(), kGroup, utils::GenerateUUID())
          .ok());

  std::this_thread::sleep_for(std::chrono::seconds(2));
  EXPECT_TRUE(node.Running()) << "cache node exited shortly after startup";

  node.Stop();
  EXPECT_FALSE(node.Running());
}

// The node answers BlockCacheService.Ping once its RPC port is up.
TEST_F(NodeTest, PingRespondsAfterStartup) {
  CacheNode node;
  ASSERT_TRUE(
      node.Start(workdir_ + "/nodep", mds_.Addr(), kGroup, utils::GenerateUUID())
          .ok());
  EXPECT_TRUE(WaitUntil([&] { return PingNode(node.Ip(), node.Port()); }))
      << "cache node never answered Ping";
}

// Stopping and restarting a node (new id) works -- the deploy harness reaps the
// first child cleanly before the second comes up.
TEST_F(NodeTest, RestartsCleanly) {
  {
    CacheNode node;
    ASSERT_TRUE(
        node.Start(workdir_ + "/n_a", mds_.Addr(), kGroup, utils::GenerateUUID())
            .ok());
    EXPECT_TRUE(node.Running());
  }

  CacheNode node;
  ASSERT_TRUE(
      node.Start(workdir_ + "/n_b", mds_.Addr(), kGroup, utils::GenerateUUID())
          .ok());
  EXPECT_TRUE(node.Running());
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
