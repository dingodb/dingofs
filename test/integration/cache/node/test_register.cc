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

#include "test/integration/cache/deploy/fixture.h"

namespace dingofs {
namespace cache {
namespace integration {

// A started node registers itself into its cache group on the MDS and shows up
// as Online via ListMembers.
TEST_F(NodeTest, RegistersOnline) {
  const auto id = utils::GenerateUUID();
  CacheNode node;
  ASSERT_TRUE(node.Start(workdir_ + "/node", mds_.Addr(), kGroup, id).ok());

  ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, id))
      << "cache node never reached Online state";

  // The registered member reports the address we launched it on.
  std::vector<CacheGroupMember> members;
  ASSERT_TRUE(mds_client_->ListMembers(kGroup, &members).ok());
  bool found = false;
  for (const auto& m : members) {
    if (m.id == id) {
      EXPECT_EQ(m.ip, node.Ip());
      EXPECT_EQ(m.port, static_cast<uint32_t>(node.Port()));
      found = true;
    }
  }
  EXPECT_TRUE(found) << "node id absent from ListMembers";
}

// Two nodes in the same group both register and are both visible.
TEST_F(NodeTest, MultipleMembersRegister) {
  const auto id1 = utils::GenerateUUID();
  const auto id2 = utils::GenerateUUID();
  CacheNode n1, n2;
  ASSERT_TRUE(n1.Start(workdir_ + "/n1", mds_.Addr(), kGroup, id1).ok());
  ASSERT_TRUE(n2.Start(workdir_ + "/n2", mds_.Addr(), kGroup, id2).ok());

  EXPECT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, id1));
  EXPECT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, id2));
}

// The member registers with the weight the node was launched with (100).
TEST_F(NodeTest, RegisteredMemberHasExpectedWeight) {
  const auto id = utils::GenerateUUID();
  CacheNode node;
  ASSERT_TRUE(node.Start(workdir_ + "/node", mds_.Addr(), kGroup, id).ok());
  ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, id));

  std::vector<CacheGroupMember> members;
  ASSERT_TRUE(mds_client_->ListMembers(kGroup, &members).ok());
  for (const auto& m : members) {
    if (m.id == id) EXPECT_EQ(m.weight, 100u);
  }
}

// A node keeps its Online state across several heartbeat cycles.
TEST_F(NodeTest, StaysOnlineAcrossHeartbeats) {
  const auto id = utils::GenerateUUID();
  CacheNode node;
  ASSERT_TRUE(node.Start(workdir_ + "/node", mds_.Addr(), kGroup, id).ok());
  ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), kGroup, id));

  std::this_thread::sleep_for(std::chrono::seconds(5));
  std::vector<CacheGroupMember> members;
  ASSERT_TRUE(mds_client_->ListMembers(kGroup, &members).ok());
  bool online = false;
  for (const auto& m : members) {
    if (m.id == id) online = (m.state == CacheGroupMemberState::kOnline);
  }
  EXPECT_TRUE(online) << "node fell out of Online while still running";
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
