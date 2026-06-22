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

// Nodes in different groups are isolated: each group's ListMembers only returns
// its own members.
TEST_F(NodeTest, NodesInDifferentGroupsAreIsolated) {
  const std::string group_a = "group-a";
  const std::string group_b = "group-b";
  const auto id_a = utils::GenerateUUID();
  const auto id_b = utils::GenerateUUID();

  CacheNode na, nb;
  ASSERT_TRUE(na.Start(workdir_ + "/na", mds_.Addr(), group_a, id_a).ok());
  ASSERT_TRUE(nb.Start(workdir_ + "/nb", mds_.Addr(), group_b, id_b).ok());
  ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), group_a, id_a));
  ASSERT_TRUE(WaitMemberOnline(mds_client_.get(), group_b, id_b));

  auto has = [&](const std::string& group, const std::string& id) {
    std::vector<CacheGroupMember> members;
    EXPECT_TRUE(mds_client_->ListMembers(group, &members).ok());
    for (const auto& m : members) {
      if (m.id == id) return true;
    }
    return false;
  };

  EXPECT_TRUE(has(group_a, id_a));
  EXPECT_FALSE(has(group_a, id_b)) << "group-a leaked a group-b member";
  EXPECT_TRUE(has(group_b, id_b));
  EXPECT_FALSE(has(group_b, id_a)) << "group-b leaked a group-a member";
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
