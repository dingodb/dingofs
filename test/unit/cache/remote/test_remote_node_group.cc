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
 * Created Date: 2026-06-21
 * Author: AI
 */

#include "cache/remote/remote_cache_cluster.h"

#include <gtest/gtest.h>

#include <string>

#include "cache/iutil/ketama_con_hash.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

namespace {

CacheGroupMember Member(const std::string& id, const std::string& ip,
                        uint32_t port, uint32_t weight,
                        CacheGroupMemberState state) {
  return CacheGroupMember{
      .id = id,
      .ip = ip,
      .port = port,
      .weight = weight,
      .state = state,
  };
}

}  // namespace

class RemoteNodeGroupBuilderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_connections_ = FLAGS_connections;
    FLAGS_connections = 1;
  }

  void TearDown() override { FLAGS_connections = saved_connections_; }

 private:
  int32_t saved_connections_{0};
};

TEST(RemoteNodeGroupTest, SelectNodeReturnsNullWhenHashMisses) {
  auto group = std::make_shared<RemoteNodeGroup>();
  auto hash = std::make_unique<iutil::KetamaConHash>();
  hash->Final();
  group->chash = std::move(hash);

  EXPECT_EQ(group->SelectNode("block-key"), nullptr);
}

TEST_F(RemoteNodeGroupBuilderTest, BuildFiltersUnavailableMembers) {
  RemoteNodeGroupBuilder builder(/*start_nodes=*/false);

  Members members{
      Member("member-offline", "10.0.1.1", 9300, 10,
             CacheGroupMemberState::kOffline),
      Member("member-zero-weight", "10.0.1.2", 9300, 0,
             CacheGroupMemberState::kOnline),
      Member("044d4698-7bd4-4e44-9e94-aee6312ff06f", "10.0.1.3", 9300,
             20, CacheGroupMemberState::kOnline),
  };

  auto group = builder.Build(members);
  ASSERT_NE(group, nullptr);
  ASSERT_EQ(group->nodes.size(), 1u);
  EXPECT_NE(group->nodes.find("044d4698-7bd4-4e44-9e94-aee6312ff06f"),
            group->nodes.end());
  EXPECT_NE(group->SelectNode("blocks/0/0/1_0_4096"), nullptr);

  EXPECT_EQ(builder.Build(members), nullptr);
}

TEST_F(RemoteNodeGroupBuilderTest, BuildKeepsReplacesAddsAndRemovesNodes) {
  RemoteNodeGroupBuilder builder(/*start_nodes=*/false);

  Members first_members{
      Member("keep-member", "10.0.1.10", 9300, 10,
             CacheGroupMemberState::kOnline),
      Member("replace-member", "10.0.1.11", 9300, 10,
             CacheGroupMemberState::kOnline),
      Member("remove-member", "10.0.1.12", 9300, 10,
             CacheGroupMemberState::kOnline),
  };
  auto first = builder.Build(first_members);
  ASSERT_NE(first, nullptr);

  auto keep_node = first->nodes["keep-member"];
  auto replace_node = first->nodes["replace-member"];

  Members second_members{
      Member("keep-member", "10.0.1.10", 9300, 30,
             CacheGroupMemberState::kOnline),
      Member("replace-member", "10.0.1.99", 9300, 10,
             CacheGroupMemberState::kOnline),
      Member("add-member", "10.0.1.13", 9300, 10,
             CacheGroupMemberState::kOnline),
  };
  auto second = builder.Build(second_members);
  ASSERT_NE(second, nullptr);

  EXPECT_EQ(second->nodes.size(), 3u);
  EXPECT_EQ(second->nodes["keep-member"], keep_node);
  EXPECT_NE(second->nodes["replace-member"], replace_node);
  EXPECT_NE(second->nodes.find("add-member"), second->nodes.end());
  EXPECT_EQ(second->nodes.find("remove-member"), second->nodes.end());
}

TEST_F(RemoteNodeGroupBuilderTest, BuildReturnsNullWhenNoOnlineMembersRemain) {
  RemoteNodeGroupBuilder builder(/*start_nodes=*/false);

  Members first_members{
      Member("member-a", "10.0.1.10", 9300, 10,
             CacheGroupMemberState::kOnline),
  };
  ASSERT_NE(builder.Build(first_members), nullptr);

  Members second_members{
      Member("member-a", "10.0.1.10", 9300, 10,
             CacheGroupMemberState::kOffline),
  };
  EXPECT_EQ(builder.Build(second_members), nullptr);
}

}  // namespace cache
}  // namespace dingofs
