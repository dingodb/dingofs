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
 * Created Date: 2026-02-02
 * Author: AI
 */

#include <gtest/gtest.h>

#include <sstream>

#include "cache/common/mds_client.h"

namespace dingofs {
namespace cache {

class MDSClientTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(MDSClientTest, CacheGroupMemberStateToString) {
  EXPECT_EQ(CacheGroupMemberStateToString(CacheGroupMemberState::kUnknown),
            "unknown");
  EXPECT_EQ(CacheGroupMemberStateToString(CacheGroupMemberState::kOnline),
            "online");
  EXPECT_EQ(CacheGroupMemberStateToString(CacheGroupMemberState::kUnstable),
            "unstable");
  EXPECT_EQ(CacheGroupMemberStateToString(CacheGroupMemberState::kOffline),
            "offline");
}

TEST_F(MDSClientTest, CacheGroupMemberEquality) {
  CacheGroupMember member1{
      .id = "550e8400-e29b-41d4-a716-446655440000",
      .ip = "192.168.1.1",
      .port = 8080,
      .weight = 100,
      .state = CacheGroupMemberState::kOnline,
  };

  {
    auto member2 = member1;
    member2.id = "550e8400-e29b-41d4-a716-446655440001";
    EXPECT_FALSE(member1 == member2);
  }

  {
    auto member2 = member1;
    member2.ip = "192.168.1.2";
    EXPECT_FALSE(member1 == member2);
  }

  {
    auto member2 = member1;
    member2.port = 8081;
    EXPECT_FALSE(member1 == member2);
  }

  {
    auto member2 = member1;
    member2.weight = 200;
    EXPECT_FALSE(member1 == member2);
  }

  {
    auto member2 = member1;
    member2.state = CacheGroupMemberState::kOffline;
    EXPECT_FALSE(member1 == member2);
  }
}

TEST_F(MDSClientTest, CacheGroupMemberStreamOperator) {
  CacheGroupMember member{
      .id = "550e8400-e29b-41d4-a716-446655440000",
      .ip = "10.0.0.1",
      .port = 9000,
      .weight = 50,
      .state = CacheGroupMemberState::kOnline,
  };

  std::ostringstream oss;
  oss << member;

  std::string output = oss.str();
  EXPECT_NE(output.find("id=550e8400-e29b-41d4-a716-446655440000"),
            std::string::npos);
  EXPECT_NE(output.find("ip=10.0.0.1"), std::string::npos);
  EXPECT_NE(output.find("port=9000"), std::string::npos);
  EXPECT_NE(output.find("weight=50"), std::string::npos);
  EXPECT_NE(output.find("state=online"), std::string::npos);
}

TEST_F(MDSClientTest, CacheGroupMemberStreamOperatorDifferentStates) {
  CacheGroupMember member{
      .id = "550e8400-e29b-41d4-a716-446655440000",
      .ip = "127.0.0.1",
      .port = 8080,
      .weight = 100,
      .state = CacheGroupMemberState::kUnknown,
  };

  {
    member.state = CacheGroupMemberState::kUnknown;
    std::ostringstream oss;
    oss << member;
    EXPECT_NE(oss.str().find("state=unknown"), std::string::npos);
  }

  {
    member.state = CacheGroupMemberState::kUnstable;
    std::ostringstream oss;
    oss << member;
    EXPECT_NE(oss.str().find("state=unstable"), std::string::npos);
  }

  {
    member.state = CacheGroupMemberState::kOffline;
    std::ostringstream oss;
    oss << member;
    EXPECT_NE(oss.str().find("state=offline"), std::string::npos);
  }
}

}  // namespace cache
}  // namespace dingofs
