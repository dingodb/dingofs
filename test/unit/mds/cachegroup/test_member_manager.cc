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

#include <string>
#include <unordered_set>
#include <vector>

#include "dingofs/error.pb.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "mds/cachegroup/member_manager.h"
#include "mds/common/context.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/dummy_storage.h"

namespace dingofs {
namespace mds {

DEFINE_bool(mds_cache_member_enable_cache, true,
            "cache member enable cache for unit tests.");

namespace unit_test {
namespace {

constexpr char kGroupA[] = "group-a";
constexpr char kGroupB[] = "group-b";
constexpr char kMemberId[] = "123e4567-e89b-12d3-a456-426614174000";
constexpr char kIp[] = "127.0.0.1";
constexpr uint32_t kPort = 9000;

}  // namespace

class CacheGroupMemberManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    old_enable_cache_ = FLAGS_mds_cache_member_enable_cache;

    storage_ = DummyStorage::New();
    ASSERT_TRUE(storage_->Init(""));

    operation_processor_ = OperationProcessor::New(storage_);
    ASSERT_TRUE(operation_processor_->Init());
    manager_ = CacheGroupMemberManager::New(operation_processor_);
  }

  void TearDown() override {
    manager_.reset();
    if (operation_processor_ != nullptr) {
      operation_processor_->Destroy();
    }
    operation_processor_.reset();
    storage_.reset();
    FLAGS_mds_cache_member_enable_cache = old_enable_cache_;
  }

  KVStorageSPtr storage_;
  OperationProcessorSPtr operation_processor_;
  CacheGroupMemberManagerSPtr manager_;
  bool old_enable_cache_{true};
};

TEST_F(CacheGroupMemberManagerTest, JoinListReweightAndLeaveUseStore) {
  FLAGS_mds_cache_member_enable_cache = false;
  Context ctx;

  ASSERT_TRUE(
      manager_->JoinCacheGroup(ctx, kGroupA, kIp, kPort, 10, kMemberId).ok());

  CacheMemberEntry member;
  ASSERT_TRUE(manager_->GetCacheMember(ctx, kMemberId, member).ok());
  EXPECT_EQ(member.member_id(), kMemberId);
  EXPECT_EQ(member.group_name(), kGroupA);
  EXPECT_EQ(member.weight(), 10u);
  EXPECT_TRUE(member.locked());
  EXPECT_EQ(member.version(), 1u);

  std::unordered_set<std::string> groups;
  ASSERT_TRUE(manager_->ListGroups(ctx, groups).ok());
  EXPECT_EQ(groups, std::unordered_set<std::string>({kGroupA}));

  std::vector<CacheMemberEntry> members;
  ASSERT_TRUE(manager_->ListMembers(ctx, kGroupA, members).ok());
  ASSERT_EQ(members.size(), 1u);
  EXPECT_EQ(members[0].state(), pb::mds::CacheGroupMemberStateUnknown);

  ASSERT_TRUE(manager_->ReweightMember(ctx, kMemberId, kIp, kPort, 20).ok());
  ASSERT_TRUE(manager_->GetCacheMember(ctx, kMemberId, member).ok());
  EXPECT_EQ(member.weight(), 20u);
  EXPECT_EQ(member.version(), 2u);

  auto status = manager_->ReweightMember(ctx, kMemberId, "10.0.0.1", kPort, 30);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_MATCH);

  status = manager_->LeaveCacheGroup(ctx, kGroupB, kMemberId, kIp, kPort);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_MATCH);

  ASSERT_TRUE(
      manager_->LeaveCacheGroup(ctx, kGroupA, kMemberId, kIp, kPort).ok());
  ASSERT_TRUE(manager_->GetCacheMember(ctx, kMemberId, member).ok());
  EXPECT_TRUE(member.group_name().empty());
  EXPECT_EQ(member.version(), 3u);
}

TEST_F(CacheGroupMemberManagerTest, CacheKeepsNewestVersionAfterReweight) {
  FLAGS_mds_cache_member_enable_cache = true;
  Context ctx;

  ASSERT_TRUE(
      manager_->JoinCacheGroup(ctx, kGroupA, kIp, kPort, 10, kMemberId).ok());
  ASSERT_TRUE(manager_->ReweightMember(ctx, kMemberId, kIp, kPort, 20).ok());

  CacheMemberEntry member;
  ASSERT_TRUE(manager_->GetCacheMember(ctx, kMemberId, member).ok());
  EXPECT_EQ(member.weight(), 20u);
  EXPECT_EQ(member.version(), 2u);
}

TEST_F(CacheGroupMemberManagerTest, UnlockAndDeleteOfflineMember) {
  FLAGS_mds_cache_member_enable_cache = true;
  Context ctx;

  ASSERT_TRUE(
      manager_->JoinCacheGroup(ctx, kGroupA, kIp, kPort, 10, kMemberId).ok());
  ASSERT_TRUE(manager_->UnlockMember(ctx, kMemberId, kIp, kPort).ok());

  CacheMemberEntry member;
  ASSERT_TRUE(manager_->GetCacheMember(ctx, kMemberId, member).ok());
  EXPECT_FALSE(member.locked());

  ASSERT_TRUE(manager_->DeleteMember(ctx, kMemberId).ok());
  auto status = manager_->GetCacheMember(ctx, kMemberId, member);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_FOUND);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
