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

#include <cstdint>
#include <string>
#include <unordered_map>

#include "common/const.h"
#include "gtest/gtest.h"
#include "mds/filesystem/parent_memo.h"
#include "mds/quota/quota.h"

namespace dingofs {
namespace mds {
namespace unit_test {
namespace {

QuotaEntry MakeQuota(int64_t max_bytes, int64_t max_inodes,
                     int64_t used_bytes = 0, int64_t used_inodes = 0,
                     uint64_t version = 1, const std::string& uuid = "quota") {
  QuotaEntry quota;
  quota.set_max_bytes(max_bytes);
  quota.set_max_inodes(max_inodes);
  quota.set_used_bytes(used_bytes);
  quota.set_used_inodes(used_inodes);
  quota.set_uuid(uuid);
  quota.set_version(version);
  quota.set_create_time_ns(1);
  return quota;
}

}  // namespace

TEST(QuotaTest, AccumulatedUsageParticipatesInLimitCheck) {
  quota::Quota quota(1, 10, MakeQuota(100, 10, 30, 2));

  EXPECT_TRUE(quota.Check(70, 8));
  EXPECT_FALSE(quota.Check(71, 0));
  EXPECT_FALSE(quota.Check(0, 9));

  quota.UpdateUsage(20, 3, "write");
  auto accumulated = quota.GetAccumulatedQuota();
  EXPECT_EQ(accumulated.used_bytes(), 50);
  EXPECT_EQ(accumulated.used_inodes(), 5);
  EXPECT_TRUE(quota.Check(50, 5));
  EXPECT_FALSE(quota.Check(51, 0));
  EXPECT_FALSE(quota.Check(0, 6));
}

TEST(QuotaTest, RefreshCompactsAcknowledgedDeltaUsage) {
  quota::Quota quota(1, 10, MakeQuota(1000, 100, 10, 1, 1, "same"));
  quota.UpdateUsage(7, 2, "write");

  auto usages = quota.GetUsage();
  ASSERT_EQ(usages.size(), 1u);

  auto refreshed = MakeQuota(1000, 100, 17, 3, 2, "same");
  quota.Refresh(refreshed, usages.back().time_ns(), "flush");

  EXPECT_TRUE(quota.GetUsage().empty());
  auto accumulated = quota.GetAccumulatedQuota();
  EXPECT_EQ(accumulated.used_bytes(), 17);
  EXPECT_EQ(accumulated.used_inodes(), 3);
  EXPECT_EQ(accumulated.version(), 2u);
}

TEST(DirQuotaMapTest, ChecksNearestAndAncestorQuotaByParentMemo) {
  ParentMemo parent_memo(1);
  parent_memo.Remeber(100, kRootIno);
  parent_memo.Remeber(200, 100);

  quota::DirQuotaMap quota_map(1, parent_memo, nullptr);
  quota_map.UpsertQuota(kRootIno, MakeQuota(1000, 3, 0, 1, 1, "root"), "root");
  quota_map.UpsertQuota(100, MakeQuota(100, 100, 30, 0, 1, "dir"), "dir");

  ASSERT_NE(quota_map.GetNearestQuota(200), nullptr);
  EXPECT_EQ(quota_map.GetNearestQuota(200)->INo(), 100u);
  EXPECT_TRUE(quota_map.CheckQuota(200, 70, 2));
  EXPECT_FALSE(quota_map.CheckQuota(200, 71, 0));
  EXPECT_FALSE(quota_map.CheckQuota(200, 0, 3));

  quota_map.UpdateUsage(200, 60, 1, "create");
  EXPECT_FALSE(quota_map.CheckQuota(200, 41, 0));
  EXPECT_FALSE(quota_map.CheckQuota(200, 0, 2));
}

TEST(DirQuotaMapTest, RefreshAddsUpdatesAndEventuallyDropsMissingQuota) {
  ParentMemo parent_memo(1);
  parent_memo.Remeber(100, kRootIno);
  quota::DirQuotaMap quota_map(1, parent_memo, nullptr);
  quota_map.UpsertQuota(100, MakeQuota(100, 10, 0, 0, 1, "old"), "old");

  std::unordered_map<Ino, QuotaEntry> refreshed;
  refreshed[200] = MakeQuota(200, 20, 1, 2, 1, "new");
  quota_map.Refresh(refreshed, "refresh");

  EXPECT_NE(quota_map.GetNearestQuota(100), nullptr);
  ASSERT_NE(quota_map.GetNearestQuota(200), nullptr);
  EXPECT_EQ(quota_map.GetNearestQuota(200)->GetQuota().used_inodes(), 2);

  for (int i = 0; i < 30; ++i) {
    quota_map.Refresh(refreshed, "refresh");
  }

  EXPECT_EQ(quota_map.GetNearestQuota(100), nullptr);
  EXPECT_NE(quota_map.GetNearestQuota(200), nullptr);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
