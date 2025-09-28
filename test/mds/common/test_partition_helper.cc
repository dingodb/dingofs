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
#include <sys/types.h>

#include <cstdint>
#include <string>
#include <vector>

#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mds/common/partition_helper.h"

namespace dingofs {
namespace mds {
namespace unit_test {

static PartitionPolicy GenPartitionPolicy(uint32_t bucket_num,
                                          const std::vector<uint64_t>& mds_ids,
                                          uint32_t expect_mds_num) {
  PartitionPolicy partition_policy;
  partition_policy.set_type(
      ::dingofs::pb::mds::PartitionType::PARENT_ID_HASH_PARTITION);
  partition_policy.set_epoch(1);
  auto* parent_hash = partition_policy.mutable_parent_hash();
  parent_hash->set_bucket_num(bucket_num);
  parent_hash->set_expect_mds_num(expect_mds_num);

  auto* distributions = parent_hash->mutable_distributions();
  for (const auto& mds_id : mds_ids) {
    distributions->insert({mds_id, BucketSetEntry()});
  }

  for (uint32_t i = 0; i < bucket_num; ++i) {
    const auto& mds_id = mds_ids[i % mds_ids.size()];
    distributions->at(mds_id).add_bucket_ids(i);
  }

  return partition_policy;
}

static uint32_t GetMdsNum(
    const std::map<uint64_t, BucketSetEntry>& distributions) {
  return distributions.size();
}

static bool IsContain(const std::map<uint64_t, BucketSetEntry>& distributions,
                      uint64_t mds_id) {
  return distributions.find(mds_id) != distributions.end();
}

class HashPartitionHelperTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(HashPartitionHelperTest, AdjustDistribution) {
  {
    PartitionPolicy partition_policy =
        GenPartitionPolicy(1024, {1001, 1002, 1003}, 3);
    std::set<uint64_t> online_mds_ids = {1001, 1002};
    std::set<uint64_t> offline_mds_ids = {1003};

    auto distributions = HashPartitionHelper::AdjustDistribution(
        partition_policy, online_mds_ids, offline_mds_ids);

    EXPECT_TRUE(HashPartitionHelper::CheckHashPartition(
        partition_policy.parent_hash()));
    EXPECT_EQ(2, GetMdsNum(distributions));
    EXPECT_TRUE(IsContain(distributions, 1001));
    EXPECT_TRUE(IsContain(distributions, 1002));
    EXPECT_FALSE(IsContain(distributions, 1003));
  }

  {
    PartitionPolicy partition_policy =
        GenPartitionPolicy(1024, {1001, 1002, 1003}, 3);
    std::set<uint64_t> online_mds_ids = {1001, 1002, 1004};
    std::set<uint64_t> offline_mds_ids = {1003};

    auto distributions = HashPartitionHelper::AdjustDistribution(
        partition_policy, online_mds_ids, offline_mds_ids);

    EXPECT_TRUE(HashPartitionHelper::CheckHashPartition(
        partition_policy.parent_hash()));
    EXPECT_EQ(3, GetMdsNum(distributions));
    EXPECT_TRUE(IsContain(distributions, 1001));
    EXPECT_TRUE(IsContain(distributions, 1002));
    EXPECT_TRUE(IsContain(distributions, 1004));
    EXPECT_FALSE(IsContain(distributions, 1003));
  }

  {
    PartitionPolicy partition_policy =
        GenPartitionPolicy(1024, {1001, 1002, 1003, 1004}, 3);
    std::set<uint64_t> online_mds_ids = {1001, 1002, 1004};
    std::set<uint64_t> offline_mds_ids = {1003};

    auto distributions = HashPartitionHelper::AdjustDistribution(
        partition_policy, online_mds_ids, offline_mds_ids);

    EXPECT_TRUE(HashPartitionHelper::CheckHashPartition(
        partition_policy.parent_hash()));
    EXPECT_EQ(3, GetMdsNum(distributions));
    EXPECT_TRUE(IsContain(distributions, 1001));
    EXPECT_TRUE(IsContain(distributions, 1002));
    EXPECT_TRUE(IsContain(distributions, 1004));
    EXPECT_FALSE(IsContain(distributions, 1003));
  }

  {
    PartitionPolicy partition_policy =
        GenPartitionPolicy(1024, {1001, 1002, 1003}, 3);
    std::set<uint64_t> online_mds_ids = {1001};
    std::set<uint64_t> offline_mds_ids = {1002, 1003};

    auto distributions = HashPartitionHelper::AdjustDistribution(
        partition_policy, online_mds_ids, offline_mds_ids);

    EXPECT_TRUE(HashPartitionHelper::CheckHashPartition(
        partition_policy.parent_hash()));
    EXPECT_EQ(1, GetMdsNum(distributions));
    EXPECT_TRUE(IsContain(distributions, 1001));
    EXPECT_FALSE(IsContain(distributions, 1002));
    EXPECT_FALSE(IsContain(distributions, 1003));
  }

  {
    PartitionPolicy partition_policy =
        GenPartitionPolicy(1024, {1001, 1002, 1003}, 4);
    std::set<uint64_t> online_mds_ids = {1001, 1004, 1005, 1006};
    std::set<uint64_t> offline_mds_ids = {1002, 1003};

    auto distributions = HashPartitionHelper::AdjustDistribution(
        partition_policy, online_mds_ids, offline_mds_ids);

    EXPECT_TRUE(HashPartitionHelper::CheckHashPartition(
        partition_policy.parent_hash()));
    EXPECT_EQ(4, GetMdsNum(distributions));
    EXPECT_TRUE(IsContain(distributions, 1001));
    EXPECT_TRUE(IsContain(distributions, 1004));
    EXPECT_TRUE(IsContain(distributions, 1005));
    EXPECT_TRUE(IsContain(distributions, 1006));
    EXPECT_FALSE(IsContain(distributions, 1002));
    EXPECT_FALSE(IsContain(distributions, 1003));
  }
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
