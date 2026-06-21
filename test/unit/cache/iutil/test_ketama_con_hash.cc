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

#include <algorithm>
#include <cmath>
#include <map>
#include <string>
#include <vector>

#include "cache/iutil/con_hash.h"
#include "cache/iutil/ketama_con_hash.h"
#include "gtest/gtest.h"

namespace dingofs {

namespace cache {
namespace iutil {

namespace {

constexpr int kSampleCount = 20000;

std::map<std::string, int> CountLookups(KetamaConHash* hash,
                                        int sample_count = kSampleCount) {
  std::map<std::string, int> count;
  for (int i = 0; i < sample_count; i++) {
    ConNode node;
    EXPECT_TRUE(hash->Lookup(std::to_string(i), node));
    count[node.key]++;
  }
  return count;
}

double DeviationPercent(int actual, int expected) {
  return std::abs(static_cast<double>(actual - expected) / expected * 100.0);
}

}  // namespace

TEST(KetamaConHashTest, LookupSingleNode) {
  KetamaConHash hash;
  hash.AddNode("/sda");
  hash.Final();

  for (int i = 0; i < 1000; i++) {
    ConNode node;
    bool find = hash.Lookup(std::to_string(i), node);
    EXPECT_TRUE(find);
    EXPECT_EQ(node.key, "/sda");
  }
}

TEST(KetamaConHashTest, DistributeDisksEvenly) {
  KetamaConHash hash;
  hash.AddNode("/sda");
  hash.AddNode("/sdb");
  hash.AddNode("/sdc");
  hash.AddNode("/sdd");
  hash.AddNode("/sde");
  hash.Final();

  auto count = CountLookups(&hash);
  ASSERT_EQ(count.size(), 5);

  int average = kSampleCount / 5;
  for (const auto& kv : count) {
    EXPECT_LE(DeviationPercent(kv.second, average), 25.0);
  }
}

TEST(KetamaConHashTest, RedistributeAfterRemoveNode) {
  KetamaConHash hash;
  hash.AddNode("/sda");
  hash.AddNode("/sdb");
  hash.AddNode("/sdc");
  hash.AddNode("/sdd");
  hash.AddNode("/sde");
  hash.Final();

  std::map<std::string, std::vector<int>> disk_keys;
  {
    for (int i = 0; i < kSampleCount; i++) {
      ConNode node;
      bool find = hash.Lookup(std::to_string(i), node);
      EXPECT_TRUE(find);
      disk_keys[node.key].push_back(i);
    }

    ASSERT_EQ(disk_keys.size(), 5);
    for (const auto& kv : disk_keys) {
      EXPECT_GT(kv.second.size(), 3000);
    }
  }

  std::map<std::string, std::vector<int>> re_distri_disk_keys;
  {
    bool res = hash.RemoveNode("/sdc");
    EXPECT_TRUE(res);
    hash.Final();

    for (int i = 0; i < kSampleCount; i++) {
      ConNode node;
      bool find = hash.Lookup(std::to_string(i), node);
      EXPECT_TRUE(find);
      EXPECT_NE(node.key, "/sdc");

      re_distri_disk_keys[node.key].push_back(i);
    }

    ASSERT_EQ(re_distri_disk_keys.size(), 4);
    for (const auto& kv : re_distri_disk_keys) {
      EXPECT_GT(kv.second.size(), 3500);
    }
  }

  for (auto& kv : re_distri_disk_keys) {
    int disk_key_count = kv.second.size();
    int added_disk_key_count = disk_key_count;

    auto it = disk_keys.find(kv.first);
    if (it != disk_keys.end()) {
      auto& original_keys = it->second;
      auto& redistributed_keys = kv.second;

      // Remove keys that are still in the original node
      auto new_end =
          std::remove_if(redistributed_keys.begin(), redistributed_keys.end(),
                         [&original_keys](int key) {
                           auto vit = std::find(original_keys.begin(),
                                                original_keys.end(), key);
                           if (vit != original_keys.end()) {
                             original_keys.erase(vit);
                             return true;
                           }
                           return false;
                         });
      redistributed_keys.erase(new_end, redistributed_keys.end());

      // Calculate added keys
      added_disk_key_count = redistributed_keys.size();

      // Ensure all original keys have been included
      EXPECT_TRUE(original_keys.empty());
    }

    EXPECT_GT(added_disk_key_count, 0);
  }
}

TEST(KetamaConHashTest, DistributeByWeight) {
  KetamaConHash hash;
  hash.AddNode("/sda", 5);
  hash.AddNode("/sdb", 10);
  hash.AddNode("/sdc", 20);
  hash.Final();

  auto count = CountLookups(&hash);
  ASSERT_EQ(count.size(), 3);

  std::map<std::string, int> expect_count;
  expect_count["/sda"] = (5 * kSampleCount) / (5 + 10 + 20);
  expect_count["/sdb"] = (10 * kSampleCount) / (5 + 10 + 20);
  expect_count["/sdc"] = (20 * kSampleCount) / (5 + 10 + 20);

  for (const auto& kv : count) {
    EXPECT_LE(DeviationPercent(kv.second, expect_count[kv.first]), 20.0);
  }
}

TEST(KetamaConHashTest, DistributeIpNodesEvenly) {
  KetamaConHash hash;
  hash.AddNode("10.0.1.1:11211");
  hash.AddNode("10.0.1.2:11211");
  hash.AddNode("10.0.1.3:11211");
  hash.AddNode("10.0.1.4:11211");
  hash.AddNode("10.0.1.5:11211");
  hash.AddNode("10.0.1.6:11211");
  hash.AddNode("10.0.1.7:11211");
  hash.AddNode("10.0.1.8:11211");
  hash.AddNode("10.0.1.9:11211");
  hash.AddNode("10.0.1.10:11211");
  hash.Final();

  auto count = CountLookups(&hash);
  ASSERT_EQ(count.size(), 10);

  int average = kSampleCount / 10;
  for (const auto& kv : count) {
    EXPECT_LE(DeviationPercent(kv.second, average), 30.0);
  }
}

TEST(KetamaConHashTest, InitWithNodes) {
  KetamaConHash hash;
  std::vector<ConNode> nodes = {{"/sda", 10}, {"/sdb", 10}};
  hash.InitWithNodes(nodes);
  hash.Final();

  ConNode node;
  ASSERT_TRUE(hash.Lookup("anykey", node));
  EXPECT_TRUE(node.key == "/sda" || node.key == "/sdb");
}

TEST(KetamaConHashTest, LookupOnEmptyContinuumReturnsFalse) {
  KetamaConHash hash;
  hash.Final();

  ConNode node;
  EXPECT_FALSE(hash.Lookup("key", node));
}

TEST(KetamaConHashTest, RemoveNode) {
  KetamaConHash hash;
  hash.AddNode("/sda");
  EXPECT_FALSE(hash.RemoveNode("/not-exist"));
  EXPECT_TRUE(hash.RemoveNode("/sda"));
  EXPECT_FALSE(hash.RemoveNode("/sda"));
}

TEST(KetamaConHashTest, Dump) {
  KetamaConHash hash;
  hash.Final();
  hash.Dump();
}

TEST(KetamaConHashTest, InitWithZeroWeightDies) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  KetamaConHash hash;
  std::vector<ConNode> nodes = {{"/sda", 0}};
  EXPECT_DEATH(hash.InitWithNodes(nodes), "");
}

TEST(KetamaConHashTest, AddDuplicateNodeDies) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  KetamaConHash hash;
  hash.AddNode("/sda");
  EXPECT_DEATH(hash.AddNode("/sda"), "");
}

TEST(KetamaConHashTest, AddZeroWeightDies) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  KetamaConHash hash;
  EXPECT_DEATH(hash.AddNode("/sda", 0), "");
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
