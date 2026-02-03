// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <unistd.h>

#include <cstdio>
#include <iomanip>
#include <string>

#include "cache/iutil/con_hash.h"
#include "cache/iutil/ketama_con_hash.h"
#include "gtest/gtest.h"

namespace dingofs {

namespace cache {
namespace iutil {

TEST(KetamaConHashTest, BaseTest) {
  KetamaConHash hash;
  hash.AddNode("/sda");
  hash.Final();

  std::map<std::string, int> count;
  for (int i = 0; i < 10000; i++) {
    ConNode node;
    bool find = hash.Lookup(std::to_string(i), node);
    EXPECT_TRUE(find);
    EXPECT_EQ(node.key, "/sda");
  }
}

TEST(KetamaConHashTest, DiskDistributeTest) {
  KetamaConHash hash;
  hash.AddNode("/sda");
  hash.AddNode("/sdb");
  hash.AddNode("/sdc");
  hash.AddNode("/sdd");
  hash.AddNode("/sde");
  hash.Final();

  std::map<std::string, int> count;
  // 100 w
  // avg 20w
  int ave = 1000000 / 5;
  for (int i = 0; i < 1000000; i++) {
    ConNode node;
    bool find = hash.Lookup(std::to_string(i), node);
    EXPECT_TRUE(find);
    count[node.key]++;
  }

  for (auto& kv : count) {
    double deviation = static_cast<double>(kv.second - ave) / ave * 100.0;
    deviation = std::abs(deviation);
    EXPECT_LE(deviation, 20.0);
    std::cout << kv.first << " " << kv.second << " " << std::fixed
              << std::setprecision(2) << deviation << "%" << std::endl;
  }
}

// 100 w test too slow, use 10 w
TEST(KetamaConHashTest, ReDiskDistributeTest) {
  KetamaConHash hash;
  hash.AddNode("/sda");
  hash.AddNode("/sdb");
  hash.AddNode("/sdc");
  hash.AddNode("/sdd");
  hash.AddNode("/sde");
  hash.Final();

  std::map<std::string, std::vector<int>> disk_keys;
  {
    // 10 w
    // avg 2w
    for (int i = 0; i < 100000; i++) {
      ConNode node;
      bool find = hash.Lookup(std::to_string(i), node);
      EXPECT_TRUE(find);
      disk_keys[node.key].push_back(i);
    }

    for (auto& disk_keys : disk_keys) {
      std::cout << "5 disks " << disk_keys.first << " "
                << disk_keys.second.size() << std::endl;
      EXPECT_GT(disk_keys.second.size(), 15000);  // 1.5w
    }
  }

  std::map<std::string, std::vector<int>> re_distri_disk_keys;
  {
    bool res = hash.RemoveNode("/sdc");
    EXPECT_TRUE(res);
    hash.Final();

    // 10 w
    // avg 2.5w
    for (int i = 0; i < 100000; i++) {
      ConNode node;
      bool find = hash.Lookup(std::to_string(i), node);
      EXPECT_TRUE(find);
      EXPECT_NE(node.key, "/sdc");

      re_distri_disk_keys[node.key].push_back(i);
    }

    for (auto& disk_keys : re_distri_disk_keys) {
      std::cout << "4 disks " << disk_keys.first << " "
                << disk_keys.second.size() << std::endl;
      EXPECT_GT(disk_keys.second.size(), 20000);  // 2w
    }
  }

  // CheckRedistribution
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
      EXPECT_EQ(original_keys.size(), 0);
    }

    std::cout << kv.first << " added_key_count:" << added_disk_key_count
              << std::endl;
  }
}

TEST(KetamaConHashTest, WeightTest) {
  KetamaConHash hash;
  hash.AddNode("/sda", 5);
  hash.AddNode("/sdb", 10);
  hash.AddNode("/sdc", 20);
  hash.Final();

  std::map<std::string, int> count;

  // 100 w
  for (int i = 0; i < 1000000; i++) {
    ConNode node;
    bool find = hash.Lookup(std::to_string(i), node);
    EXPECT_TRUE(find);
    count[node.key]++;
  }

  std::map<std::string, int> expect_count;
  expect_count["/sda"] = (5 * 1000000) / (5 + 10 + 20);
  expect_count["/sdb"] = (10 * 1000000) / (5 + 10 + 20);
  expect_count["/sdc"] = (20 * 1000000) / (5 + 10 + 20);

  for (auto& kv : count) {
    int expect = expect_count[kv.first];
    double deviation = static_cast<double>(kv.second - expect) / expect * 100.0;
    deviation = std::abs(deviation);
    EXPECT_LE(deviation, 10);
    std::cout << kv.first << " " << kv.second << " " << std::fixed
              << std::setprecision(2) << deviation << "%" << std::endl;
  }
}

TEST(KetamaConHashTest, IpDistributeTest) {
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

  std::map<std::string, int> count;
  // 100 w
  // avg 10w
  int ave = 1000000 / 10;
  for (int i = 0; i < 1000000; i++) {
    ConNode node;
    bool find = hash.Lookup(std::to_string(i), node);
    EXPECT_TRUE(find);
    count[node.key]++;
  }

  for (auto& kv : count) {
    double deviation = static_cast<double>(kv.second - ave) / ave * 100.0;
    deviation = std::abs(deviation);
    EXPECT_LE(deviation, 20.0);
    std::cout << kv.first << " " << kv.second << " " << std::fixed
              << std::setprecision(2) << deviation << "%" << std::endl;
  }
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
