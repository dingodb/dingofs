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

#include <string>
#include <unordered_map>
#include <vector>

#include "cache/iutil/ketama_con_hash.h"

namespace dingofs {
namespace cache {
namespace iutil {

class KetamaConHashTest : public ::testing::Test {
 protected:
  void SetUp() override { hash_ = std::make_unique<KetamaConHash>(); }

  std::unique_ptr<KetamaConHash> hash_;
};

TEST_F(KetamaConHashTest, InitWithNodes) {
  std::vector<ConNode> nodes = {
      {.key = "node1", .weight = 10},
      {.key = "node2", .weight = 20},
      {.key = "node3", .weight = 30},
  };

  hash_->InitWithNodes(nodes);
  hash_->Final();

  ConNode result;
  EXPECT_TRUE(hash_->Lookup("test_key", result));
}

TEST_F(KetamaConHashTest, AddNode) {
  hash_->AddNode("node1", 10);
  hash_->AddNode("node2", 20);
  hash_->Final();

  ConNode result;
  EXPECT_TRUE(hash_->Lookup("test_key", result));
  EXPECT_TRUE(result.key == "node1" || result.key == "node2");
}

TEST_F(KetamaConHashTest, AddNodeWithConNode) {
  ConNode node1 = {.key = "node1", .weight = 10};
  ConNode node2 = {.key = "node2", .weight = 20};

  hash_->AddNode(node1);
  hash_->AddNode(node2);
  hash_->Final();

  ConNode result;
  EXPECT_TRUE(hash_->Lookup("test_key", result));
}

TEST_F(KetamaConHashTest, RemoveNode) {
  hash_->AddNode("node1", 10);
  hash_->AddNode("node2", 20);

  EXPECT_TRUE(hash_->RemoveNode("node1"));
  EXPECT_FALSE(hash_->RemoveNode("node1"));
  EXPECT_FALSE(hash_->RemoveNode("not_exist"));

  hash_->Final();

  ConNode result;
  EXPECT_TRUE(hash_->Lookup("test_key", result));
  EXPECT_EQ(result.key, "node2");
}

TEST_F(KetamaConHashTest, LookupEmpty) {
  hash_->Final();

  ConNode result;
  EXPECT_FALSE(hash_->Lookup("test_key", result));
}

TEST_F(KetamaConHashTest, LookupConsistency) {
  hash_->AddNode("node1", 10);
  hash_->AddNode("node2", 10);
  hash_->AddNode("node3", 10);
  hash_->Final();

  ConNode result1, result2;
  EXPECT_TRUE(hash_->Lookup("same_key", result1));
  EXPECT_TRUE(hash_->Lookup("same_key", result2));
  EXPECT_EQ(result1.key, result2.key);
}

TEST_F(KetamaConHashTest, Distribution) {
  hash_->AddNode("node1", 10);
  hash_->AddNode("node2", 10);
  hash_->AddNode("node3", 10);
  hash_->Final();

  std::unordered_map<std::string, int> distribution;
  for (int i = 0; i < 1000; i++) {
    ConNode result;
    std::string key = "key_" + std::to_string(i);
    EXPECT_TRUE(hash_->Lookup(key, result));
    distribution[result.key]++;
  }

  EXPECT_EQ(distribution.size(), 3);
  for (const auto& kv : distribution) {
    EXPECT_GT(kv.second, 100);
  }
}

TEST_F(KetamaConHashTest, WeightDistribution) {
  hash_->AddNode("node1", 10);
  hash_->AddNode("node2", 20);
  hash_->Final();

  std::unordered_map<std::string, int> distribution;
  for (int i = 0; i < 3000; i++) {
    ConNode result;
    std::string key = "key_" + std::to_string(i);
    EXPECT_TRUE(hash_->Lookup(key, result));
    distribution[result.key]++;
  }

  EXPECT_GT(distribution["node2"], distribution["node1"]);
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
