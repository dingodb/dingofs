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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <iostream>
#include <string>
#include <utility>

#include "utils/shards.h"

namespace dingofs {
namespace utils {
namespace unit_test {

TEST(ShardTest, position) {
  using Map = std::map<int, std::string>;
  Shards<Map, 16> shard_map_;

  for (int i = 0; i < 10000; ++i) {
    EXPECT_EQ(shard_map_.position(i), shard_map_.position(i));
    EXPECT_GE(shard_map_.position(i), 0);
    EXPECT_LT(shard_map_.position(i), 16);
  }
}

TEST(ShardTest, put_and_get) {
  using Map = std::map<int, std::string>;
  Shards<Map, 16> shard_map_;

  for (int i = 0; i < 1000; ++i) {
    shard_map_.withWLock([i](Map& map) { map[i] = std::to_string(i * 10); }, i);
  }

  for (int i = 0; i < 1000; ++i) {
    shard_map_.withRLock(
        [i](Map& map) {
          auto it = map.find(i);
          ASSERT_NE(it, map.end());
          EXPECT_EQ(it->second, std::to_string(i * 10));
        },
        i);
  }
}

TEST(ShardTest, iterate) {
  using Map = std::map<int, std::string>;
  Shards<Map, 16> shard_map_;

  for (int i = 0; i < 1000; ++i) {
    shard_map_.withWLock([i](Map& map) { map[i] = std::to_string(i * 10); }, i);
  }

  int count = 0;
  shard_map_.iterate([&count](const Map& map) { count += map.size(); });

  EXPECT_EQ(count, 1000);
}

}  // namespace unit_test
}  // namespace utils
}  // namespace dingofs
