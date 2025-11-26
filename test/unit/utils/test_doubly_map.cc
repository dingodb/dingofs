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

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "butil/containers/flat_map.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "utils/doubly_map.h"

namespace dingofs {
namespace utils {

TEST(DoublyMapTest, type) {
  {
    DoublyMap<butil::FlatMap<std::string, std::string>> str_map;
    DoublyMap<butil::FlatMap<std::string, int>> int_map;
  }

  {
    DoublyMap<std::map<std::string, std::string>> str_map;
    DoublyMap<std::map<std::string, int>> int_map;
  }

  {
    DoublyMap<std::unordered_map<std::string, std::string>> str_map;
    DoublyMap<std::unordered_map<std::string, int>> int_map;
  }

  {
    DoublyMap<absl::btree_map<std::string, std::string>> str_map;
    DoublyMap<absl::btree_map<std::string, int>> int_map;
  }

  {
    DoublyMap<absl::flat_hash_map<std::string, std::string>> str_map;
    DoublyMap<absl::flat_hash_map<std::string, int>> int_map;
  }

  EXPECT_TRUE(true);
}

TEST(DoublyMapTest, flat_map_put) {
  DoublyMap<butil::FlatMap<std::string, std::string>> map;
  ASSERT_TRUE(map.Init(1000));

  ASSERT_TRUE(map.Put("key0001", "value0001"));
  ASSERT_EQ(map.Get("key0001"), "value0001");
  ASSERT_EQ(map.Size(), 1);
  ASSERT_TRUE(map.Put("key0002", std::string("value0002")));
  ASSERT_EQ(map.Get("key0002"), "value0002");
  ASSERT_EQ(map.Size(), 2);

  ASSERT_TRUE(map.PutIfExist("key0001", "value0001_updated"));
  ASSERT_EQ(map.Get("key0001"), "value0001_updated");
  ASSERT_EQ(map.Size(), 2);

  ASSERT_FALSE(map.PutIfExist("key0003", "value0003"));
  ASSERT_EQ(map.Get("key0003"), "");
  ASSERT_EQ(map.Size(), 2);

  ASSERT_TRUE(map.PutIfAbsent("key0003", "value0003"));
  ASSERT_EQ(map.Get("key0003"), "value0003");
  ASSERT_EQ(map.Size(), 3);

  ASSERT_FALSE(map.PutIfAbsent("key0002", "value0002_updated"));
  ASSERT_EQ(map.Get("key0002"), "value0002");
  ASSERT_EQ(map.Size(), 3);

  ASSERT_TRUE(map.PutIfNotEqual("key0002", "value0002_updated"));
  ASSERT_EQ(map.Get("key0002"), "value0002_updated");
  ASSERT_EQ(map.Size(), 3);

  ASSERT_TRUE(map.Erase("key0001"));
  ASSERT_EQ(map.Size(), 2);

  ASSERT_TRUE(map.Erase("key0005"));
  ASSERT_EQ(map.Size(), 2);

  ASSERT_TRUE(map.Clear());
  ASSERT_EQ(map.Size(), 0);
}

TEST(DoublyMapTest, flat_map_get) {
  DoublyMap<butil::FlatMap<std::string, std::string>> map;
  ASSERT_TRUE(map.Init(1000));

  ASSERT_TRUE(map.Put("key0001", "value0001"));
  ASSERT_TRUE(map.Put("key0002", "value0002"));
  ASSERT_TRUE(map.Put("key0003", "value0003"));
  ASSERT_TRUE(map.Put("key0004", "value0004"));
  ASSERT_TRUE(map.Put("key0005", "value0005"));
  ASSERT_TRUE(map.Put("key0006", "value0006"));
  ASSERT_TRUE(map.Put("key0007", "value0007"));

  ASSERT_EQ(map.Size(), 7);
  ASSERT_EQ(map.Get("key0001"), "value0001");
  ASSERT_EQ(map.Get("key0002"), "value0002");
  ASSERT_EQ(map.Get("key0003"), "value0003");
  ASSERT_EQ(map.Get("key0004"), "value0004");
  ASSERT_EQ(map.Get("key0005"), "value0005");
  ASSERT_EQ(map.Get("key0006"), "value0006");
  ASSERT_EQ(map.Get("key0007"), "value0007");

  // multi get
  auto values = map.Get({"key0002", "key0004", "key0006"});
  ASSERT_EQ(values.size(), 3);
  std::sort(values.begin(), values.end());  // NOLINT
  ASSERT_EQ(values[0], "value0002");
  ASSERT_EQ(values[1], "value0004");
  ASSERT_EQ(values[2], "value0006");

  // get all
  auto all_values = map.GetAll();
  ASSERT_EQ(all_values.size(), 7);
  std::sort(all_values.begin(), all_values.end());  // NOLINT
  ASSERT_EQ(all_values[0], "value0001");
  ASSERT_EQ(all_values[1], "value0002");
  ASSERT_EQ(all_values[2], "value0003");
  ASSERT_EQ(all_values[3], "value0004");
  ASSERT_EQ(all_values[4], "value0005");
  ASSERT_EQ(all_values[5], "value0006");
  ASSERT_EQ(all_values[6], "value0007");

  // filter
  auto value = map.Filter(
      [](const std::string& k, const std::string&) { return k == "key0005"; });
  ASSERT_EQ(value, "value0005");

  auto value2 = map.Filter([](const std::string&, const std::string& v) {
    return v == "value0005";
  });
  ASSERT_EQ(value2, "value0005");

  // filters
  auto results = map.Filters([](const std::string& k, const std::string&) {
    return k >= "key0003" && k <= "key0005";
  });
  ASSERT_EQ(results.size(), 3);
  std::sort(results.begin(), results.end());  // NOLINT
  ASSERT_EQ(results[0], "value0003");
  ASSERT_EQ(results[1], "value0004");
  ASSERT_EQ(results[2], "value0005");

  // iterate
  std::vector<std::string> iterated_keys;
  map.Iterate([&iterated_keys](const std::string& k, const std::string&) {
    iterated_keys.push_back(k);
  });
  ASSERT_EQ(iterated_keys.size(), 7);
  std::sort(iterated_keys.begin(), iterated_keys.end());  // NOLINT
  ASSERT_EQ(iterated_keys[0], "key0001");
  ASSERT_EQ(iterated_keys[1], "key0002");
  ASSERT_EQ(iterated_keys[2], "key0003");
  ASSERT_EQ(iterated_keys[3], "key0004");
  ASSERT_EQ(iterated_keys[4], "key0005");
  ASSERT_EQ(iterated_keys[5], "key0006");
  ASSERT_EQ(iterated_keys[6], "key0007");
}

TEST(DoublyMapTest, std_map_put) {
  DoublyMap<std::map<std::string, std::string>> map;
  ASSERT_TRUE(map.Init(1000));

  ASSERT_TRUE(map.Put("key0001", "value0001"));
  ASSERT_EQ(map.Get("key0001"), "value0001");
  ASSERT_EQ(map.Size(), 1);
  ASSERT_TRUE(map.Put("key0002", std::string("value0002")));
  ASSERT_EQ(map.Get("key0002"), "value0002");
  ASSERT_EQ(map.Size(), 2);

  ASSERT_TRUE(map.PutIfExist("key0001", "value0001_updated"));
  ASSERT_EQ(map.Get("key0001"), "value0001_updated");
  ASSERT_EQ(map.Size(), 2);

  ASSERT_FALSE(map.PutIfExist("key0003", "value0003"));
  ASSERT_EQ(map.Get("key0003"), "");
  ASSERT_EQ(map.Size(), 2);

  ASSERT_TRUE(map.PutIfAbsent("key0003", "value0003"));
  ASSERT_EQ(map.Get("key0003"), "value0003");
  ASSERT_EQ(map.Size(), 3);

  ASSERT_FALSE(map.PutIfAbsent("key0002", "value0002_updated"));
  ASSERT_EQ(map.Get("key0002"), "value0002");
  ASSERT_EQ(map.Size(), 3);

  ASSERT_TRUE(map.PutIfNotEqual("key0002", "value0002_updated"));
  ASSERT_EQ(map.Get("key0002"), "value0002_updated");
  ASSERT_EQ(map.Size(), 3);

  ASSERT_TRUE(map.Erase("key0001"));
  ASSERT_EQ(map.Size(), 2);

  ASSERT_TRUE(map.Erase("key0005"));
  ASSERT_EQ(map.Size(), 2);

  ASSERT_TRUE(map.Clear());
  ASSERT_EQ(map.Size(), 0);
}

TEST(DoublyMapTest, std_map_get) {
  DoublyMap<std::map<std::string, std::string>> map;
  ASSERT_TRUE(map.Init(1000));

  ASSERT_TRUE(map.Put("key0001", "value0001"));
  ASSERT_TRUE(map.Put("key0002", "value0002"));
  ASSERT_TRUE(map.Put("key0003", "value0003"));
  ASSERT_TRUE(map.Put("key0004", "value0004"));
  ASSERT_TRUE(map.Put("key0005", "value0005"));
  ASSERT_TRUE(map.Put("key0006", "value0006"));
  ASSERT_TRUE(map.Put("key0007", "value0007"));

  ASSERT_EQ(map.Size(), 7);
  ASSERT_EQ(map.Get("key0001"), "value0001");
  ASSERT_EQ(map.Get("key0002"), "value0002");
  ASSERT_EQ(map.Get("key0003"), "value0003");
  ASSERT_EQ(map.Get("key0004"), "value0004");
  ASSERT_EQ(map.Get("key0005"), "value0005");
  ASSERT_EQ(map.Get("key0006"), "value0006");
  ASSERT_EQ(map.Get("key0007"), "value0007");

  // multi get
  auto values = map.Get({"key0002", "key0004", "key0006"});
  ASSERT_EQ(values.size(), 3);
  std::sort(values.begin(), values.end());  // NOLINT
  ASSERT_EQ(values[0], "value0002");
  ASSERT_EQ(values[1], "value0004");
  ASSERT_EQ(values[2], "value0006");

  // get all
  auto all_values = map.GetAll();
  ASSERT_EQ(all_values.size(), 7);
  std::sort(all_values.begin(), all_values.end());  // NOLINT
  ASSERT_EQ(all_values[0], "value0001");
  ASSERT_EQ(all_values[1], "value0002");
  ASSERT_EQ(all_values[2], "value0003");
  ASSERT_EQ(all_values[3], "value0004");
  ASSERT_EQ(all_values[4], "value0005");
  ASSERT_EQ(all_values[5], "value0006");
  ASSERT_EQ(all_values[6], "value0007");

  // filter
  auto value = map.Filter(
      [](const std::string& k, const std::string&) { return k == "key0005"; });
  ASSERT_EQ(value, "value0005");

  auto value2 = map.Filter([](const std::string&, const std::string& v) {
    return v == "value0005";
  });
  ASSERT_EQ(value2, "value0005");

  // filters
  auto results = map.Filters([](const std::string& k, const std::string&) {
    return k >= "key0003" && k <= "key0005";
  });
  ASSERT_EQ(results.size(), 3);
  std::sort(results.begin(), results.end());  // NOLINT
  ASSERT_EQ(results[0], "value0003");
  ASSERT_EQ(results[1], "value0004");
  ASSERT_EQ(results[2], "value0005");

  // iterate
  std::vector<std::string> iterated_keys;
  map.Iterate([&iterated_keys](const std::string& k, const std::string&) {
    iterated_keys.push_back(k);
  });
  ASSERT_EQ(iterated_keys.size(), 7);
  std::sort(iterated_keys.begin(), iterated_keys.end());  // NOLINT
  ASSERT_EQ(iterated_keys[0], "key0001");
  ASSERT_EQ(iterated_keys[1], "key0002");
  ASSERT_EQ(iterated_keys[2], "key0003");
  ASSERT_EQ(iterated_keys[3], "key0004");
  ASSERT_EQ(iterated_keys[4], "key0005");
  ASSERT_EQ(iterated_keys[5], "key0006");
  ASSERT_EQ(iterated_keys[6], "key0007");
}

TEST(DoublyMapTest, absl_btree_map_put) {
  DoublyMap<absl::btree_map<std::string, std::string>> map;
  ASSERT_TRUE(map.Init(1000));

  ASSERT_TRUE(map.Put("key0001", "value0001"));
  ASSERT_EQ(map.Get("key0001"), "value0001");
  ASSERT_EQ(map.Size(), 1);
  ASSERT_TRUE(map.Put("key0002", std::string("value0002")));
  ASSERT_EQ(map.Get("key0002"), "value0002");
  ASSERT_EQ(map.Size(), 2);

  ASSERT_TRUE(map.PutIfExist("key0001", "value0001_updated"));
  ASSERT_EQ(map.Get("key0001"), "value0001_updated");
  ASSERT_EQ(map.Size(), 2);

  ASSERT_FALSE(map.PutIfExist("key0003", "value0003"));
  ASSERT_EQ(map.Get("key0003"), "");
  ASSERT_EQ(map.Size(), 2);

  ASSERT_TRUE(map.PutIfAbsent("key0003", "value0003"));
  ASSERT_EQ(map.Get("key0003"), "value0003");
  ASSERT_EQ(map.Size(), 3);

  ASSERT_FALSE(map.PutIfAbsent("key0002", "value0002_updated"));
  ASSERT_EQ(map.Get("key0002"), "value0002");
  ASSERT_EQ(map.Size(), 3);

  ASSERT_TRUE(map.PutIfNotEqual("key0002", "value0002_updated"));
  ASSERT_EQ(map.Get("key0002"), "value0002_updated");
  ASSERT_EQ(map.Size(), 3);

  ASSERT_TRUE(map.Erase("key0001"));
  ASSERT_EQ(map.Size(), 2);

  ASSERT_TRUE(map.Erase("key0005"));
  ASSERT_EQ(map.Size(), 2);

  ASSERT_TRUE(map.Clear());
  ASSERT_EQ(map.Size(), 0);
}

TEST(DoublyMapTest, absl_btree_map_get) {
  DoublyMap<absl::btree_map<std::string, std::string>> map;
  ASSERT_TRUE(map.Init(1000));

  ASSERT_TRUE(map.Put("key0001", "value0001"));
  ASSERT_TRUE(map.Put("key0002", "value0002"));
  ASSERT_TRUE(map.Put("key0003", "value0003"));
  ASSERT_TRUE(map.Put("key0004", "value0004"));
  ASSERT_TRUE(map.Put("key0005", "value0005"));
  ASSERT_TRUE(map.Put("key0006", "value0006"));
  ASSERT_TRUE(map.Put("key0007", "value0007"));

  ASSERT_EQ(map.Size(), 7);
  ASSERT_EQ(map.Get("key0001"), "value0001");
  ASSERT_EQ(map.Get("key0002"), "value0002");
  ASSERT_EQ(map.Get("key0003"), "value0003");
  ASSERT_EQ(map.Get("key0004"), "value0004");
  ASSERT_EQ(map.Get("key0005"), "value0005");
  ASSERT_EQ(map.Get("key0006"), "value0006");
  ASSERT_EQ(map.Get("key0007"), "value0007");

  // multi get
  auto values = map.Get({"key0002", "key0004", "key0006"});
  ASSERT_EQ(values.size(), 3);
  std::sort(values.begin(), values.end());  // NOLINT
  ASSERT_EQ(values[0], "value0002");
  ASSERT_EQ(values[1], "value0004");
  ASSERT_EQ(values[2], "value0006");

  // get all
  auto all_values = map.GetAll();
  ASSERT_EQ(all_values.size(), 7);
  std::sort(all_values.begin(), all_values.end());  // NOLINT
  ASSERT_EQ(all_values[0], "value0001");
  ASSERT_EQ(all_values[1], "value0002");
  ASSERT_EQ(all_values[2], "value0003");
  ASSERT_EQ(all_values[3], "value0004");
  ASSERT_EQ(all_values[4], "value0005");
  ASSERT_EQ(all_values[5], "value0006");
  ASSERT_EQ(all_values[6], "value0007");

  // filter
  auto value = map.Filter(
      [](const std::string& k, const std::string&) { return k == "key0005"; });
  ASSERT_EQ(value, "value0005");

  auto value2 = map.Filter([](const std::string&, const std::string& v) {
    return v == "value0005";
  });
  ASSERT_EQ(value2, "value0005");

  // filters
  auto results = map.Filters([](const std::string& k, const std::string&) {
    return k >= "key0003" && k <= "key0005";
  });
  ASSERT_EQ(results.size(), 3);
  std::sort(results.begin(), results.end());  // NOLINT
  ASSERT_EQ(results[0], "value0003");
  ASSERT_EQ(results[1], "value0004");
  ASSERT_EQ(results[2], "value0005");

  // iterate
  std::vector<std::string> iterated_keys;
  map.Iterate([&iterated_keys](const std::string& k, const std::string&) {
    iterated_keys.push_back(k);
  });
  ASSERT_EQ(iterated_keys.size(), 7);
  std::sort(iterated_keys.begin(), iterated_keys.end());  // NOLINT
  ASSERT_EQ(iterated_keys[0], "key0001");
  ASSERT_EQ(iterated_keys[1], "key0002");
  ASSERT_EQ(iterated_keys[2], "key0003");
  ASSERT_EQ(iterated_keys[3], "key0004");
  ASSERT_EQ(iterated_keys[4], "key0005");
  ASSERT_EQ(iterated_keys[5], "key0006");
  ASSERT_EQ(iterated_keys[6], "key0007");
}

}  // namespace utils
}  // namespace dingofs
