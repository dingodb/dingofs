// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include <gtest/gtest.h>

#include <regex>
#include <unordered_set>

#include "utils/uuid.h"

namespace dingofs {
namespace utils {
namespace unit_test {

// canonical UUID form: 8-4-4-4-12 lowercase hex digits separated by dashes.
static const std::regex kUuidPattern(
    "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

TEST(UuidTest, GenerateUUIDMatchesCanonicalFormat) {
  std::string uuid = GenerateUUID();
  EXPECT_EQ(uuid.size(), 36U);
  EXPECT_TRUE(std::regex_match(uuid, kUuidPattern)) << uuid;
}

TEST(UuidTest, GenerateUUIDTimeMatchesCanonicalFormat) {
  std::string uuid = GenerateUUIDTime();
  EXPECT_EQ(uuid.size(), 36U);
  EXPECT_TRUE(std::regex_match(uuid, kUuidPattern)) << uuid;
}

TEST(UuidTest, GenerateUUIDRandomMatchesCanonicalFormat) {
  std::string uuid = GenerateUUIDRandom();
  EXPECT_EQ(uuid.size(), 36U);
  EXPECT_TRUE(std::regex_match(uuid, kUuidPattern)) << uuid;
}

TEST(UuidTest, GeneratedUUIDsAreUnique) {
  std::unordered_set<std::string> seen;
  for (int i = 0; i < 1000; ++i) {
    EXPECT_TRUE(seen.insert(GenerateUUID()).second)
        << "duplicate uuid generated";
  }
}

}  // namespace unit_test
}  // namespace utils
}  // namespace dingofs
