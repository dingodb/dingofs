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

#include "common/version.h"

#include <gtest/gtest.h>

namespace dingofs {

TEST(VersionTest, DingoVersionStringContainsAllFields) {
  std::string s = DingoVersionString();
  EXPECT_NE(s.find("DINGOFS VERSION:["), std::string::npos);
  EXPECT_NE(s.find("DINGOFS GIT_LAST_TAG:["), std::string::npos);
  EXPECT_NE(s.find("DINGOFS GIT_BRANCH_NAME:["), std::string::npos);
  EXPECT_NE(s.find("DINGOFS GIT_COMMIT_HASH:["), std::string::npos);
  EXPECT_NE(s.find("DINGOFS PROTO_COMMIT_HASH:["), std::string::npos);
  EXPECT_NE(s.find("DINGOFS BUILD_TYPE:["), std::string::npos);
  EXPECT_NE(s.find("LINK_TCMALLOC:["), std::string::npos);
}

TEST(VersionTest, DingoShortVersionStringIsLowercased) {
  std::string s = DingoShortVersionString();
  for (char c : s) {
    EXPECT_EQ(c, static_cast<char>(tolower(static_cast<unsigned char>(c))));
  }
}

TEST(VersionTest, DingoVersionReturnsExpectedKeysInOrder) {
  auto kvs = DingoVersion();
  ASSERT_EQ(kvs.size(), 7u);
  EXPECT_EQ(kvs[0].first, "BRANCH");
  EXPECT_EQ(kvs[1].first, "COMMIT_HASH");
  EXPECT_EQ(kvs[2].first, "PROTO_COMMIT_HASH");
  EXPECT_EQ(kvs[3].first, "COMMIT_USER");
  EXPECT_EQ(kvs[4].first, "COMMIT_MAIL");
  EXPECT_EQ(kvs[5].first, "COMMIT_TIME");
  EXPECT_EQ(kvs[6].first, "BUILD_TYPE");
}

TEST(VersionTest, GetGitVersionMatchesDingoVersionStringContent) {
  EXPECT_NE(DingoVersionString().find(GetGitVersion()), std::string::npos);
}

TEST(VersionTest, GetGitCommitHashMatchesDingoVersionEntry) {
  auto kvs = DingoVersion();
  EXPECT_EQ(kvs[1].second, GetGitCommitHash());
}

TEST(VersionTest, GetGitCommitTimeMatchesDingoVersionEntry) {
  auto kvs = DingoVersion();
  EXPECT_EQ(kvs[5].second, GetGitCommitTime());
}

TEST(VersionTest, GetProtoGitCommitHashMatchesDingoVersionEntry) {
  auto kvs = DingoVersion();
  EXPECT_EQ(kvs[2].second, GetProtoGitCommitHash());
}

TEST(VersionTest, DingoLogVersionDoesNotCrash) {
  // Purely a logging side-effect; just exercise the code path.
  DingoLogVersion();
}

TEST(VersionTest, ExposeDingoVersionDoesNotCrash) {
  ExposeDingoVersion();
}

}  // namespace dingofs
