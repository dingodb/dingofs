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

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "test/unit/coverage/coverage.h"

namespace dingofs::unit_test {

// Not part of the public coverage.h API: exposed here only so this test can
// exercise the report-completion behavior extracted from RunCoverage().
int FinishCoverageReport(const std::string& html, int test_rc);

TEST(CoverageTest, UsesConfiguredSourceDirectory) {
  EXPECT_TRUE(std::filesystem::exists(CoverageSourceDir() + "/CMakeLists.txt"));
  EXPECT_TRUE(std::filesystem::exists(CoverageSourceDir() + "/src/common"));
}

TEST(CoverageTest, BuildsTargetScopedGcovrCommand) {
  const CoverageConfig config{"test_mds", "src/mds/"};
  const std::string command = BuildGcovrCommand(config, "/repo", "/repo/build");

  EXPECT_NE(command.find("--filter '/repo/src/mds/'"), std::string::npos);
  EXPECT_NE(command.find("--html-details "
                         "'/repo/build/coverage/test_mds/index.html'"),
            std::string::npos);
  EXPECT_NE(command.find("--csv "
                         "'/repo/build/coverage/test_mds/summary.csv'"),
            std::string::npos);
  EXPECT_NE(command.find("' '/repo/build'"), std::string::npos);
}

TEST(CoverageTest, GcovrCommandExcludesConfiguredPaths) {
  const CoverageConfig config{"test_common", "src/common/",
                              {"src/common/blockaccess/bench/.*",
                               "src/common/blockaccess/rados/.*"}};
  const std::string command = BuildGcovrCommand(config, "/repo", "/repo/build");

  EXPECT_NE(command.find("--exclude '/repo/src/common/blockaccess/bench/.*'"),
            std::string::npos);
  EXPECT_NE(command.find("--exclude '/repo/src/common/blockaccess/rados/.*'"),
            std::string::npos);
}

TEST(CoverageTest, ReportCompletionReturnsTestResult) {
  EXPECT_EQ(0, FinishCoverageReport("/tmp/index.html", 0));
  EXPECT_EQ(1, FinishCoverageReport("/tmp/index.html", 1));
}
}  // namespace dingofs::unit_test
