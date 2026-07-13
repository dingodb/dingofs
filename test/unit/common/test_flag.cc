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

#include "common/flag.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

namespace dingofs {

namespace {

gflags::CommandLineFlagInfo MakeFlagInfo(const std::string& name,
                                         const std::string& description,
                                         const std::string& default_value,
                                         const std::string& current_value,
                                         const std::string& filename,
                                         bool has_validator = false) {
  gflags::CommandLineFlagInfo info;
  info.name = name;
  info.type = "string";
  info.description = description;
  info.default_value = default_value;
  info.current_value = current_value;
  info.filename = filename;
  info.has_validator_fn = has_validator;
  info.is_default = (default_value == current_value);
  return info;
}

}  // namespace

TEST(FlagsHelperTest, GenHelpIncludesProgramUsageAndEachFlag) {
  FlagsInfo flags;
  flags.extra_info.program = "dingo-test";
  flags.extra_info.usage = "dingo-test [options]";
  flags.extra_info.examples = "dingo-test --conf=a.conf";
  flags.gflags.push_back(
      MakeFlagInfo("conf", "Config file path", "", "a.conf", "src/x.cc"));

  std::string help = FlagsHelper::GenHelp(flags);
  EXPECT_NE(help.find("dingo-test"), std::string::npos);
  EXPECT_NE(help.find("dingo-test [options]"), std::string::npos);
  EXPECT_NE(help.find("dingo-test --conf=a.conf"), std::string::npos);
  EXPECT_NE(help.find("--conf"), std::string::npos);
  // Normalize() lowercases descriptions for uniform display casing.
  EXPECT_NE(help.find("config file path"), std::string::npos);
}

TEST(FlagsHelperTest, GenHelpMarksRequiredFlagsWithoutDefault) {
  FlagsInfo flags;
  flags.gflags.push_back(MakeFlagInfo("id", "Node id", /*default_value=*/"",
                                      "5", "src/x.cc",
                                      /*has_validator=*/true));

  std::string help = FlagsHelper::GenHelp(flags);
  EXPECT_NE(help.find("[required]"), std::string::npos);
}

TEST(FlagsHelperTest, GenHelpShowsDefaultValueWhenPresent) {
  FlagsInfo flags;
  flags.gflags.push_back(
      MakeFlagInfo("workers", "Worker count", "4", "4", "src/x.cc"));

  std::string help = FlagsHelper::GenHelp(flags);
  EXPECT_NE(help.find("(default: 4)"), std::string::npos);
}

TEST(FlagsHelperTest, GenTemplateEmitsKeyEqualsDefaultValue) {
  FlagsInfo flags;
  flags.gflags.push_back(
      MakeFlagInfo("workers", "Worker count", "4", "4", "src/x.cc"));

  std::string tmpl = FlagsHelper::GenTemplate(flags);
  EXPECT_NE(tmpl.find("--workers=4\n"), std::string::npos);
}

TEST(FlagsHelperTest, GenTemplateGeneratesUuidForIdFlag) {
  FlagsInfo flags;
  flags.gflags.push_back(MakeFlagInfo("id", "Node id", "", "", "src/x.cc"));

  std::string tmpl = FlagsHelper::GenTemplate(flags);
  // A generated UUID contains hyphens and is not the (empty) default value.
  EXPECT_EQ(tmpl.find("--id=\n"), std::string::npos);
  EXPECT_NE(tmpl.find("--id="), std::string::npos);
}

TEST(FlagsHelperTest, GenCurrentFlagsEmitsKeyEqualsCurrentValue) {
  FlagsInfo flags;
  flags.gflags.push_back(
      MakeFlagInfo("workers", "Worker count", "4", "8", "src/x.cc"));

  std::string current = FlagsHelper::GenCurrentFlags(flags);
  EXPECT_NE(current.find("--workers=8\n"), std::string::npos);
}

TEST(FlagsHelperTest, ResetBrpcFlagDefaultValueIsNoOpForUnregisteredFlags) {
  // None of the brpc-specific flag names (log_dir, max_connection_pool_size,
  // connect_timeout_as_unreachable) are registered in this test binary, so
  // this must simply skip them without crashing.
  EXPECT_NO_FATAL_FAILURE(FlagsHelper::ResetBrpcFlagDefaultValue());
}

TEST(FlagsHelperTest, GetAllGFlagsNeverReturnsFlagsWithEmptyDescription) {
  // Broad pattern so this actually pulls in real registered flags.
  auto out = FlagsHelper::GetAllGFlags("dingo-test", {"src/"});
  ASSERT_FALSE(out.empty());
  for (const auto& f : out) {
    EXPECT_FALSE(f.description.empty());
  }
}

TEST(FlagsHelperTest, GetAllGFlagsFiltersByFileNamePattern) {
  // FLAGS_iops_total_limit (from common/options/blockaccess.cc) is linked
  // into this binary, has a description, and is NOT in FlagsHelper's
  // kGflagWhiteList, so unlike "log_level"/"log_v" it only appears when a
  // pattern actually matches its file.
  auto matched =
      FlagsHelper::GetAllGFlags("dingo-test", {"common/options/blockaccess"});
  bool found = false;
  for (const auto& f : matched) {
    if (f.name == "iops_total_limit") found = true;
  }
  EXPECT_TRUE(found);

  auto unmatched = FlagsHelper::GetAllGFlags("dingo-test", {"no/such/pattern"});
  for (const auto& f : unmatched) {
    EXPECT_NE(f.name, "iops_total_limit");
  }
}

TEST(FlagsHelperTest, GetAllGFlagsAlwaysIncludesWhitelistedFlagsRegardlessOfPattern) {
  // "log_level" is in FlagsHelper's kGflagWhiteList, so the pattern filter is
  // bypassed entirely for it.
  auto flags = FlagsHelper::GetAllGFlags("dingo-test", {"no/such/pattern"});
  bool found = false;
  for (const auto& f : flags) {
    if (f.name == "log_level") found = true;
  }
  EXPECT_TRUE(found);
}

TEST(FlagsHelperTest, GetAllGFlagsResultIsSortedByName) {
  auto flags = FlagsHelper::GetAllGFlags("dingo-test", {"common/"});
  for (size_t i = 1; i < flags.size(); ++i) {
    EXPECT_LE(flags[i - 1].name, flags[i].name);
  }
}

}  // namespace dingofs
