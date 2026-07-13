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

#include "common/helper.h"

#include <fcntl.h>
#include <gtest/gtest.h>

#include <filesystem>

namespace dingofs {

// ------------------------- ParseMetaURL -------------------------

TEST(HelperTest, ParseMetaURLParsesMdsScheme) {
  MetaSystemType type;
  std::string addrs, fs_name, storage_info;
  ASSERT_TRUE(Helper::ParseMetaURL("mds://127.0.0.1:7800/testfs", type, addrs,
                                   fs_name, storage_info));
  EXPECT_EQ(type, MetaSystemType::MDS);
  EXPECT_EQ(addrs, "127.0.0.1:7800");
  EXPECT_EQ(fs_name, "testfs");
}

TEST(HelperTest, ParseMetaURLParsesLocalSchemeWithoutQuery) {
  MetaSystemType type;
  std::string addrs, fs_name, storage_info;
  ASSERT_TRUE(
      Helper::ParseMetaURL("local://dingofs", type, addrs, fs_name, storage_info));
  EXPECT_EQ(type, MetaSystemType::LOCAL);
  EXPECT_EQ(fs_name, "dingofs");
  EXPECT_TRUE(storage_info.empty());
}

TEST(HelperTest, ParseMetaURLParsesLocalSchemeWithQuery) {
  MetaSystemType type;
  std::string addrs, fs_name, storage_info;
  ASSERT_TRUE(Helper::ParseMetaURL("local://dingofs?storage=file&path=/tmp/data",
                                   type, addrs, fs_name, storage_info));
  EXPECT_EQ(fs_name, "dingofs");
  EXPECT_EQ(storage_info, "storage=file&path=/tmp/data");
}

TEST(HelperTest, ParseMetaURLParsesMemoryScheme) {
  MetaSystemType type;
  std::string addrs, fs_name, storage_info;
  ASSERT_TRUE(
      Helper::ParseMetaURL("memory://memory_fs", type, addrs, fs_name, storage_info));
  EXPECT_EQ(type, MetaSystemType::MEMORY);
  EXPECT_EQ(fs_name, "memory_fs");
}

TEST(HelperTest, ParseMetaURLRejectsMissingProtocolSeparator) {
  MetaSystemType type;
  std::string addrs, fs_name, storage_info;
  EXPECT_FALSE(
      Helper::ParseMetaURL("not-a-url", type, addrs, fs_name, storage_info));
}

TEST(HelperTest, ParseMetaURLRejectsMissingSlashForMds) {
  MetaSystemType type;
  std::string addrs, fs_name, storage_info;
  EXPECT_FALSE(Helper::ParseMetaURL("mds://127.0.0.1:7800", type, addrs,
                                    fs_name, storage_info));
}

// ------------------------- string helpers -------------------------

TEST(HelperTest, ToLowerCaseConvertsAsciiUppercase) {
  EXPECT_EQ(Helper::ToLowerCase("HeLLo-World_123"), "hello-world_123");
}

TEST(HelperTest, RemoveHttpPrefixStripsHttpsPreservingCase) {
  EXPECT_EQ(Helper::RemoveHttpPrefix("https://MyBucket.S3.Example.com"),
           "MyBucket.S3.Example.com");
}

TEST(HelperTest, RemoveHttpPrefixStripsHttpPreservingCase) {
  EXPECT_EQ(Helper::RemoveHttpPrefix("http://MyBucket.S3.Example.com"),
           "MyBucket.S3.Example.com");
}

TEST(HelperTest, RemoveHttpPrefixLeavesUnprefixedHostUnchanged) {
  // Regression: previously fell through to a fully-lowercased copy of the
  // input even when there was no http(s):// prefix to remove.
  EXPECT_EQ(Helper::RemoveHttpPrefix("MyBucket.S3.Example.com"),
           "MyBucket.S3.Example.com");
}

TEST(HelperTest, SplitUniteCacheDirParsesSizeSuffix) {
  std::vector<std::pair<std::string, uint64_t>> dirs;
  Helper::SplitUniteCacheDir("/data1:100,/data2:200", 50, &dirs);
  ASSERT_EQ(dirs.size(), 2u);
  EXPECT_EQ(dirs[0], std::make_pair(std::string("/data1"), uint64_t(100)));
  EXPECT_EQ(dirs[1], std::make_pair(std::string("/data2"), uint64_t(200)));
}

TEST(HelperTest, SplitUniteCacheDirUsesDefaultSizeWhenOmitted) {
  std::vector<std::pair<std::string, uint64_t>> dirs;
  Helper::SplitUniteCacheDir("/data1", 77, &dirs);
  ASSERT_EQ(dirs.size(), 1u);
  EXPECT_EQ(dirs[0], std::make_pair(std::string("/data1"), uint64_t(77)));
}

TEST(HelperTest, SplitStringToStringsSplitsOnDelimiter) {
  std::vector<std::string> out;
  Helper::SplitString("a,b,,c", ',', out);
  EXPECT_EQ(out, (std::vector<std::string>{"a", "b", "", "c"}));
}

TEST(HelperTest, SplitStringToInt64ParsesEachToken) {
  std::vector<int64_t> out;
  Helper::SplitString("1,2,3", ',', out);
  EXPECT_EQ(out, (std::vector<int64_t>{1, 2, 3}));
}

TEST(HelperTest, SplitStringToInt64SkipsUnparsableTokens) {
  std::vector<int64_t> out;
  Helper::SplitString("1,notanumber,3", ',', out);
  EXPECT_EQ(out, (std::vector<int64_t>{1, 3}));
}

TEST(HelperTest, DescOpenFlagsDescribesReadWriteModes) {
  EXPECT_STREQ(Helper::DescOpenFlags(O_RDONLY), "RDONLY");
  EXPECT_STREQ(Helper::DescOpenFlags(O_WRONLY), "WRONLY");
  EXPECT_STREQ(Helper::DescOpenFlags(O_WRONLY | O_TRUNC), "WRONLY|TRUNC");
  EXPECT_STREQ(Helper::DescOpenFlags(O_WRONLY | O_APPEND), "WRONLY|APPEND");
  EXPECT_STREQ(Helper::DescOpenFlags(O_RDWR), "RDWR");
  EXPECT_STREQ(Helper::DescOpenFlags(O_RDWR | O_TRUNC), "RDWR|TRUNC");
  EXPECT_STREQ(Helper::DescOpenFlags(O_RDWR | O_APPEND), "RDWR|APPEND");
}

TEST(HelperTest, Char2AddrFormatsPointerAsHex) {
  int value = 0;
  std::string addr = Helper::Char2Addr(reinterpret_cast<const char*>(&value));
  EXPECT_EQ(addr.rfind("0x", 0), 0u);
}

// ------------------------- filesystem helpers -------------------------

TEST(HelperTest, CreateDirectoryThenIsExistPath) {
  std::string dir = (std::filesystem::temp_directory_path() /
                     "dingofs_helper_test_dir")
                        .string();
  std::filesystem::remove_all(dir);
  EXPECT_FALSE(Helper::IsExistPath(dir));
  ASSERT_TRUE(Helper::CreateDirectory(dir));
  EXPECT_TRUE(Helper::IsExistPath(dir));
  // Creating an already-existing directory is a no-op success, not a failure.
  EXPECT_TRUE(Helper::CreateDirectory(dir));
  std::filesystem::remove_all(dir);
}

TEST(HelperTest, ToCanonicalPathResolvesDotSegments) {
  std::string canonical = Helper::ToCanonicalPath("/tmp/./a/../b");
  EXPECT_EQ(canonical, "/tmp/b");
}

TEST(HelperTest, ExpandPathReplacesHomeTilde) {
  std::string home = Helper::GetHomeDir();
  EXPECT_EQ(Helper::ExpandPath("~/data"), home + "/data");
}

}  // namespace dingofs
