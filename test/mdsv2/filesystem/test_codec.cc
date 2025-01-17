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

#include <fmt/format.h>

#include <string>

#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/filesystem/codec.h"

namespace dingofs {
namespace mdsv2 {
namespace unit_test {

class MetaDataCodecTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(MetaDataCodecTest, GetFsTableRange) {
  {
    std::string start_key;
    std::string end_key;
    MetaDataCodec::GetFsTableRange(start_key, end_key);

    LOG(INFO) << fmt::format("fs table start_key: {} end_key: {}", Helper::StringToHex(start_key),
                             Helper::StringToHex(end_key));
  }

  {
    std::string start_key;
    std::string end_key;
    MetaDataCodec::GetDentryTableRange(1, start_key, end_key);

    LOG(INFO) << fmt::format("dentry table start_key: {} end_key: {}", Helper::StringToHex(start_key),
                             Helper::StringToHex(end_key));
  }

  {
    std::string start_key;
    std::string end_key;
    MetaDataCodec::GetFileInodeTableRange(1, start_key, end_key);

    LOG(INFO) << fmt::format("file inode table start_key: {} end_key: {}", Helper::StringToHex(start_key),
                             Helper::StringToHex(end_key));
  }
}

TEST_F(MetaDataCodecTest, EncodeFSKey) {
  {
    std::string expected_name = "test";
    std::string key = MetaDataCodec::EncodeFSKey(expected_name);

    LOG(INFO) << fmt::format("key: {}", Helper::StringToHex(key));

    std::string actual_name;
    MetaDataCodec::DecodeFSKey(key, actual_name);
    EXPECT_EQ(expected_name, actual_name);
  }

  {
    std::string expected_name = "a";
    std::string key = MetaDataCodec::EncodeFSKey(expected_name);

    std::string actual_name;
    MetaDataCodec::DecodeFSKey(key, actual_name);
    EXPECT_EQ(expected_name, actual_name);
  }

  {
    std::string expected_name = "a1232";
    std::string key = MetaDataCodec::EncodeFSKey(expected_name);

    std::string actual_name;
    MetaDataCodec::DecodeFSKey(key, actual_name);
    EXPECT_EQ(expected_name, actual_name);
  }
}

TEST_F(MetaDataCodecTest, EncodeDentryKey) {
  {
    int expected_fs_id = 1;
    uint64_t expected_inode_id = 12345;
    std::string expected_name = "dentry";
    std::string key = MetaDataCodec::EncodeDentryKey(expected_fs_id, expected_inode_id, expected_name);

    int actual_fs_id;
    uint64_t actual_inode_id;
    std::string actual_name;
    MetaDataCodec::DecodeDentryKey(key, actual_fs_id, actual_inode_id, actual_name);
    EXPECT_EQ(expected_fs_id, actual_fs_id);
    EXPECT_EQ(expected_inode_id, actual_inode_id);
    EXPECT_EQ(expected_name, actual_name);
  }

  {
    int expected_fs_id = 1000;
    uint64_t expected_inode_id = 93432234;
    std::string expected_name = "dentryfdsaf";
    std::string key = MetaDataCodec::EncodeDentryKey(expected_fs_id, expected_inode_id, expected_name);

    int actual_fs_id;
    uint64_t actual_inode_id;
    std::string actual_name;
    MetaDataCodec::DecodeDentryKey(key, actual_fs_id, actual_inode_id, actual_name);
    EXPECT_EQ(expected_fs_id, actual_fs_id);
    EXPECT_EQ(expected_inode_id, actual_inode_id);
    EXPECT_EQ(expected_name, actual_name);
  }
}

TEST_F(MetaDataCodecTest, EncodeDirInodeKey) {
  {
    int expected_fs_id = 1;
    uint64_t expected_inode_id = 12345;
    std::string key = MetaDataCodec::EncodeDirInodeKey(expected_fs_id, expected_inode_id);

    int actual_fs_id;
    uint64_t actual_inode_id;
    MetaDataCodec::DecodeDirInodeKey(key, actual_fs_id, actual_inode_id);
    EXPECT_EQ(expected_fs_id, actual_fs_id);
    EXPECT_EQ(expected_inode_id, actual_inode_id);
  }

  {
    int expected_fs_id = 100;
    uint64_t expected_inode_id = 123434345;
    std::string key = MetaDataCodec::EncodeDirInodeKey(expected_fs_id, expected_inode_id);

    int actual_fs_id;
    uint64_t actual_inode_id;
    MetaDataCodec::DecodeDirInodeKey(key, actual_fs_id, actual_inode_id);
    EXPECT_EQ(expected_fs_id, actual_fs_id);
    EXPECT_EQ(expected_inode_id, actual_inode_id);
  }
}

TEST_F(MetaDataCodecTest, EncodeFileInodeKey) {
  {
    int expected_fs_id = 1;
    uint64_t expected_inode_id = 12345;
    std::string key = MetaDataCodec::EncodeFileInodeKey(expected_fs_id, expected_inode_id);

    int actual_fs_id;
    uint64_t actual_inode_id;
    MetaDataCodec::DecodeFileInodeKey(key, actual_fs_id, actual_inode_id);
    EXPECT_EQ(expected_fs_id, actual_fs_id);
    EXPECT_EQ(expected_inode_id, actual_inode_id);
  }
  {
    int expected_fs_id = 1000;
    uint64_t expected_inode_id = 12397745;
    std::string key = MetaDataCodec::EncodeFileInodeKey(expected_fs_id, expected_inode_id);

    int actual_fs_id;
    uint64_t actual_inode_id;
    MetaDataCodec::DecodeFileInodeKey(key, actual_fs_id, actual_inode_id);
    EXPECT_EQ(expected_fs_id, actual_fs_id);
    EXPECT_EQ(expected_inode_id, actual_inode_id);
  }
}

}  // namespace unit_test
}  // namespace mdsv2
}  // namespace dingofs