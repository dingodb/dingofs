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

#include "mds/common/type.h"

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace mds {
namespace unit_test {

class TypeTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(TypeTest, IsDir) {
  // Directory ino has LSB set to 1
  EXPECT_TRUE(IsDir(1));    // 0b1
  EXPECT_TRUE(IsDir(3));    // 0b11
  EXPECT_TRUE(IsDir(5));    // 0b101
  EXPECT_TRUE(IsDir(7));    // 0b111
  EXPECT_TRUE(IsDir(0xFF)); // 0b11111111
}

TEST_F(TypeTest, IsFile) {
  // File ino has LSB set to 0
  EXPECT_TRUE(IsFile(0));   // 0b0
  EXPECT_TRUE(IsFile(2));   // 0b10
  EXPECT_TRUE(IsFile(4));   // 0b100
  EXPECT_TRUE(IsFile(6));   // 0b110
  EXPECT_TRUE(IsFile(0xFE)); // 0b11111110
}

TEST_F(TypeTest, IsDirAndIsFileMutuallyExclusive) {
  // Any number is either a dir or a file, never both
  for (Ino i = 0; i < 100; ++i) {
    EXPECT_NE(IsDir(i), IsFile(i));
  }
}

TEST_F(TypeTest, RangeToString) {
  Range range;
  range.start = "abc";
  range.end = "xyz";

  EXPECT_EQ(range.ToString(), "[abc, xyz)");
}

TEST_F(TypeTest, IntRangeToString) {
  IntRange range;
  range.start = 100;
  range.end = 200;

  EXPECT_EQ(range.ToString(), "[100, 200)");
}

TEST_F(TypeTest, S3InfoValidation) {
  {
    S3Info info;
    info.ak = "access_key";
    info.sk = "secret_key";
    info.endpoint = "http://s3.example.com";
    info.bucket_name = "mybucket";
    info.object_name = "myobject";

    EXPECT_TRUE(info.Validate());
  }

  {
    S3Info info;
    info.ak = "";
    info.sk = "secret_key";
    info.endpoint = "http://s3.example.com";
    info.bucket_name = "mybucket";
    info.object_name = "myobject";

    EXPECT_FALSE(info.Validate());
  }

  {
    S3Info info;
    info.ak = "access_key";
    info.sk = "";
    info.endpoint = "http://s3.example.com";
    info.bucket_name = "mybucket";
    info.object_name = "myobject";

    EXPECT_FALSE(info.Validate());
  }

  {
    S3Info info;
    info.ak = "access_key";
    info.sk = "secret_key";
    info.endpoint = "";
    info.bucket_name = "mybucket";
    info.object_name = "myobject";

    EXPECT_FALSE(info.Validate());
  }

  {
    S3Info info;
    info.ak = "access_key";
    info.sk = "secret_key";
    info.endpoint = "http://s3.example.com";
    info.bucket_name = "";
    info.object_name = "myobject";

    EXPECT_FALSE(info.Validate());
  }

  {
    S3Info info;
    info.ak = "access_key";
    info.sk = "secret_key";
    info.endpoint = "http://s3.example.com";
    info.bucket_name = "mybucket";
    info.object_name = "";

    EXPECT_FALSE(info.Validate());
  }

  {
    S3Info info;
    // All empty
    EXPECT_FALSE(info.Validate());
  }
}

TEST_F(TypeTest, S3InfoToString) {
  S3Info info;
  info.ak = "access_key";
  info.sk = "secret_key";
  info.endpoint = "http://s3.example.com";
  info.bucket_name = "mybucket";
  info.object_name = "myobject";

  std::string str = info.ToString();
  EXPECT_NE(str.find("access_key"), std::string::npos);
  EXPECT_NE(str.find("secret_key"), std::string::npos);
  EXPECT_NE(str.find("s3.example.com"), std::string::npos);
  EXPECT_NE(str.find("mybucket"), std::string::npos);
  EXPECT_NE(str.find("myobject"), std::string::npos);
}

TEST_F(TypeTest, RadosInfoDefaultValues) {
  RadosInfo info;

  EXPECT_EQ(info.mon_host, "");
  EXPECT_EQ(info.user_name, "");
  EXPECT_EQ(info.key, "");
  EXPECT_EQ(info.pool_name, "");
  EXPECT_EQ(info.cluster_name, "ceph"); // Default value
}

TEST_F(TypeTest, RadosInfoCustomValues) {
  RadosInfo info;
  info.mon_host = "mon1,mon2,mon3";
  info.user_name = "admin";
  info.key = "secret_key";
  info.pool_name = "my_pool";
  info.cluster_name = "my_cluster";

  EXPECT_EQ(info.mon_host, "mon1,mon2,mon3");
  EXPECT_EQ(info.user_name, "admin");
  EXPECT_EQ(info.key, "secret_key");
  EXPECT_EQ(info.pool_name, "my_pool");
  EXPECT_EQ(info.cluster_name, "my_cluster");
}

TEST_F(TypeTest, LocalFileInfo) {
  LocalFileInfo info;
  info.path = "/path/to/file";

  EXPECT_EQ(info.path, "/path/to/file");
}

TEST_F(TypeTest, InoTypeAlias) {
  // Verify Ino is an alias for uint64_t
  Ino ino = 12345;
  EXPECT_EQ(sizeof(ino), sizeof(uint64_t));
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
