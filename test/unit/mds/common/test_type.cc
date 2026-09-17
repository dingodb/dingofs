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

namespace {

AttrMutationEntry MakeMutation(uint32_t index, uint64_t delta_version) {
  AttrMutationEntry mutation;
  mutation.set_ino(1);
  mutation.set_index(index);
  mutation.set_delta_version(delta_version);
  return mutation;
}

}  // namespace

TEST_F(TypeTest, AttrVersionVecConstructFromBaseVersion) {
  AttrVersionVec version_vec(100);

  EXPECT_EQ(version_vec.BaseVersion(), 100);
  EXPECT_EQ(version_vec.CompleteVersion(), 100);
  EXPECT_EQ(version_vec.total_delta_version, 0);
  ASSERT_EQ(version_vec.delta_versions.size(), kDirAttrMutationNum);
  for (uint32_t i = 0; i < kDirAttrMutationNum; ++i) {
    EXPECT_EQ(version_vec.DeltaVersion(i), 0) << "index=" << i;
  }
  EXPECT_EQ(version_vec.ToString(), "100 0");
}

TEST_F(TypeTest, AttrVersionVecConstructFromAttrWithMutation) {
  AttrWithMutation attr_with_mutation;
  attr_with_mutation.attr.set_ino(1);
  attr_with_mutation.attr.set_version(50);
  attr_with_mutation.mutations.push_back(MakeMutation(2, 3));
  attr_with_mutation.mutations.push_back(MakeMutation(5, 7));

  AttrVersionVec version_vec(attr_with_mutation);

  EXPECT_EQ(version_vec.BaseVersion(), 50);
  EXPECT_EQ(version_vec.DeltaVersion(2), 3);
  EXPECT_EQ(version_vec.DeltaVersion(5), 7);
  EXPECT_EQ(version_vec.DeltaVersion(0), 0);
  EXPECT_EQ(version_vec.total_delta_version, 10);
  EXPECT_EQ(version_vec.CompleteVersion(), 60);
  EXPECT_EQ(version_vec.ToString(), "50 10");
}

TEST_F(TypeTest, AttrVersionVecDedupsRepeatedMutationSlots) {
  // Retried operations may append the same slot twice; the version must not inflate.
  AttrWithMutation attr_with_mutation;
  attr_with_mutation.attr.set_ino(1);
  attr_with_mutation.attr.set_version(50);
  attr_with_mutation.mutations.push_back(MakeMutation(2, 3));
  attr_with_mutation.mutations.push_back(MakeMutation(5, 7));
  attr_with_mutation.mutations.push_back(MakeMutation(2, 3));
  attr_with_mutation.mutations.push_back(MakeMutation(5, 7));

  EXPECT_EQ(attr_with_mutation.TotalDeltaVersion(), 10);
  EXPECT_EQ(attr_with_mutation.ToCompleteAttr().version(), 60);

  AttrVersionVec version_vec(attr_with_mutation);
  EXPECT_EQ(version_vec.total_delta_version, 10);
  EXPECT_EQ(version_vec.CompleteVersion(), 60);
}

TEST_F(TypeTest, AttrVersionVecPutIfAttrMutationEntry) {
  AttrVersionVec version_vec(10);

  // newer delta is accepted and total is adjusted
  EXPECT_TRUE(version_vec.PutIf(MakeMutation(1, 5)));
  EXPECT_EQ(version_vec.DeltaVersion(1), 5);
  EXPECT_EQ(version_vec.total_delta_version, 5);
  EXPECT_EQ(version_vec.CompleteVersion(), 15);

  // equal or older delta is rejected
  EXPECT_FALSE(version_vec.PutIf(MakeMutation(1, 5)));
  EXPECT_FALSE(version_vec.PutIf(MakeMutation(1, 3)));
  EXPECT_EQ(version_vec.DeltaVersion(1), 5);

  // larger delta only bumps the total by the difference
  EXPECT_TRUE(version_vec.PutIf(MakeMutation(1, 8)));
  EXPECT_EQ(version_vec.DeltaVersion(1), 8);
  EXPECT_EQ(version_vec.total_delta_version, 8);

  // independent index accumulates
  EXPECT_TRUE(version_vec.PutIf(MakeMutation(2, 4)));
  EXPECT_EQ(version_vec.total_delta_version, 12);
}

TEST_F(TypeTest, AttrVersionVecPutIfAttrVersion) {
  AttrVersionVec version_vec(10);

  // base version
  EXPECT_TRUE(version_vec.PutIf(AttrVersion(uint64_t{20})));
  EXPECT_EQ(version_vec.BaseVersion(), 20);
  EXPECT_EQ(version_vec.total_delta_version, 0);
  EXPECT_FALSE(version_vec.PutIf(AttrVersion(uint64_t{20})));
  EXPECT_FALSE(version_vec.PutIf(AttrVersion(uint64_t{15})));

  // delta version
  EXPECT_TRUE(version_vec.PutIf(AttrVersion(1, 4)));
  EXPECT_EQ(version_vec.DeltaVersion(1), 4);
  EXPECT_EQ(version_vec.total_delta_version, 4);
  EXPECT_FALSE(version_vec.PutIf(AttrVersion(1, 4)));
  EXPECT_FALSE(version_vec.PutIf(AttrVersion(1, 2)));
  EXPECT_TRUE(version_vec.PutIf(AttrVersion(1, 6)));
  EXPECT_EQ(version_vec.total_delta_version, 6);
}

TEST_F(TypeTest, AttrVersionVecPutIfAttrEntry) {
  AttrVersionVec version_vec(10);

  AttrEntry attr;
  attr.set_ino(1);
  attr.set_version(20);

  EXPECT_TRUE(version_vec.PutIf(attr));
  EXPECT_EQ(version_vec.BaseVersion(), 20);
  EXPECT_FALSE(version_vec.PutIf(attr));

  attr.set_version(30);
  EXPECT_TRUE(version_vec.PutIf(attr));
  EXPECT_EQ(version_vec.BaseVersion(), 30);
}

TEST_F(TypeTest, AttrVersionVecPutIfAttrWithMutation) {
  AttrVersionVec version_vec(10);

  AttrWithMutation attr_with_mutation;
  attr_with_mutation.attr.set_ino(1);
  attr_with_mutation.attr.set_version(20);
  attr_with_mutation.mutations.push_back(MakeMutation(1, 3));
  attr_with_mutation.mutations.push_back(MakeMutation(2, 4));

  EXPECT_TRUE(version_vec.PutIf(attr_with_mutation));
  EXPECT_EQ(version_vec.BaseVersion(), 20);
  EXPECT_EQ(version_vec.DeltaVersion(1), 3);
  EXPECT_EQ(version_vec.DeltaVersion(2), 4);
  EXPECT_EQ(version_vec.CompleteVersion(), 27);

  // replaying the same attr/mutations is a no-op
  EXPECT_FALSE(version_vec.PutIf(attr_with_mutation));
}

TEST_F(TypeTest, AttrVersionVecPutIfAttrVersionVec) {
  AttrVersionVec version_vec(10);

  AttrVersionVec other(20);
  other.PutIf(AttrVersion(1, 5));
  other.PutIf(AttrVersion(2, 7));

  EXPECT_TRUE(version_vec.PutIf(other));
  EXPECT_EQ(version_vec.BaseVersion(), 20);
  EXPECT_EQ(version_vec.DeltaVersion(1), 5);
  EXPECT_EQ(version_vec.DeltaVersion(2), 7);
  EXPECT_EQ(version_vec.total_delta_version, 12);

  // replaying the same vec is a no-op
  EXPECT_FALSE(version_vec.PutIf(other));

  // base-only newer vec keeps existing deltas
  AttrVersionVec base_only(30);
  EXPECT_TRUE(version_vec.PutIf(base_only));
  EXPECT_EQ(version_vec.BaseVersion(), 30);
  EXPECT_EQ(version_vec.total_delta_version, 12);

  // older base and older deltas are rejected
  AttrVersionVec older(5);
  EXPECT_FALSE(version_vec.PutIf(older));

  // newer base but some stale/some newer deltas: only newer deltas win
  AttrVersionVec mixed(40);
  mixed.PutIf(AttrVersion(1, 2));  // stale -> not applied
  mixed.PutIf(AttrVersion(3, 9));  // newer -> applied
  EXPECT_TRUE(version_vec.PutIf(mixed));
  EXPECT_EQ(version_vec.BaseVersion(), 40);
  EXPECT_EQ(version_vec.DeltaVersion(1), 5);
  EXPECT_EQ(version_vec.DeltaVersion(2), 7);
  EXPECT_EQ(version_vec.DeltaVersion(3), 9);
  EXPECT_EQ(version_vec.total_delta_version, 21);
}

TEST_F(TypeTest, AttrVersionVecLessThanOrEqual) {
  AttrVersionVec version_vec(10);
  version_vec.PutIf(AttrVersion(1, 4));

  EXPECT_TRUE(version_vec.LessThanOrEqual(AttrVersion(uint64_t{10})));
  EXPECT_TRUE(version_vec.LessThanOrEqual(AttrVersion(uint64_t{11})));
  EXPECT_FALSE(version_vec.LessThanOrEqual(AttrVersion(uint64_t{9})));

  EXPECT_TRUE(version_vec.LessThanOrEqual(AttrVersion(1, 4)));
  EXPECT_TRUE(version_vec.LessThanOrEqual(AttrVersion(1, 5)));
  EXPECT_FALSE(version_vec.LessThanOrEqual(AttrVersion(1, 3)));
}

TEST_F(TypeTest, AttrVersionVecGreaterThanOrEqual) {
  AttrVersionVec version_vec(10);
  version_vec.PutIf(AttrVersion(1, 4));

  EXPECT_TRUE(version_vec.GreaterThanOrEqual(AttrVersion(uint64_t{10})));
  EXPECT_TRUE(version_vec.GreaterThanOrEqual(AttrVersion(uint64_t{9})));
  EXPECT_FALSE(version_vec.GreaterThanOrEqual(AttrVersion(uint64_t{11})));

  EXPECT_TRUE(version_vec.GreaterThanOrEqual(AttrVersion(1, 4)));
  EXPECT_TRUE(version_vec.GreaterThanOrEqual(AttrVersion(1, 3)));
  EXPECT_FALSE(version_vec.GreaterThanOrEqual(AttrVersion(1, 5)));
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
