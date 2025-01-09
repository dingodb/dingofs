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

#include <cstdint>
#include <string>

#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "gtest/gtest.h"
#include "mdsv2/coordinator/dummy_coordinator_client.h"
#include "mdsv2/filesystem/filesystem.h"
#include "mdsv2/storage/dummy_storage.h"

namespace dingofs {
namespace mdsv2 {
namespace unit_test {

// test FileSystemSet
class FileSystemSetTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto fs_id_generator = AutoIncrementIdGenerator::New(DummyCoordinatorClient::New(), 1000, 20000, 8);

    fs_set = FileSystemSet::New(fs_id_generator, DummyStorage::New());
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

 public:
  static FileSystemSetPtr fs_set;

  static FileSystemSetPtr FsSet() { return fs_set; }
};

FileSystemSetPtr FileSystemSetTest::fs_set = nullptr;

// test FileSystem
class FileSystemTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto fs_id_generator = AutoIncrementIdGenerator::New(DummyCoordinatorClient::New(), 1000, 20000, 8);

    fs = FileSystemTest::New(fs_id_generator, DummyStorage::New());
  }

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

 public:
  static FileSystemPtr fs;

  static FileSystemPtr Fs() { return fs; }
};

FileSystemPtr FileSystemTest::fs = nullptr;

static pb::mdsv2::S3Info CreateS3Info() {
  pb::mdsv2::S3Info s3_info;
  s3_info.set_ak("ak");
  s3_info.set_sk("sk");
  s3_info.set_endpoint("http://s3.com");
  s3_info.set_bucketname("bucket");
  s3_info.set_block_size(1024 * 1024);
  s3_info.set_chunk_size(1024 * 1024);
  s3_info.set_object_prefix(1);

  return s3_info;
}

TEST_F(FileSystemSetTest, CreateFs) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_name = "test_fs_for_create";
  param.block_size = 1024 * 1024;
  param.fs_type = pb::mdsv2::FsType::S3;
  *param.fs_detail.mutable_s3_info() = CreateS3Info();
  param.enable_sum_in_dir = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  int64_t fs_id = 0;
  auto status = fs_set->CreateFs(param, fs_id);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  ASSERT_TRUE(fs_set->GetFileSystem(fs_id) != nullptr) << "get fs fail.";
}

TEST_F(FileSystemSetTest, GetFsInfo) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_name = "test_fs_for_get";
  param.block_size = 1024 * 1024;
  param.fs_type = pb::mdsv2::FsType::S3;
  *param.fs_detail.mutable_s3_info() = CreateS3Info();
  param.enable_sum_in_dir = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  int64_t fs_id = 0;
  auto status = fs_set->CreateFs(param, fs_id);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  pb::mdsv2::FsInfo fs_info;
  status = fs_set->GetFsInfo(param.fs_name, fs_info);
  ASSERT_TRUE(status.ok()) << "get fs info fail, error: " << status.error_str();
  ASSERT_EQ(fs_info.fs_id(), fs_id) << "fs id not equal.";
  ASSERT_EQ(fs_info.fs_name(), param.fs_name) << "fs name not equal.";
  ASSERT_EQ(fs_info.fs_type(), param.fs_type) << "fs type not equal.";
  ASSERT_EQ(fs_info.block_size(), param.block_size) << "block size not equal.";
  ASSERT_EQ(fs_info.enable_sum_in_dir(), param.enable_sum_in_dir) << "enable sum in dir not equal.";
  ASSERT_EQ(fs_info.owner(), param.owner) << "owner not equal.";
  ASSERT_EQ(fs_info.capacity(), param.capacity) << "capacity not equal.";
  ASSERT_EQ(fs_info.recycle_time_hour(), param.recycle_time_hour) << "recycle time hour not equal.";
}

TEST_F(FileSystemSetTest, DeleteFs) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_name = "test_fs_for_delete";
  param.block_size = 1024 * 1024;
  param.fs_type = pb::mdsv2::FsType::S3;
  *param.fs_detail.mutable_s3_info() = CreateS3Info();
  param.enable_sum_in_dir = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  int64_t fs_id = 0;
  auto status = fs_set->CreateFs(param, fs_id);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  status = fs_set->DeleteFs(param.fs_name);
  ASSERT_TRUE(status.ok()) << "delete fs fail, error: " << status.error_str();

  ASSERT_EQ(nullptr, fs_set->GetFileSystem(fs_id));

  pb::mdsv2::FsInfo fs_info;
  status = fs_set->GetFsInfo(param.fs_name, fs_info);
  ASSERT_TRUE(pb::error::ENOT_FOUND == status.error_code()) << "not should found fs, error: " << status.error_str();
}

TEST_F(FileSystemSetTest, MountFs) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_name = "test_fs_for_mount";
  param.block_size = 1024 * 1024;
  param.fs_type = pb::mdsv2::FsType::S3;
  *param.fs_detail.mutable_s3_info() = CreateS3Info();
  param.enable_sum_in_dir = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  int64_t fs_id = 0;
  auto status = fs_set->CreateFs(param, fs_id);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  pb::mdsv2::MountPoint mount_point;
  mount_point.set_hostname("localhost");
  mount_point.set_port(8080);
  mount_point.set_path("/mnt/dingofs");
  mount_point.set_cto(true);
  status = fs_set->MountFs(param.fs_name, mount_point);
  ASSERT_TRUE(status.ok()) << "mount fs fail, error: " << status.error_str();

  pb::mdsv2::FsInfo fs_info;
  status = fs_set->GetFsInfo(param.fs_name, fs_info);
  ASSERT_TRUE(status.ok()) << "get fs info fail, error: " << status.error_str();
  ASSERT_EQ(fs_info.fs_id(), fs_id) << "fs id not equal.";
  ASSERT_EQ(1, fs_info.mount_points_size()) << "mount point size not equal.";
  auto actual_mount_point = fs_info.mount_points(0);
  ASSERT_EQ(mount_point.hostname(), actual_mount_point.hostname()) << "hostname not equal.";
  ASSERT_EQ(mount_point.port(), actual_mount_point.port()) << "port not equal.";
  ASSERT_EQ(mount_point.path(), actual_mount_point.path()) << "path not equal.";
  ASSERT_EQ(mount_point.cto(), actual_mount_point.cto()) << "cto not equal.";
}

TEST_F(FileSystemSetTest, UnMountFs) {
  auto fs_set = FsSet();

  FileSystemSet::CreateFsParam param;
  param.fs_name = "test_fs_for_mount";
  param.block_size = 1024 * 1024;
  param.fs_type = pb::mdsv2::FsType::S3;
  *param.fs_detail.mutable_s3_info() = CreateS3Info();
  param.enable_sum_in_dir = false;
  param.owner = "dengzh";
  param.capacity = 1024 * 1024 * 1024;
  param.recycle_time_hour = 24;

  int64_t fs_id = 0;
  auto status = fs_set->CreateFs(param, fs_id);
  ASSERT_TRUE(status.ok()) << "create fs fail, error: " << status.error_str();
  ASSERT_GT(fs_id, 0) << "fs id is invalid.";

  pb::mdsv2::MountPoint mount_point;
  mount_point.set_hostname("localhost");
  mount_point.set_port(8080);
  mount_point.set_path("/mnt/dingofs");
  mount_point.set_cto(true);
  status = fs_set->MountFs(param.fs_name, mount_point);
  ASSERT_TRUE(status.ok()) << "mount fs fail, error: " << status.error_str();

  pb::mdsv2::FsInfo fs_info;
  status = fs_set->GetFsInfo(param.fs_name, fs_info);
  ASSERT_TRUE(status.ok()) << "get fs info fail, error: " << status.error_str();
  ASSERT_EQ(fs_info.fs_id(), fs_id) << "fs id not equal.";
  ASSERT_EQ(1, fs_info.mount_points_size()) << "mount point size not equal.";
  auto actual_mount_point = fs_info.mount_points(0);
  ASSERT_EQ(mount_point.hostname(), actual_mount_point.hostname()) << "hostname not equal.";
  ASSERT_EQ(mount_point.port(), actual_mount_point.port()) << "port not equal.";
  ASSERT_EQ(mount_point.path(), actual_mount_point.path()) << "path not equal.";
  ASSERT_EQ(mount_point.cto(), actual_mount_point.cto()) << "cto not equal.";

  status = fs_set->UmountFs(param.fs_name, mount_point);
  ASSERT_TRUE(status.ok()) << "unmount fs fail, error: " << status.error_str();

  {
    pb::mdsv2::FsInfo fs_info;
    status = fs_set->GetFsInfo(param.fs_name, fs_info);
    ASSERT_TRUE(status.ok()) << "get fs info fail, error: " << status.error_str();
    ASSERT_EQ(fs_info.fs_id(), fs_id) << "fs id not equal.";
    ASSERT_EQ(0, fs_info.mount_points_size()) << "mount point size not equal.";
  }
}

TEST_F(FileSystemTest, MkNod) {}

TEST_F(FileSystemTest, MkDir) {}

TEST_F(FileSystemTest, RmDir) {}

TEST_F(FileSystemTest, Link) {}

TEST_F(FileSystemTest, UnLink) {}

TEST_F(FileSystemTest, Symlink) {}

TEST_F(FileSystemTest, ReadLink) {}

TEST_F(FileSystemTest, GetDentry) {}

TEST_F(FileSystemTest, GetInode) {}

TEST_F(FileSystemTest, UpdateInode) {}

TEST_F(FileSystemTest, GetXAttr) {}

TEST_F(FileSystemTest, SetXAttr) {}

}  // namespace unit_test
}  // namespace mdsv2
}  // namespace dingofs