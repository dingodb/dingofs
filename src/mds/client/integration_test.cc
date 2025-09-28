// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#include "mds/client/integration_test.h"

#include <gflags/gflags_declare.h>

#include "mds/client/mds.h"
#include "mds/common/helper.h"

DECLARE_string(mds_addr);

namespace dingofs {
namespace mds {
namespace client {

class MDSIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override;
  void TearDown() override;

  std::unique_ptr<MDSClient> mds_client_;
  static const std::string fs_name_;
  static const std::string client_id_;
};

const std::string MDSIntegrationTest::fs_name_ = "integration_test_fs_" + dingofs::mds::Helper::GenerateRandomString(8);
const std::string MDSIntegrationTest::client_id_ = "integration_test_client";

void MDSIntegrationTest::SetUp() {
  mds_client_ = std::make_unique<MDSClient>(0);
  ASSERT_TRUE(mds_client_->Init(FLAGS_mds_addr));

  // 1. Create file system
  MDSClient::CreateFsParams params;
  params.partition_type = "monolithic";
  params.chunk_size = 1048576;
  params.block_size = 65536;

  auto& s3_info = params.s3_info;
  s3_info.endpoint = "test_endpoint";
  s3_info.ak = "test_ak";
  s3_info.sk = "test_sk";
  s3_info.bucket_name = "test_bucket";
  auto create_fs_resp = mds_client_->CreateFs(fs_name_, params);
  ASSERT_EQ(create_fs_resp.error().errcode(), dingofs::pb::error::OK);
  mds_client_->SetFsId(create_fs_resp.fs_info().fs_id());
  mds_client_->SetEpoch(create_fs_resp.fs_info().partition_policy().epoch());
}

void MDSIntegrationTest::TearDown() {
  // 14. Delete file system
  auto delete_fs_resp = mds_client_->DeleteFs(fs_name_, true);
  ASSERT_EQ(delete_fs_resp.error().errcode(), dingofs::pb::error::OK);
}

TEST_F(MDSIntegrationTest, FileSystemOperations) {
  // 2. Mount file system
  auto mount_fs_resp = mds_client_->MountFs(fs_name_, client_id_);
  ASSERT_EQ(mount_fs_resp.error().errcode(), dingofs::pb::error::OK);

  // 3. Get file system
  auto get_fs_resp = mds_client_->GetFs(fs_name_);
  ASSERT_EQ(get_fs_resp.error().errcode(), dingofs::pb::error::OK);
  ASSERT_EQ(get_fs_resp.fs_info().fs_name(), fs_name_);

  // 4. List file systems
  auto list_fs_resp = mds_client_->ListFs();
  ASSERT_EQ(list_fs_resp.error().errcode(), dingofs::pb::error::OK);
  ASSERT_FALSE(list_fs_resp.fs_infos().empty());

  // 5. Create directory
  auto mkdir_resp = mds_client_->MkDir(1, "test_dir");
  ASSERT_EQ(mkdir_resp.error().errcode(), dingofs::pb::error::OK);
  auto test_dir_ino = mkdir_resp.inode().ino();

  // 6. Create file
  auto mknod_resp = mds_client_->MkNod(test_dir_ino, "test_file");
  ASSERT_EQ(mknod_resp.error().errcode(), dingofs::pb::error::OK);
  auto test_file_ino = mknod_resp.inode().ino();

  // 7. Create hard link
  auto link_resp = mds_client_->Link(test_file_ino, test_dir_ino, "test_file_hardlink");
  ASSERT_EQ(link_resp.error().errcode(), dingofs::pb::error::OK);

  // 8. Create symbolic link
  auto symlink_resp = mds_client_->Symlink(test_dir_ino, "test_file_symlink", "test_file");
  ASSERT_EQ(symlink_resp.error().errcode(), dingofs::pb::error::OK);

  // 9. Delete symbolic link
  auto unlink_symlink_resp = mds_client_->UnLink(test_dir_ino, "test_file_symlink");
  ASSERT_EQ(unlink_symlink_resp.error().errcode(), dingofs::pb::error::OK);

  // 10. Delete file
  auto unlink_file_resp = mds_client_->UnLink(test_dir_ino, "test_file");
  ASSERT_EQ(unlink_file_resp.error().errcode(), dingofs::pb::error::OK);

  // 11. Delete hard link
  auto unlink_hardlink_resp = mds_client_->UnLink(test_dir_ino, "test_file_hardlink");
  ASSERT_EQ(unlink_hardlink_resp.error().errcode(), dingofs::pb::error::OK);

  // 12. Delete directory
  auto rmdir_resp = mds_client_->RmDir(1, "test_dir");
  ASSERT_EQ(rmdir_resp.error().errcode(), dingofs::pb::error::OK);

  // 13. Unmount file system
  auto umount_fs_resp = mds_client_->UmountFs(fs_name_, client_id_);
  ASSERT_EQ(umount_fs_resp.error().errcode(), dingofs::pb::error::OK);
}

bool IntegrationTestCommandRunner::Run(const std::string& cmd) {
  if (cmd != "integrationtest") return false;

  ::testing::InitGoogleTest();
  return RUN_ALL_TESTS();
}

}  // namespace client
}  // namespace mds
}  // namespace dingofs