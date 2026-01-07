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

#include <string>

#include "common/const.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "mds/client/mds.h"
#include "mds/common/helper.h"

namespace dingofs {
namespace test {
namespace integration {

DEFINE_string(mds_addr, "127.0.0.1:7800", "mds addr");
DEFINE_uint32(mds_id, 1001, "mds id");

const uint32_t kRandomStringLength = 8;

using mds::client::MDSClient;
using MDSClientUPtr = std::unique_ptr<MDSClient>;

class MDSTest : public testing::Test {
 protected:
  static MDSClientUPtr CreateFilesystem(const std::string& mds_addr,
                                        const std::string& fs_name);
  static bool DeleteFilesystem(MDSClientUPtr& client,
                               const std::string& fs_name);

  static bool MountFilesystem(MDSClientUPtr& client, const std::string& fs_name,
                              const std::string& client_id);
  static bool UmountFilesystem(MDSClientUPtr& client,
                               const std::string& fs_name,
                               const std::string& client_id);

  static void SetUpTestSuite() {
    // create filesystem
    mds_client_ = CreateFilesystem(FLAGS_mds_addr, fs_name_);
    ASSERT_TRUE(mds_client_ != nullptr) << "create filesystem fail.";

    // mount filesystem
    ASSERT_TRUE(MountFilesystem(mds_client_, fs_name_, client_id_))
        << "mount filesystem fail.";
  }

  static void TearDownTestSuite() {
    ASSERT_TRUE(UmountFilesystem(mds_client_, fs_name_, client_id_))
        << "umount filesystem fail.";

    // delete filesystem
    ASSERT_TRUE(DeleteFilesystem(mds_client_, fs_name_))
        << "delete filesystem fail.";
  }

  void SetUp() override {}
  void TearDown() override {}

 public:
  static const std::string fs_name_;
  static const std::string client_id_;

  static MDSClientUPtr mds_client_;
};

const std::string MDSTest::fs_name_ =
    "integration_test_" +
    dingofs::mds::Helper::GenerateRandomString(kRandomStringLength);
const std::string MDSTest::client_id_ = "7872fd60-62df-470c-813c-111111111111";

MDSClientUPtr MDSTest::mds_client_ = nullptr;

MDSClientUPtr MDSTest::CreateFilesystem(const std::string& mds_addr,
                                        const std::string& fs_name) {
  MDSClientUPtr mds_client = std::make_unique<MDSClient>(0);
  if (!mds_client->Init(mds_addr)) return nullptr;

  // create filesystem
  MDSClient::CreateFsParams params;
  params.partition_type = "monolithic";
  params.chunk_size = 1048576;
  params.block_size = 65536;
  params.candidate_mds_ids.push_back(FLAGS_mds_id);

  auto& s3_info = params.s3_info;
  s3_info.endpoint = "test_endpoint";
  s3_info.ak = "test_ak";
  s3_info.sk = "test_sk";
  s3_info.bucket_name = "test_bucket";
  auto response = mds_client->CreateFs(fs_name, params);
  if (response.error().errcode() != dingofs::pb::error::OK) return nullptr;

  mds_client->SetFsId(response.fs_info().fs_id());
  mds_client->SetEpoch(response.fs_info().partition_policy().epoch());

  return mds_client;
}

bool MDSTest::DeleteFilesystem(MDSClientUPtr& client,
                               const std::string& fs_name) {
  auto response = client->DeleteFs(fs_name, true);
  return response.error().errcode() == dingofs::pb::error::OK;
}

bool MDSTest::MountFilesystem(MDSClientUPtr& client, const std::string& fs_name,
                              const std::string& client_id) {
  auto response = client->MountFs(fs_name, client_id);
  return response.error().errcode() == dingofs::pb::error::OK;
}

bool MDSTest::UmountFilesystem(MDSClientUPtr& client,
                               const std::string& fs_name,
                               const std::string& client_id) {
  auto response = client->UmountFs(fs_name, client_id);
  return response.error().errcode() == dingofs::pb::error::OK;
}

TEST_F(MDSTest, Echo) {
  const std::string message = "test";
  auto response = mds_client_->Echo(message);
  ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  ASSERT_EQ(message, response.message());
}

TEST_F(MDSTest, Heartbeat) {
  auto response = mds_client_->Heartbeat(FLAGS_mds_id);
  ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
}

TEST_F(MDSTest, GetMDSList) {
  auto response = mds_client_->GetMdsList();
  ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  ASSERT_GT(response.mdses_size(), 0);
}

TEST_F(MDSTest, FsInfoOperations) {
  // get filesystem info

  auto response = mds_client_->GetFs(MDSTest::fs_name_);
  ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  auto fs_info = response.fs_info();

  // list filesystem info
  {
    auto response = mds_client_->ListFs();
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
    bool found = false;
    for (const auto& fi : response.fs_infos()) {
      if (fi.fs_name() == MDSTest::fs_name_) {
        found = true;
        break;
      }
    }
    ASSERT_TRUE(found);
  }

  // UpdateFs (change owner temporarily)
  {
    fs_info.set_owner("integration_test_owner");
    auto upd_resp = mds_client_->UpdateFs(MDSTest::fs_name_, fs_info);
    ASSERT_EQ(upd_resp.error().errcode(), dingofs::pb::error::OK);
  }

  // verify update
  {
    auto response = mds_client_->GetFs(MDSTest::fs_name_);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
    ASSERT_EQ(response.fs_info().owner(), "integration_test_owner");
  }
}

TEST_F(MDSTest, DentryAndInodeOperations) {
  std::string dir_name =
      "test_dir_" +
      dingofs::mds::Helper::GenerateRandomString(kRandomStringLength);
  // create a directory
  {
    auto response = mds_client_->MkDir(dingofs::kRootIno, dir_name);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  }

  // ListDentry should find it
  {
    auto response = mds_client_->ListDentry(dingofs::kRootIno, false);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
    bool dir_found = false;
    for (const auto& d : response.dentries()) {
      if (d.name() == dir_name) {
        dir_found = true;
        break;
      }
    }
    ASSERT_TRUE(dir_found);
  }

  // get dentry
  {
    auto response = mds_client_->GetDentry(dingofs::kRootIno, dir_name);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  }

  // lookup
  uint64_t ino = 0;
  {
    auto response = mds_client_->Lookup(dingofs::kRootIno, dir_name);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
    ino = response.inode().ino();
  }

  // GetInode
  {
    auto response = mds_client_->GetInode(ino);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  }

  // BatchGetInode
  std::vector<int64_t> inos = {static_cast<int64_t>(ino)};
  {
    auto response = mds_client_->BatchGetInode(inos);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
    ASSERT_GT(response.inodes_size(), 0);
  }

  // BatchGetXAttr (should succeed even if empty)
  {
    auto response = mds_client_->BatchGetXattr(inos);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  }

  // read dir
  {
    auto response = mds_client_->ReadDir(dingofs::kRootIno, "", true, true);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
    ASSERT_GT(response.entries_size(), 0);
  }

  // rm dir
  {
    auto response = mds_client_->RmDir(dingofs::kRootIno, dir_name);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  }
}

TEST_F(MDSTest, FileCreateOpenRelease) {
  std::string file_name =
      "test_file_" +
      dingofs::mds::Helper::GenerateRandomString(kRandomStringLength);
  // create file
  {
    auto response = mds_client_->MkNod(dingofs::kRootIno, file_name);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  }

  // lookup to get inode
  uint64_t ino = 0;
  {
    auto response = mds_client_->Lookup(dingofs::kRootIno, file_name);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
    ino = response.inode().ino();
  }

  // Open
  std::string session_id;
  {
    auto response = mds_client_->Open(ino);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
    session_id = response.session_id();
  }

  // Release
  {
    auto response = mds_client_->Release(ino, session_id);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  }
}

TEST_F(MDSTest, LinkUnlinkSymlinkReadlink) {
  std::string src_name =
      "src_file_" +
      dingofs::mds::Helper::GenerateRandomString(kRandomStringLength);

  // create a file to link
  {
    auto response = mds_client_->MkNod(dingofs::kRootIno, src_name);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  }

  // lookup to get inode
  uint64_t ino = 0;
  {
    auto response = mds_client_->Lookup(dingofs::kRootIno, src_name);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
    ino = response.inode().ino();
  }

  // link to new parent (same root) with new name
  std::string link_name =
      "linked_" +
      dingofs::mds::Helper::GenerateRandomString(kRandomStringLength);
  {
    auto response = mds_client_->Link(ino, dingofs::kRootIno, link_name);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  }

  // verify linked dentry exists
  {
    auto response = mds_client_->Lookup(dingofs::kRootIno, link_name);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
    ASSERT_EQ(response.inode().ino(), ino);
  }

  // unlink the new name
  {
    auto response = mds_client_->UnLink(dingofs::kRootIno, link_name);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
  }

  // symlink and readlink
  std::string symlink_name =
      "symlink_" +
      dingofs::mds::Helper::GenerateRandomString(kRandomStringLength);
  std::string target_path = "/some/target/path";
  auto response =
      mds_client_->Symlink(dingofs::kRootIno, symlink_name, target_path);
  ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);

  // lookup symlink and readlink
  {
    auto response = mds_client_->Lookup(dingofs::kRootIno, symlink_name);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);

    uint64_t sym_ino = response.inode().ino();
    {
      auto response = mds_client_->ReadLink(sym_ino);
      ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
      ASSERT_EQ(response.symlink(), target_path);
    }
  }
}

TEST_F(MDSTest, SliceOps) {
  // alloc slice id
  {
    auto response = mds_client_->AllocSliceId(1, 0);
    ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);
    ASSERT_GT(response.slice_id(), 0);
  }

  // create a file to write/read slice
  std::string file_name =
      "slice_file_" +
      dingofs::mds::Helper::GenerateRandomString(kRandomStringLength);
  auto response = mds_client_->MkNod(dingofs::kRootIno, file_name);
  ASSERT_EQ(response.error().errcode(), dingofs::pb::error::OK);

  auto lookup_resp = mds_client_->Lookup(dingofs::kRootIno, file_name);
  ASSERT_EQ(lookup_resp.error().errcode(), dingofs::pb::error::OK);
  uint64_t ino = lookup_resp.inode().ino();

  // write slice
  {
    auto write_resp = mds_client_->WriteSlice(dingofs::kRootIno, ino, 0);
    ASSERT_EQ(write_resp.error().errcode(), dingofs::pb::error::OK);

    // read slice
    auto read_resp = mds_client_->ReadSlice(ino, 0);
    ASSERT_EQ(read_resp.error().errcode(), dingofs::pb::error::OK);
    ASSERT_GT(read_resp.chunks_size(), 0);
  }
}

TEST_F(MDSTest, AttrOperations) {
  std::string file_name =
      "attr_file_" +
      dingofs::mds::Helper::GenerateRandomString(kRandomStringLength);

  // create file
  auto mknod_resp = mds_client_->MkNod(dingofs::kRootIno, file_name);
  ASSERT_EQ(mknod_resp.error().errcode(), dingofs::pb::error::OK);

  // lookup to get inode
  auto lookup_resp = mds_client_->Lookup(dingofs::kRootIno, file_name);
  ASSERT_EQ(lookup_resp.error().errcode(), dingofs::pb::error::OK);
  uint64_t ino = lookup_resp.inode().ino();

  // get attr
  auto getattr_resp = mds_client_->GetAttr(ino);
  ASSERT_EQ(getattr_resp.error().errcode(), dingofs::pb::error::OK);

  // set attr
  int uid = 101, gid = 101;
  auto attr = getattr_resp.inode();
  attr.set_uid(uid);
  attr.set_gid(gid);
  auto setattr_resp = mds_client_->SetAttr(
      ino, dingofs::kSetAttrUid | dingofs::kSetAttrGid, attr);
  ASSERT_EQ(setattr_resp.error().errcode(), dingofs::pb::error::OK);

  // verify GetAttr still succeeds after SetAttr
  auto getattr_resp2 = mds_client_->GetAttr(ino);
  ASSERT_EQ(getattr_resp2.error().errcode(), dingofs::pb::error::OK);
  ASSERT_EQ(getattr_resp2.inode().uid(), uid);
  ASSERT_EQ(getattr_resp2.inode().gid(), gid);
}

TEST_F(MDSTest, XAttrOperations) {
  std::string file_name =
      "xattr_file_" +
      dingofs::mds::Helper::GenerateRandomString(kRandomStringLength);
  // create file
  auto mknod_resp = mds_client_->MkNod(dingofs::kRootIno, file_name);
  ASSERT_EQ(mknod_resp.error().errcode(), dingofs::pb::error::OK);

  // lookup to get inode
  auto lookup_resp = mds_client_->Lookup(dingofs::kRootIno, file_name);
  ASSERT_EQ(lookup_resp.error().errcode(), dingofs::pb::error::OK);
  uint64_t ino = lookup_resp.inode().ino();

  // ensure no xattrs initially (ListXAttr should succeed)
  auto listxattr_resp = mds_client_->ListXAttr(ino);
  ASSERT_EQ(listxattr_resp.error().errcode(), dingofs::pb::error::OK);

  // set xattr
  std::string key = "test.key." + dingofs::mds::Helper::GenerateRandomString(
                                      kRandomStringLength);
  std::string value = "test_value";
  std::map<std::string, std::string> xattrs = {{key, value}};
  auto setxattr_resp = mds_client_->SetXAttr(ino, xattrs);
  ASSERT_EQ(setxattr_resp.error().errcode(), dingofs::pb::error::OK);

  // get xattr
  auto getxattr_resp = mds_client_->GetXAttr(ino, key);
  ASSERT_EQ(getxattr_resp.error().errcode(), dingofs::pb::error::OK);
  ASSERT_EQ(getxattr_resp.value(), value);

  // list xattr should contain the key
  {
    auto listxattr_resp = mds_client_->ListXAttr(ino);
    ASSERT_EQ(listxattr_resp.error().errcode(), dingofs::pb::error::OK);
    bool found = false;
    for (const auto& [k, v] : listxattr_resp.xattrs()) {
      if (k == key) {
        found = true;
        break;
      }
    }
    ASSERT_TRUE(found);
  }

  // remove xattr
  auto removexattr_resp = mds_client_->RemoveXAttr(ino, key);
  ASSERT_EQ(removexattr_resp.error().errcode(), dingofs::pb::error::OK);

  // verify key is removed (ListXAttr should not contain it)
  {
    auto listxattr_resp = mds_client_->ListXAttr(ino);
    ASSERT_EQ(listxattr_resp.error().errcode(), dingofs::pb::error::OK);
    bool found = false;
    for (const auto& [k, v] : listxattr_resp.xattrs()) {
      if (k == key) {
        found = true;
        break;
      }
    }
    ASSERT_FALSE(found);
  }
}

}  // namespace integration
}  // namespace test
}  // namespace dingofs
