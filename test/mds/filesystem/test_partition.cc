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

#include "gtest/gtest.h"
#include "mds/common/helper.h"
#include "mds/filesystem/inode.h"
#include "mds/filesystem/partition.h"

namespace dingofs {
namespace mds {
namespace unit_test {

const int64_t kFsId = 1000;

static pb::mds::Inode GenInode(uint32_t fs_id, uint64_t ino,
                               pb::mds::FileType type) {
  pb::mds::Inode inode;
  inode.set_ino(ino);
  inode.set_fs_id(fs_id);
  inode.set_length(0);
  inode.set_mode(S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP |
                 S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  inode.set_uid(1008);
  inode.set_gid(1008);
  inode.set_rdev(0);
  inode.set_type(type);

  auto now_ns = Helper::TimestampNs();

  inode.set_atime(now_ns);
  inode.set_mtime(now_ns);
  inode.set_ctime(now_ns);

  if (type == pb::mds::FileType::DIRECTORY) {
    inode.set_nlink(2);
  } else {
    inode.set_nlink(1);
  }

  return inode;
}

class PartitionCacheTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(PartitionCacheTest, Put) {
  PartitionCache partition_cache(kFsId);

  InodeSPtr inode =
      Inode::New(GenInode(kFsId, 1, pb::mds::FileType::DIRECTORY));
  auto partition = Partition::New(inode);

  uint64_t parent_ino = 1;
  partition->PutChild(Dentry(kFsId, "dir01", parent_ino, 100000,
                             pb::mds::FileType::DIRECTORY, 0));
  partition->PutChild(Dentry(kFsId, "dir02", parent_ino, 100001,
                             pb::mds::FileType::DIRECTORY, 0));
  partition->PutChild(Dentry(kFsId, "dir03", parent_ino, 100002,
                             pb::mds::FileType::DIRECTORY, 0));
  partition->PutChild(Dentry(kFsId, "dir04", parent_ino, 100003,
                             pb::mds::FileType::DIRECTORY, 0));
  partition->PutChild(
      Dentry(kFsId, "file01", parent_ino, 100004, pb::mds::FileType::FILE, 0));
  partition->PutChild(
      Dentry(kFsId, "file01", parent_ino, 100005, pb::mds::FileType::FILE, 0));

  partition_cache.PutIf(inode->Ino(), partition);

  ASSERT_TRUE(partition_cache.Get(inode->Ino()) != nullptr);
}

TEST_F(PartitionCacheTest, Delete) {
  PartitionCache partition_cache(kFsId);

  InodeSPtr inode =
      Inode::New(GenInode(kFsId, 1, pb::mds::FileType::DIRECTORY));
  auto partition = Partition::New(inode);

  uint64_t parent_ino = 1;
  partition->PutChild(Dentry(kFsId, "dir01", parent_ino, 100000,
                             pb::mds::FileType::DIRECTORY, 0));
  partition->PutChild(Dentry(kFsId, "dir02", parent_ino, 100001,
                             pb::mds::FileType::DIRECTORY, 0));
  partition->PutChild(Dentry(kFsId, "dir03", parent_ino, 100002,
                             pb::mds::FileType::DIRECTORY, 0));
  partition->PutChild(Dentry(kFsId, "dir04", parent_ino, 100003,
                             pb::mds::FileType::DIRECTORY, 0));
  partition->PutChild(
      Dentry(kFsId, "file01", parent_ino, 100004, pb::mds::FileType::FILE, 0));
  partition->PutChild(
      Dentry(kFsId, "file01", parent_ino, 100005, pb::mds::FileType::FILE, 0));

  partition_cache.PutIf(inode->Ino(), partition);

  ASSERT_TRUE(partition_cache.Get(inode->Ino()) != nullptr);

  partition_cache.Delete(inode->Ino());
  ASSERT_TRUE(partition_cache.Get(inode->Ino()) == nullptr);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
