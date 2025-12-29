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

#include <algorithm>
#include <cstdint>

#include "gtest/gtest.h"
#include "mds/filesystem/inode.h"
#include "mds/filesystem/partition.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {
namespace unit_test {

const int64_t kFsId = 1000;

static pb::mds::Inode GenInode(uint32_t fs_id, uint64_t ino,
                               pb::mds::FileType type, uint64_t version = 1) {
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

  auto now_ns = utils::TimestampNs();

  inode.set_atime(now_ns);
  inode.set_mtime(now_ns);
  inode.set_ctime(now_ns);

  if (type == pb::mds::FileType::DIRECTORY) {
    inode.set_nlink(2);
  } else {
    inode.set_nlink(1);
  }

  inode.add_parents(utils::TimestampMs());
  inode.add_parents(utils::TimestampMs() + 1);
  inode.add_parents(utils::TimestampMs() + 2);

  inode.mutable_xattrs()->insert({"key1", "value1"});
  inode.mutable_xattrs()->insert({"key2", "value2"});
  inode.mutable_xattrs()->insert({"key3", "value3"});

  inode.set_version(version);

  return inode;
}

class PartitionTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

class PartitionCacheTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(PartitionTest, PutAndGetDelete) {
  const Ino parent = 1;

  // ready data
  Partition partition(
      Inode::New(GenInode(kFsId, parent, pb::mds::FileType::DIRECTORY)));

  {
    partition.Put(
        Dentry(kFsId, "file01", parent, 1001, pb::mds::FileType::FILE, 0), 1);
    partition.Put(
        Dentry(kFsId, "file02", parent, 1002, pb::mds::FileType::FILE, 0), 1);
    partition.Put(
        Dentry(kFsId, "file03", parent, 1003, pb::mds::FileType::FILE, 0), 1);
    partition.Put(
        Dentry(kFsId, "file04", parent, 1004, pb::mds::FileType::FILE, 0), 1);
    partition.Put(
        Dentry(kFsId, "file05", parent, 1005, pb::mds::FileType::FILE, 0), 1);

    ASSERT_EQ(partition.Size(), 5);
  }

  // get one
  {
    Dentry dentry1;
    ASSERT_TRUE(partition.Get("file01", dentry1));
    ASSERT_EQ(dentry1.INo(), 1001);

    Dentry dentry2;
    ASSERT_TRUE(partition.Get("file02", dentry1));
    ASSERT_EQ(dentry1.INo(), 1002);

    Dentry dentry3;
    ASSERT_TRUE(partition.Get("file03", dentry1));
    ASSERT_EQ(dentry1.INo(), 1003);

    Dentry dentry4;
    ASSERT_TRUE(partition.Get("file04", dentry1));
    ASSERT_EQ(dentry1.INo(), 1004);

    Dentry dentry5;
    ASSERT_TRUE(partition.Get("file05", dentry1));
    ASSERT_EQ(dentry1.INo(), 1005);
  }

  // get all
  {
    auto dentries = partition.GetAll();
    ASSERT_EQ(dentries.size(), 5);
    std::sort(  // NOLINT
        dentries.begin(), dentries.end(),
        [](const Dentry& a, const Dentry& b) { return a.INo() < b.INo(); });
    ASSERT_EQ(dentries[0].INo(), 1001);
    ASSERT_EQ(dentries[1].INo(), 1002);
    ASSERT_EQ(dentries[2].INo(), 1003);
    ASSERT_EQ(dentries[3].INo(), 1004);
    ASSERT_EQ(dentries[4].INo(), 1005);
  }

  // scan
  {
    auto dentries = partition.Scan("file02", 2, false);
    ASSERT_EQ(dentries.size(), 2);
    ASSERT_EQ(dentries[0].INo(), 1003);
    ASSERT_EQ(dentries[1].INo(), 1004);
  }

  // delete
  {
    partition.Delete("file03", 2);
    ASSERT_EQ(partition.Size(), 4);

    Dentry temp_dentry;
    ASSERT_FALSE(partition.Get("file03", temp_dentry));
  }
}

TEST_F(PartitionTest, Merge) {
  const Ino parent = 1;

  // only put
  {
    Partition partition1(
        Inode::New(GenInode(kFsId, parent, pb::mds::FileType::DIRECTORY, 1)));

    partition1.Put(
        Dentry(kFsId, "file01", parent, 1001, pb::mds::FileType::FILE, 0), 1);

    partition1.Put(
        Dentry(kFsId, "file02", parent, 1002, pb::mds::FileType::FILE, 0), 2);

    Partition partition2(
        Inode::New(GenInode(kFsId, parent, pb::mds::FileType::DIRECTORY, 2)));

    partition2.Put(
        Dentry(kFsId, "file01", parent, 1001, pb::mds::FileType::FILE, 0), 3);

    partition2.Put(
        Dentry(kFsId, "file02", parent, 1002, pb::mds::FileType::FILE, 0), 4);

    partition2.Put(
        Dentry(kFsId, "file03", parent, 1003, pb::mds::FileType::FILE, 0), 5);

    partition2.Put(
        Dentry(kFsId, "file04", parent, 1004, pb::mds::FileType::FILE, 0), 6);

    partition1.Merge(std::move(partition2));
    ASSERT_EQ(partition1.Size(), 4);

    // get all
    {
      auto dentries = partition1.GetAll();
      ASSERT_EQ(dentries.size(), 4);
      std::sort(  // NOLINT
          dentries.begin(), dentries.end(),
          [](const Dentry& a, const Dentry& b) { return a.INo() < b.INo(); });
      ASSERT_EQ(dentries[0].INo(), 1001);
      ASSERT_EQ(dentries[1].INo(), 1002);
      ASSERT_EQ(dentries[2].INo(), 1003);
      ASSERT_EQ(dentries[3].INo(), 1004);
    }
  }

  // put and delete
  {
    Partition partition1(
        Inode::New(GenInode(kFsId, parent, pb::mds::FileType::DIRECTORY, 1)));

    Partition partition2(
        Inode::New(GenInode(kFsId, parent, pb::mds::FileType::DIRECTORY, 2)));

    partition2.Put(
        Dentry(kFsId, "file03", parent, 1003, pb::mds::FileType::FILE, 0), 4);

    partition2.Put(
        Dentry(kFsId, "file04", parent, 1004, pb::mds::FileType::FILE, 0), 5);

    partition1.Put(
        Dentry(kFsId, "file01", parent, 1001, pb::mds::FileType::FILE, 0), 6);

    partition1.Put(
        Dentry(kFsId, "file02", parent, 1002, pb::mds::FileType::FILE, 0), 7);

    partition1.Delete("file01", 8);

    partition1.Merge(std::move(partition2));
    ASSERT_EQ(partition1.Size(), 3);

    // get all
    {
      auto dentries = partition1.GetAll();
      ASSERT_EQ(dentries.size(), 3);
      std::sort(  // NOLINT
          dentries.begin(), dentries.end(),
          [](const Dentry& a, const Dentry& b) { return a.INo() < b.INo(); });
      ASSERT_EQ(dentries[0].INo(), 1002);
      ASSERT_EQ(dentries[1].INo(), 1003);
      ASSERT_EQ(dentries[2].INo(), 1004);
    }
  }
}

TEST_F(PartitionCacheTest, Put) {
  PartitionCache partition_cache(kFsId);

  // put PartitionPtr
  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 1, pb::mds::FileType::DIRECTORY));
    auto partition = Partition::New(inode);

    uint64_t parent_ino = 1;
    partition->Put(Dentry(kFsId, "dir01", parent_ino, 100000,
                          pb::mds::FileType::DIRECTORY, 0),
                   1);
    partition->Put(
        Dentry(kFsId, "file01", parent_ino, 100001, pb::mds::FileType::FILE, 0),
        2);

    partition_cache.PutIf(partition);
    ASSERT_EQ(partition_cache.Size(), 1);

    partition = partition_cache.Get(inode->Ino());
    ASSERT_TRUE(partition != nullptr);
    ASSERT_EQ(partition->INo(), inode->Ino());
  }

  // put Partition&&
  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 1, pb::mds::FileType::DIRECTORY));
    Partition partition(inode);

    uint64_t parent_ino = 2;
    partition.Put(Dentry(kFsId, "dir01", parent_ino, 100000,
                         pb::mds::FileType::DIRECTORY, 0),
                  1);
    partition.Put(
        Dentry(kFsId, "file01", parent_ino, 100001, pb::mds::FileType::FILE, 0),
        2);

    partition_cache.PutIf(std::move(partition));
    ASSERT_EQ(partition_cache.Size(), 1);
    ASSERT_TRUE(partition.Empty());

    auto partition2 = partition_cache.Get(inode->Ino());
    ASSERT_TRUE(partition2 != nullptr);
    ASSERT_EQ(partition2->INo(), inode->Ino());
  }

  // put return value
  {
    auto partition1 = Partition::New(
        Inode::New(GenInode(kFsId, 20000, pb::mds::FileType::DIRECTORY)));

    auto resp_partition = partition_cache.PutIf(partition1);
    ASSERT_TRUE(resp_partition.get() == partition1.get());

    auto partition2 = Partition::New(
        Inode::New(GenInode(kFsId, 20000, pb::mds::FileType::DIRECTORY, 2)));
    Dentry dentry(kFsId, "file1", 1, 1000, pb::mds::FileType::FILE, 1212,
                  nullptr);
    partition2->Put(dentry, 2);

    resp_partition = partition_cache.PutIf(partition2);
    ASSERT_TRUE(resp_partition.get() == partition1.get());
    Dentry temp_dentry;
    ASSERT_TRUE(resp_partition->Get("file1", temp_dentry));
  }
}

TEST_F(PartitionCacheTest, Delete) {
  PartitionCache partition_cache(kFsId);

  InodeSPtr inode =
      Inode::New(GenInode(kFsId, 1, pb::mds::FileType::DIRECTORY));
  auto partition = Partition::New(inode);

  uint64_t parent_ino = 1;
  partition->Put(Dentry(kFsId, "dir01", parent_ino, 100000,
                        pb::mds::FileType::DIRECTORY, 0),
                 1);
  partition->Put(Dentry(kFsId, "dir02", parent_ino, 100001,
                        pb::mds::FileType::DIRECTORY, 0),
                 2);
  partition->Put(Dentry(kFsId, "dir03", parent_ino, 100002,
                        pb::mds::FileType::DIRECTORY, 0),
                 3);
  partition->Put(Dentry(kFsId, "dir04", parent_ino, 100003,
                        pb::mds::FileType::DIRECTORY, 0),
                 4);
  partition->Put(
      Dentry(kFsId, "file01", parent_ino, 100004, pb::mds::FileType::FILE, 0),
      5);
  partition->Put(
      Dentry(kFsId, "file01", parent_ino, 100005, pb::mds::FileType::FILE, 0),
      6);

  partition_cache.PutIf(partition);

  ASSERT_TRUE(partition_cache.Get(inode->Ino()) != nullptr);

  partition_cache.Delete(inode->Ino());
  ASSERT_TRUE(partition_cache.Get(inode->Ino()) == nullptr);
}

TEST_F(PartitionCacheTest, Get) {
  PartitionCache partition_cache(kFsId);

  // ready data
  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 1, pb::mds::FileType::DIRECTORY));
    Partition partition(inode);

    partition_cache.PutIf(std::move(partition));
  }

  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 2, pb::mds::FileType::DIRECTORY));
    Partition partition(inode);

    partition_cache.PutIf(std::move(partition));
  }

  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 3, pb::mds::FileType::DIRECTORY));
    Partition partition(inode);

    partition_cache.PutIf(std::move(partition));
  }

  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 4, pb::mds::FileType::DIRECTORY));
    Partition partition(inode);

    partition_cache.PutIf(std::move(partition));
  }

  // get one
  {
    auto partition = partition_cache.Get(1);
    ASSERT_TRUE(partition != nullptr);
    ASSERT_EQ(partition->INo(), 1);
  }

  {
    auto partition = partition_cache.Get(2);
    ASSERT_TRUE(partition != nullptr);
    ASSERT_EQ(partition->INo(), 2);
  }

  {
    auto partition = partition_cache.Get(3);
    ASSERT_TRUE(partition != nullptr);
    ASSERT_EQ(partition->INo(), 3);
  }

  {
    auto partition = partition_cache.Get(4);
    ASSERT_TRUE(partition != nullptr);
    ASSERT_EQ(partition->INo(), 4);
  }

  {
    auto partition = partition_cache.Get(101);
    ASSERT_TRUE(partition == nullptr);
  }

  // get all
  {
    auto partitions = partition_cache.GetAll();
    ASSERT_EQ(partitions.size(), 4);
    std::sort(partitions.begin(), partitions.end(),  // NOLINT
              [](const PartitionPtr& a, const PartitionPtr& b) {
                return a->INo() < b->INo();
              });
    ASSERT_EQ(partitions[0]->INo(), 1);
    ASSERT_EQ(partitions[1]->INo(), 2);
    ASSERT_EQ(partitions[2]->INo(), 3);
    ASSERT_EQ(partitions[3]->INo(), 4);
  }

  // clear
  {
    partition_cache.Clear();
    ASSERT_EQ(partition_cache.Size(), 0);
    auto partitions = partition_cache.GetAll();
    ASSERT_EQ(partitions.size(), 0);
  }
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
