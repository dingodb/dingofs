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
#include <atomic>
#include <string>
#include <type_traits>
#include <utility>

#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "gtest/gtest.h"
#include "mds/common/helper.h"
#include "mds/filesystem/inode.h"
#include "utils/time.h"

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

  inode.set_version(1);

  return inode;
}

class InodeCacheTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(InodeCacheTest, Put) {
  InodeCache inode_cache(kFsId);

  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 2000, pb::mds::FileType::DIRECTORY));
    inode_cache.PutIf(inode->Ino(), inode);

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) != nullptr);
  }

  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 2001, pb::mds::FileType::DIRECTORY));
    inode_cache.PutIf(inode->Ino(), inode);

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) != nullptr);
  }

  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 2003, pb::mds::FileType::DIRECTORY));
    inode_cache.PutIf(inode->Ino(), inode);

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) != nullptr);
  }

  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 2004, pb::mds::FileType::DIRECTORY));
    inode_cache.PutIf(inode->Ino(), inode);

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) != nullptr);
  }

  // put by InodeSPtr
  {
    const Ino ino = 2005;

    InodeSPtr inode =
        Inode::New(GenInode(kFsId, ino, pb::mds::FileType::DIRECTORY));
    inode_cache.PutIf(inode->Ino(), inode);

    inode = inode_cache.Get(inode->Ino());
    ASSERT_TRUE(inode != nullptr);
    ASSERT_EQ(inode->Ino(), ino);
    ASSERT_EQ(inode->Gid(), 1008);
    ASSERT_EQ(inode->Uid(), 1008);
    ASSERT_EQ(inode->Version(), 1);

    auto attr_entry = GenInode(kFsId, ino, pb::mds::FileType::DIRECTORY);
    attr_entry.set_gid(1234);
    attr_entry.set_uid(5678);
    attr_entry.set_length(1234567);
    attr_entry.set_version(2);
    inode = Inode::New(attr_entry);
    inode_cache.PutIf(inode->Ino(), inode);

    inode = inode_cache.Get(inode->Ino());
    ASSERT_TRUE(inode != nullptr);
    ASSERT_EQ(inode->Ino(), ino);
    ASSERT_EQ(inode->Gid(), 1234);
    ASSERT_EQ(inode->Uid(), 5678);
    ASSERT_EQ(inode->Length(), 1234567);
    ASSERT_EQ(inode->Version(), 2);
  }

  {
    const Ino ino = 2006;
    auto attr_entry = GenInode(kFsId, ino, pb::mds::FileType::FILE);
    inode_cache.PutIf(attr_entry);

    auto inode = inode_cache.Get(ino);
    ASSERT_TRUE(inode != nullptr);
    ASSERT_EQ(inode->Ino(), ino);
  }

  {
    const Ino ino = 2007;
    auto attr_entry = GenInode(kFsId, ino, pb::mds::FileType::FILE);
    inode_cache.PutIf(std::move(attr_entry));

    ASSERT_EQ(attr_entry.parents_size(), 0);
    ASSERT_EQ(attr_entry.xattrs_size(), 0);

    auto inode = inode_cache.Get(ino);
    ASSERT_TRUE(inode != nullptr);
    ASSERT_EQ(inode->Ino(), ino);
    ASSERT_EQ(inode->Type(), pb::mds::FileType::FILE);
  }

  // put by AttrEntry&
  {
    const Ino ino = 2008;

    // insert
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, ino, pb::mds::FileType::DIRECTORY));
    inode_cache.PutIf(inode->Ino(), inode);

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) != nullptr);

    // update
    auto attr_entry = GenInode(kFsId, ino, pb::mds::FileType::DIRECTORY);
    attr_entry.set_gid(1234);
    attr_entry.set_uid(5678);
    attr_entry.set_length(1234567);
    attr_entry.set_version(2);
    inode_cache.PutIf(attr_entry);

    ASSERT_EQ(attr_entry.parents_size(), 3);
    ASSERT_EQ(attr_entry.xattrs_size(), 3);

    inode = inode_cache.Get(ino);
    ASSERT_TRUE(inode != nullptr);
    ASSERT_EQ(inode->Ino(), ino);
    ASSERT_EQ(inode->Type(), pb::mds::FileType::DIRECTORY);
    ASSERT_EQ(inode->Gid(), 1234);
    ASSERT_EQ(inode->Uid(), 5678);
    ASSERT_EQ(inode->Length(), 1234567);
    ASSERT_EQ(inode->Version(), 2);
  }

  // put by AttrEntry&&
  {
    const Ino ino = 2009;

    // insert
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, ino, pb::mds::FileType::DIRECTORY));
    inode_cache.PutIf(ino, inode);

    inode = inode_cache.Get(ino);
    ASSERT_TRUE(inode != nullptr);
    ASSERT_EQ(inode->Ino(), ino);
    ASSERT_EQ(inode->Version(), 1);

    // update
    auto attr_entry = GenInode(kFsId, ino, pb::mds::FileType::DIRECTORY);
    attr_entry.set_gid(1234);
    attr_entry.set_uid(5678);
    attr_entry.set_length(1234567);
    attr_entry.set_version(2);
    attr_entry.add_parents(100000001);
    ASSERT_EQ(attr_entry.parents_size(), 4);
    inode_cache.PutIf(std::move(attr_entry));

    ASSERT_EQ(attr_entry.parents_size(), 3);

    inode = inode_cache.Get(ino);
    ASSERT_TRUE(inode != nullptr);
    ASSERT_EQ(inode->Ino(), ino);
    ASSERT_EQ(inode->Type(), pb::mds::FileType::DIRECTORY);
    ASSERT_EQ(inode->Gid(), 1234);
    ASSERT_EQ(inode->Uid(), 5678);
    ASSERT_EQ(inode->Length(), 1234567);
    ASSERT_EQ(inode->Version(), 2);
  }
}

TEST_F(InodeCacheTest, Delete) {
  InodeCache inode_cache(kFsId);

  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 2000, pb::mds::FileType::DIRECTORY));
    inode_cache.PutIf(inode->Ino(), inode);

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) != nullptr);

    inode_cache.Delete(inode->Ino());

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) == nullptr);
  }

  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 2001, pb::mds::FileType::DIRECTORY));
    inode_cache.PutIf(inode->Ino(), inode);

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) != nullptr);

    inode_cache.Delete(inode->Ino());

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) == nullptr);
  }

  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 2002, pb::mds::FileType::DIRECTORY));
    inode_cache.PutIf(inode->Ino(), inode);

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) != nullptr);

    inode_cache.Delete(inode->Ino());

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) == nullptr);
  }

  {
    InodeSPtr inode =
        Inode::New(GenInode(kFsId, 2003, pb::mds::FileType::DIRECTORY));
    inode_cache.PutIf(inode->Ino(), inode);

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) != nullptr);

    inode_cache.Delete(inode->Ino());

    ASSERT_TRUE(inode_cache.Get(inode->Ino()) == nullptr);
  }
}

TEST_F(InodeCacheTest, Get) {
  InodeCache inode_cache(kFsId);

  inode_cache.PutIf(GenInode(kFsId, 4001, pb::mds::FileType::FILE));
  ASSERT_TRUE(inode_cache.Get(4001) != nullptr);
  ASSERT_EQ(inode_cache.Size(), 1);

  inode_cache.PutIf(GenInode(kFsId, 4002, pb::mds::FileType::FILE));
  ASSERT_TRUE(inode_cache.Get(4002) != nullptr);
  ASSERT_EQ(inode_cache.Size(), 2);

  inode_cache.PutIf(GenInode(kFsId, 4003, pb::mds::FileType::FILE));
  ASSERT_TRUE(inode_cache.Get(4003) != nullptr);
  ASSERT_EQ(inode_cache.Size(), 3);

  inode_cache.PutIf(GenInode(kFsId, 4004, pb::mds::FileType::FILE));
  ASSERT_TRUE(inode_cache.Get(4004) != nullptr);
  ASSERT_EQ(inode_cache.Size(), 4);

  auto attr_entry = GenInode(kFsId, 4001, pb::mds::FileType::FILE);
  attr_entry.set_version(2);
  inode_cache.PutIf(std::move(attr_entry));
  ASSERT_TRUE(inode_cache.Get(4001) != nullptr);
  ASSERT_EQ(inode_cache.Size(), 4);

  attr_entry = GenInode(kFsId, 4001, pb::mds::FileType::FILE);
  attr_entry.set_version(3);
  inode_cache.PutIf(std::move(attr_entry));
  ASSERT_TRUE(inode_cache.Get(4001) != nullptr);
  ASSERT_EQ(inode_cache.Size(), 4);

  inode_cache.PutIf(GenInode(kFsId, 4002, pb::mds::FileType::FILE));
  ASSERT_TRUE(inode_cache.Get(4002) != nullptr);
  ASSERT_EQ(inode_cache.Size(), 4);

  inode_cache.PutIf(GenInode(kFsId, 4005, pb::mds::FileType::FILE));
  ASSERT_TRUE(inode_cache.Get(4005) != nullptr);
  ASSERT_EQ(inode_cache.Size(), 5);

  auto inodes = inode_cache.Get({4001, 4002, 4003, 5000, 6000});
  std::sort(inodes.begin(), inodes.end(),  // NOLINT
            [](const InodeSPtr& a, const InodeSPtr& b) {
              return a->Ino() < b->Ino();
            });
  ASSERT_EQ(inodes.size(), 3);
  ASSERT_EQ(inodes[0]->Ino(), 4001);
  ASSERT_EQ(inodes[1]->Ino(), 4002);
  ASSERT_EQ(inodes[2]->Ino(), 4003);

  inodes = inode_cache.GetAll();
  std::sort(inodes.begin(), inodes.end(),  // NOLINT
            [](const InodeSPtr& a, const InodeSPtr& b) {
              return a->Ino() < b->Ino();
            });
  ASSERT_EQ(inodes.size(), 5);
  ASSERT_EQ(inodes[0]->Ino(), 4001);
  ASSERT_EQ(inodes[1]->Ino(), 4002);
  ASSERT_EQ(inodes[2]->Ino(), 4003);
  ASSERT_EQ(inodes[3]->Ino(), 4004);
  ASSERT_EQ(inodes[4]->Ino(), 4005);
}

TEST_F(InodeCacheTest, Benchmark) {
  GTEST_SKIP() << "Skip InodeCacheTest.Benchmark test case.";

  InodeCache inode_cache(kFsId);

  std::atomic<Ino> ino_gen{1000000000000};
  // create 2 thread to put inodes
  constexpr int kThreadNum = 4;
  std::vector<std::thread> threads;
  for (int i = 0; i < kThreadNum; ++i) {
    threads.emplace_back([thread_no = i, &inode_cache, &ino_gen]() {
      for (;;) {
        Ino ino = ino_gen.fetch_add(1, std::memory_order_relaxed);
        inode_cache.PutIf(GenInode(kFsId, ino, pb::mds::FileType::FILE));
      }
    });
  }

  // create 10 threads to get inodes
  constexpr int kGetThreadNum = 32;
  for (int i = 0; i < kGetThreadNum; ++i) {
    threads.emplace_back([thread_no = i, &inode_cache, &ino_gen]() {
      for (;;) {
        Ino ino =
            1000000000000 + Helper::GenerateRealRandomInteger(0, 100000000);
        auto inode = inode_cache.Get(ino);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs