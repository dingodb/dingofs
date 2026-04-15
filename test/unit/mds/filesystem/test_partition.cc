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
#include <cstdint>
#include <string>
#include <vector>

#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "gtest/gtest.h"
#include "mds/common/helper.h"
#include "mds/filesystem/dentry.h"
#include "mds/filesystem/inode.h"
#include "mds/filesystem/partition.h"
#include "mds/filesystem/store_operation.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {
namespace unit_test {

const int64_t kFsId = 1000;
const Ino kParentIno = 100;

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

static pb::mds::Dentry GenDentry(uint32_t fs_id, uint64_t parent, uint64_t ino,
                                 const std::string& name,
                                 pb::mds::FileType type) {
  pb::mds::Dentry dentry;
  dentry.set_fs_id(fs_id);
  dentry.set_parent(parent);
  dentry.set_ino(ino);
  dentry.set_name(name);
  dentry.set_type(type);
  return dentry;
}

// Mock OperationProcessor for testing
class MockOperationProcessor : public OperationProcessor {
 public:
  MockOperationProcessor() : OperationProcessor(nullptr) {}

  bool Init() { return true; }
  bool Destroy() { return true; }

  bool RunBatched(Operation* operation) {
    // For UpdateShardBoundariesOperation, just simulate success
    if (operation->GetOpType() == Operation::OpType::kUpdateShardBoundaries) {
      operation->NotifyEvent();
      return true;
    }
    return false;
  }

  Status RunAlone(Operation* operation) {
    // For ScanDirShardOperation, return empty results
    if (operation->GetOpType() == Operation::OpType::kScanDirShard) {
      return Status::OK();
    }
    return Status(pb::error::EINTERNAL, "mock not implemented");
  }
};

class DirShardTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(DirShardTest, BasicPutGetDelete) {
  Range range{"", ""};
  DirShardSPtr shard = DirShard::New(1, range, 1, std::vector<Dentry>{});

  ASSERT_TRUE(shard != nullptr);
  ASSERT_EQ(shard->ID(), 1);
  ASSERT_EQ(shard->Version(), 1);
  ASSERT_TRUE(shard->Empty());

  // Put dentry
  Dentry dentry1(
      GenDentry(kFsId, kParentIno, 200, "file1", pb::mds::FileType::FILE));
  shard->Put(dentry1);

  ASSERT_FALSE(shard->Empty());
  ASSERT_EQ(shard->Size(), 1);

  // Get dentry
  Dentry out;
  ASSERT_TRUE(shard->Get("file1", out));
  ASSERT_EQ(out.Name(), "file1");
  ASSERT_EQ(out.INo(), 200);

  // Get non-existent
  ASSERT_FALSE(shard->Get("nonexistent", out));

  // Delete dentry
  shard->Delete("file1");
  ASSERT_TRUE(shard->Empty());
  ASSERT_FALSE(shard->Get("file1", out));
}

TEST_F(DirShardTest, MultipleDentries) {
  Range range{"", ""};
  DirShardSPtr shard = DirShard::New(1, range, 1, std::vector<Dentry>{});

  // Put multiple dentries
  for (int i = 0; i < 10; ++i) {
    Dentry dentry(GenDentry(kFsId, kParentIno, 200 + i,
                            fmt::format("file{}", i), pb::mds::FileType::FILE));
    shard->Put(dentry);
  }

  ASSERT_EQ(shard->Size(), 10);
  ASSERT_FALSE(shard->Empty());

  // Get all
  for (int i = 0; i < 10; ++i) {
    Dentry out;
    ASSERT_TRUE(shard->Get(fmt::format("file{}", i), out));
    ASSERT_EQ(out.INo(), 200 + i);
  }
}

TEST_F(DirShardTest, Scan) {
  Range range{"", ""};
  std::vector<Dentry> initial_dentries;
  initial_dentries.reserve(10);
  for (int i = 0; i < 10; ++i) {
    initial_dentries.emplace_back(GenDentry(kFsId, kParentIno, 200 + i,
                                            fmt::format("file{:02d}", i),
                                            pb::mds::FileType::FILE));
  }
  DirShardSPtr shard = DirShard::New(1, range, 1, initial_dentries);

  ASSERT_EQ(shard->Size(), 10);

  // Scan all
  std::vector<Dentry> results;
  shard->Scan("", 100, false, results);
  ASSERT_EQ(results.size(), 10);

  // Scan with limit
  results.clear();
  shard->Scan("", 5, false, results);
  ASSERT_EQ(results.size(), 5);

  // Scan from specific name
  results.clear();
  shard->Scan("file05", 100, false, results);
  ASSERT_EQ(results.size(), 5);

  // Scan only directories
  results.clear();
  shard->Scan("", 100, true, results);
  ASSERT_EQ(results.size(), 0);
}

TEST_F(DirShardTest, Contains) {
  // Full range shard
  Range range1{"", ""};
  DirShardSPtr shard1 = DirShard::New(1, range1, 1, std::vector<Dentry>{});
  ASSERT_TRUE(shard1->Contains("anything"));
  ASSERT_TRUE(shard1->Contains(""));

  // Bounded range shard [a, c)
  Range range2{"a", "c"};
  DirShardSPtr shard2 = DirShard::New(2, range2, 1, std::vector<Dentry>{});
  ASSERT_TRUE(shard2->Contains("a"));
  ASSERT_TRUE(shard2->Contains("apple"));
  ASSERT_TRUE(shard2->Contains("b"));
  ASSERT_FALSE(shard2->Contains("c"));
  ASSERT_FALSE(shard2->Contains("d"));
  ASSERT_FALSE(shard2->Contains(""));

  // Half-open range [c, )
  Range range3{"c", ""};
  DirShardSPtr shard3 = DirShard::New(3, range3, 1, std::vector<Dentry>{});
  ASSERT_TRUE(shard3->Contains("c"));
  ASSERT_TRUE(shard3->Contains("z"));
  ASSERT_FALSE(shard3->Contains("a"));
  ASSERT_FALSE(shard3->Contains("b"));
}

TEST_F(DirShardTest, IsLastShard) {
  Range range1{"", ""};
  DirShardSPtr shard1 = DirShard::New(1, range1, 1, std::vector<Dentry>{});
  ASSERT_TRUE(shard1->IsLastShard());

  Range range2{"a", ""};
  DirShardSPtr shard2 = DirShard::New(2, range2, 1, std::vector<Dentry>{});
  ASSERT_TRUE(shard2->IsLastShard());

  Range range3{"a", "c"};
  DirShardSPtr shard3 = DirShard::New(3, range3, 1, std::vector<Dentry>{});
  ASSERT_FALSE(shard3->IsLastShard());
}

TEST_F(DirShardTest, Mid) {
  Range range{"", ""};
  std::vector<Dentry> initial_dentries;
  initial_dentries.reserve(10);
  for (int i = 0; i < 10; ++i) {
    initial_dentries.emplace_back(GenDentry(kFsId, kParentIno, 200 + i,
                                            fmt::format("file{:02d}", i),
                                            pb::mds::FileType::FILE));
  }
  DirShardSPtr shard = DirShard::New(1, range, 1, initial_dentries);

  std::string mid = shard->Mid();
  ASSERT_EQ(mid, "file05");
}

TEST_F(DirShardTest, Split) {
  Range range{"", ""};
  std::vector<Dentry> initial_dentries;
  initial_dentries.reserve(10);
  for (int i = 0; i < 10; ++i) {
    initial_dentries.emplace_back(GenDentry(kFsId, kParentIno, 200 + i,
                                            fmt::format("file{:02d}", i),
                                            pb::mds::FileType::FILE));
  }
  DirShardSPtr shard = DirShard::New(1, range, 1, initial_dentries);

  auto pair_shards = shard->Split("file05", 2, 3);
  DirShardSPtr left = pair_shards.first;
  DirShardSPtr right = pair_shards.second;

  ASSERT_NE(left, nullptr);
  ASSERT_NE(right, nullptr);
  ASSERT_EQ(left->ID(), 2);
  ASSERT_EQ(right->ID(), 3);
  ASSERT_EQ(left->Size(), 5);
  ASSERT_EQ(right->Size(), 5);
  ASSERT_EQ(left->Start(), "");
  ASSERT_EQ(left->End(), "file05");
  ASSERT_EQ(right->Start(), "file05");
  ASSERT_EQ(right->End(), "");

  // Verify left contains [start, file05)
  ASSERT_TRUE(left->Contains("file00"));
  ASSERT_TRUE(left->Contains("file04"));
  ASSERT_FALSE(left->Contains("file05"));

  // Verify right contains [file05, end)
  ASSERT_TRUE(right->Contains("file05"));
  ASSERT_TRUE(right->Contains("file09"));
  ASSERT_FALSE(right->Contains("file04"));
}

TEST_F(DirShardTest, SizeAndBytes) {
  Range range{"", ""};
  DirShardSPtr shard = DirShard::New(1, range, 1, std::vector<Dentry>{});

  ASSERT_EQ(shard->Size(), 0);
  ASSERT_EQ(shard->Bytes(), 0);

  Dentry dentry1(
      GenDentry(kFsId, kParentIno, 200, "file1", pb::mds::FileType::FILE));
  shard->Put(dentry1);

  ASSERT_EQ(shard->Size(), 1);
  ASSERT_EQ(shard->Bytes(), sizeof(Dentry));
}

TEST_F(DirShardTest, ToString) {
  Range range{"a", "c"};
  DirShardSPtr shard = DirShard::New(1, range, 1, std::vector<Dentry>{});

  std::string str = shard->ToString();
  ASSERT_NE(str.find("id(1)"), std::string::npos);
  ASSERT_NE(str.find("version(1)"), std::string::npos);
}

TEST_F(DirShardTest, UpdateLastActiveTime) {
  Range range{"", ""};
  DirShardSPtr shard = DirShard::New(1, range, 1, std::vector<Dentry>{});

  uint64_t before = shard->LastActiveTimeS();

  // Sleep a bit to ensure time changes
  usleep(1000);

  Dentry dentry1(
      GenDentry(kFsId, kParentIno, 200, "file1", pb::mds::FileType::FILE));
  shard->Put(dentry1);

  uint64_t after = shard->LastActiveTimeS();
  ASSERT_GE(after, before);
}

TEST_F(DirShardTest, EmptyShardOperations) {
  Range range{"", ""};
  DirShardSPtr shard = DirShard::New(1, range, 1, std::vector<Dentry>{});

  // Mid on empty shard should return empty string
  ASSERT_EQ(shard->Mid(), "");

  // Scan on empty shard should return empty results
  std::vector<Dentry> results;
  shard->Scan("", 100, false, results);
  ASSERT_TRUE(results.empty());

  // Get on empty shard should return false
  Dentry out;
  ASSERT_FALSE(shard->Get("anything", out));

  // Delete on empty shard should be fine
  shard->Delete("nonexistent");
}

TEST_F(DirShardTest, OverwriteDentry) {
  Range range{"", ""};
  DirShardSPtr shard = DirShard::New(1, range, 1, std::vector<Dentry>{});

  // Put initial dentry
  Dentry dentry1(
      GenDentry(kFsId, kParentIno, 200, "file1", pb::mds::FileType::FILE));
  shard->Put(dentry1);

  // Put with same name but different ino
  Dentry dentry2(
      GenDentry(kFsId, kParentIno, 300, "file1", pb::mds::FileType::DIRECTORY));
  shard->Put(dentry2);

  ASSERT_EQ(shard->Size(), 1);

  Dentry out;
  ASSERT_TRUE(shard->Get("file1", out));
  ASSERT_EQ(out.INo(), 300);
  ASSERT_EQ(out.Type(), pb::mds::FileType::DIRECTORY);
}

TEST_F(DirShardTest, ScanWithMixedTypes) {
  Range range{"", ""};
  std::vector<Dentry> initial_dentries;

  // Add mix of files and directories
  for (int i = 0; i < 5; ++i) {
    initial_dentries.emplace_back(GenDentry(kFsId, kParentIno, 200 + i,
                                            fmt::format("file{:02d}", i),
                                            pb::mds::FileType::FILE));
    initial_dentries.emplace_back(GenDentry(kFsId, kParentIno, 300 + i,
                                            fmt::format("dir{:02d}", i),
                                            pb::mds::FileType::DIRECTORY));
  }
  DirShardSPtr shard = DirShard::New(1, range, 1, initial_dentries);

  ASSERT_EQ(shard->Size(), 10);

  // Scan all
  std::vector<Dentry> results;
  shard->Scan("", 100, false, results);
  ASSERT_EQ(results.size(), 10);

  // Scan only directories
  results.clear();
  shard->Scan("", 100, true, results);
  ASSERT_EQ(results.size(), 5);
}

TEST_F(DirShardTest, RangeBoundariesEdgeCases) {
  // Test exact boundary matches
  Range range{"a", "b"};
  DirShardSPtr shard = DirShard::New(1, range, 1, std::vector<Dentry>{});

  ASSERT_TRUE(shard->Contains("a"));
  ASSERT_FALSE(shard->Contains("b"));

  // Empty end boundary means unlimited
  Range range2{"z", ""};
  DirShardSPtr shard2 = DirShard::New(2, range2, 1, std::vector<Dentry>{});

  ASSERT_TRUE(shard2->Contains("z"));
  ASSERT_TRUE(shard2->Contains("zzzz"));
  ASSERT_FALSE(shard2->Contains("y"));

  // Empty start and end means all
  Range range3{"", ""};
  DirShardSPtr shard3 = DirShard::New(3, range3, 1, std::vector<Dentry>{});

  ASSERT_TRUE(shard3->Contains(""));
  ASSERT_TRUE(shard3->Contains("anything"));
}

class PartitionCacheTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(PartitionCacheTest, PutIfAndGet) {
  PartitionCache cache(kFsId);

  auto mock_processor = std::make_shared<MockOperationProcessor>();

  // Create inode for partition
  auto inode =
      Inode::New(GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY));

  // Create partition
  PartitionPtr partition = ShardPartition::New(mock_processor, inode);

  // Put into cache
  auto result = cache.PutIf(partition);
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->INo(), kParentIno);

  // Get from cache
  auto got = cache.Get(kParentIno);
  ASSERT_NE(got, nullptr);
  ASSERT_EQ(got->INo(), kParentIno);

  ASSERT_EQ(cache.Size(), 1);
}

TEST_F(PartitionCacheTest, PutIfExisting) {
  PartitionCache cache(kFsId);

  auto mock_processor = std::make_shared<MockOperationProcessor>();

  // Create first partition
  auto inode1 =
      Inode::New(GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 1));
  PartitionPtr partition1 = ShardPartition::New(mock_processor, inode1);

  // Put first
  auto result1 = cache.PutIf(partition1);
  ASSERT_EQ(result1->BaseVersion(), 1);

  // Create second partition with higher version
  auto inode2 =
      Inode::New(GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 2));

  PartitionPtr partition2 = ShardPartition::New(mock_processor, inode2);
  // Note: PutIf will call Refresh on the existing partition when putting a
  // partition with same ino

  // Put second - should return existing (and trigger Refresh)
  auto result2 = cache.PutIf(partition2);
  ASSERT_EQ(result2->INo(), kParentIno);

  ASSERT_EQ(cache.Size(), 1);
}

TEST_F(PartitionCacheTest, Delete) {
  PartitionCache cache(kFsId);

  auto mock_processor = std::make_shared<MockOperationProcessor>();

  // Create and put partition
  auto inode =
      Inode::New(GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY));
  PartitionPtr partition = ShardPartition::New(mock_processor, inode);
  cache.PutIf(partition);

  ASSERT_NE(cache.Get(kParentIno), nullptr);
  ASSERT_EQ(cache.Size(), 1);

  // Delete
  cache.Delete(kParentIno);
  ASSERT_EQ(cache.Get(kParentIno), nullptr);
  ASSERT_EQ(cache.Size(), 0);
}

TEST_F(PartitionCacheTest, DeleteIf) {
  PartitionCache cache(kFsId);

  auto mock_processor = std::make_shared<MockOperationProcessor>();

  // Create multiple partitions
  for (int i = 0; i < 10; ++i) {
    auto inode =
        Inode::New(GenInode(kFsId, 100 + i, pb::mds::FileType::DIRECTORY));
    PartitionPtr partition = ShardPartition::New(mock_processor, inode);
    cache.PutIf(partition);
  }

  ASSERT_EQ(cache.Size(), 10);

  // Delete if ino >= 105
  cache.DeleteIf([](const Ino& ino) { return ino >= 105; });

  ASSERT_EQ(cache.Size(), 5);
  ASSERT_NE(cache.Get(100), nullptr);
  ASSERT_NE(cache.Get(104), nullptr);
  ASSERT_EQ(cache.Get(105), nullptr);
  ASSERT_EQ(cache.Get(109), nullptr);
}

TEST_F(PartitionCacheTest, Clear) {
  PartitionCache cache(kFsId);

  auto mock_processor = std::make_shared<MockOperationProcessor>();

  // Create multiple partitions
  for (int i = 0; i < 5; ++i) {
    auto inode =
        Inode::New(GenInode(kFsId, 100 + i, pb::mds::FileType::DIRECTORY));
    PartitionPtr partition = ShardPartition::New(mock_processor, inode);
    cache.PutIf(partition);
  }

  ASSERT_EQ(cache.Size(), 5);

  // Clear all
  cache.Clear();

  ASSERT_EQ(cache.Size(), 0);
  ASSERT_EQ(cache.Get(100), nullptr);
}

TEST_F(PartitionCacheTest, GetAll) {
  PartitionCache cache(kFsId);

  auto mock_processor = std::make_shared<MockOperationProcessor>();

  // Create multiple partitions
  for (int i = 0; i < 5; ++i) {
    auto inode =
        Inode::New(GenInode(kFsId, 100 + i, pb::mds::FileType::DIRECTORY));
    PartitionPtr partition = ShardPartition::New(mock_processor, inode);
    cache.PutIf(partition);
  }

  auto all = cache.GetAll();
  ASSERT_EQ(all.size(), 5);
}

TEST_F(PartitionCacheTest, CacheMiss) {
  PartitionCache cache(kFsId);

  // Get non-existent partition
  auto got = cache.Get(9999);
  ASSERT_EQ(got, nullptr);
}

TEST_F(PartitionCacheTest, MultiplePutIf) {
  PartitionCache cache(kFsId);

  auto mock_processor = std::make_shared<MockOperationProcessor>();

  // Add multiple different partitions
  for (int i = 0; i < 100; ++i) {
    auto inode =
        Inode::New(GenInode(kFsId, 1000 + i, pb::mds::FileType::DIRECTORY));
    PartitionPtr partition = ShardPartition::New(mock_processor, inode);
    cache.PutIf(partition);
  }

  ASSERT_EQ(cache.Size(), 100);

  // Verify all are accessible
  for (int i = 0; i < 100; ++i) {
    auto got = cache.Get(1000 + i);
    ASSERT_NE(got, nullptr);
    ASSERT_EQ(got->INo(), 1000 + i);
  }
}

TEST_F(PartitionCacheTest, BytesAndShardSize) {
  PartitionCache cache(kFsId);

  auto mock_processor = std::make_shared<MockOperationProcessor>();

  // Create partition with some data
  auto inode =
      Inode::New(GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY));
  PartitionPtr partition = ShardPartition::New(mock_processor, inode);

  // Note: PutWithInode requires shard to exist first
  // Without fetched shards, ShardSize and Bytes will be 0
  cache.PutIf(partition);

  ASSERT_EQ(cache.Size(), 1);
  ASSERT_EQ(cache.ShardSize(), 0);  // No shards fetched
  ASSERT_EQ(cache.Bytes(), 0);      // No bytes without shards
}

class ShardPartitionBasicTest : public testing::Test {
 protected:
  void SetUp() override {
    mock_processor_ = std::make_shared<MockOperationProcessor>();
    inode_ = Inode::New(
        GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 1));
    partition_ = ShardPartition::New(mock_processor_, inode_);
  }

  void TearDown() override {}

  std::shared_ptr<MockOperationProcessor> mock_processor_;
  InodeSPtr inode_;
  PartitionPtr partition_;
};

TEST_F(ShardPartitionBasicTest, BasicProperties) {
  ASSERT_EQ(partition_->FsId(), kFsId);
  ASSERT_EQ(partition_->INo(), kParentIno);
  ASSERT_EQ(partition_->BaseVersion(), 1);
  ASSERT_EQ(partition_->DeltaVersion(), 1);
}

TEST_F(ShardPartitionBasicTest, ParentInode) {
  auto parent = partition_->ParentInode();
  ASSERT_NE(parent, nullptr);
  ASSERT_EQ(parent->Ino(), kParentIno);

  // Set new parent inode
  auto new_inode =
      Inode::New(GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY));
  partition_->SetParentInode(new_inode);

  auto got = partition_->ParentInode();
  ASSERT_EQ(got->Ino(), kParentIno);
}

TEST_F(ShardPartitionBasicTest, PutWithVersion) {
  Dentry dentry(
      GenDentry(kFsId, kParentIno, 200, "file1", pb::mds::FileType::FILE));

  // Put with version - stores in delta ops
  partition_->Put(dentry, 2);

  // Note: Size() only counts dentries in shards, not delta ops
  // So size will be 0 because no shard has been fetched yet
  ASSERT_EQ(partition_->Size(), 0);
}

TEST_F(ShardPartitionBasicTest, DeleteSingle) {
  // Note: Delete only works if shard exists
  // Without a fetched shard, Delete just adds to delta ops
  partition_->Delete("file1", 2);
  ASSERT_EQ(partition_->Size(), 0);
}

TEST_F(ShardPartitionBasicTest, DeleteMultiple) {
  // Delete multiple - only adds to delta ops since no shard exists
  std::vector<std::string> names = {"file0", "file2", "file4"};
  partition_->Delete(names, 2);

  ASSERT_EQ(partition_->Size(), 0);
}

TEST_F(ShardPartitionBasicTest, Empty) {
  ASSERT_TRUE(partition_->Empty());

  // Put adds to delta ops but Size() counts only shard entries
  Dentry dentry(
      GenDentry(kFsId, kParentIno, 200, "file1", pb::mds::FileType::FILE));
  partition_->Put(dentry, 2);

  // Still empty because no shard fetched yet
  ASSERT_TRUE(partition_->Empty());
}

TEST_F(ShardPartitionBasicTest, SizeAndShardSize) {
  ASSERT_EQ(partition_->Size(), 0);
  ASSERT_EQ(partition_->ShardSize(), 0);

  // Put dentries - they go to delta ops since no shard fetched yet
  for (int i = 0; i < 10; ++i) {
    Dentry dentry(GenDentry(kFsId, kParentIno, 200 + i,
                            fmt::format("file{}", i), pb::mds::FileType::FILE));
    partition_->Put(dentry, 2 + i);
  }

  // Size counts entries in shards, not delta ops
  ASSERT_EQ(partition_->Size(), 0);
  ASSERT_EQ(partition_->ShardSize(), 0);
}

TEST_F(ShardPartitionBasicTest, NeedCompact) {
  // Initially should not need compact
  ASSERT_FALSE(partition_->NeedCompact());
}

TEST_F(ShardPartitionBasicTest, GetAll) {
  // Without fetched shards, GetAll returns empty
  auto all = partition_->GetAll();
  ASSERT_EQ(all.size(), 0);
}

TEST_F(ShardPartitionBasicTest, Refresh) {
  // Create new inode with higher version and put into a new cache to trigger
  // refresh
  PartitionCache cache(kFsId);

  // Put the partition first
  cache.PutIf(partition_);
  ASSERT_EQ(partition_->BaseVersion(), 1);

  // Create new inode with higher version
  auto new_inode =
      Inode::New(GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 2));
  ASSERT_EQ(new_inode->Version(), 2);

  // Create new partition with higher version inode
  auto partition2 = ShardPartition::New(mock_processor_, new_inode);

  // PutIf should trigger refresh on existing partition (since ino already
  // exists)
  auto result = cache.PutIf(partition2);
  // Note: Refresh clears shards, so size should be 0 after refresh
}

TEST_F(ShardPartitionBasicTest, PartitionCacheIntegration) {
  // Create a cache and put the partition
  PartitionCache cache(kFsId);
  cache.PutIf(partition_);
  ASSERT_EQ(cache.Size(), 1);

  // Create new inode with higher version
  auto new_inode =
      Inode::New(GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 2));

  // Create new partition with higher version - PutIf will trigger refresh on
  // existing
  auto partition2 = ShardPartition::New(mock_processor_, new_inode);
  auto result = cache.PutIf(partition2);
  ASSERT_EQ(result->INo(), kParentIno);

  // Note: After refresh through PutIf, the existing partition's shards are
  // cleared
}

TEST_F(ShardPartitionBasicTest, DeltaVersionTracking) {
  ASSERT_EQ(partition_->DeltaVersion(), 1);

  // Put dentry with version
  Dentry dentry(
      GenDentry(kFsId, kParentIno, 200, "file1", pb::mds::FileType::FILE));
  partition_->Put(dentry, 3);

  ASSERT_EQ(partition_->DeltaVersion(), 3);

  // Delete with higher version
  partition_->Delete("file1", 5);

  ASSERT_EQ(partition_->DeltaVersion(), 5);

  // Delete with lower version should not change delta_version
  partition_->Delete("file1", 4);

  ASSERT_EQ(partition_->DeltaVersion(), 5);
}

TEST_F(ShardPartitionBasicTest, DeleteNonExistent) {
  // Should not crash when deleting non-existent dentry
  partition_->Delete("nonexistent", 2);

  ASSERT_TRUE(partition_->Empty());
  ASSERT_EQ(partition_->Size(), 0);
}

class ShardPartitionWithBoundariesTest : public testing::Test {
 protected:
  void SetUp() override {
    mock_processor_ = std::make_shared<MockOperationProcessor>();

    // Create inode with shard boundaries
    pb::mds::Inode inode_proto =
        GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 1);
    inode_proto.add_shard_boundaries("m");
    inode_ = Inode::New(inode_proto);

    partition_ = ShardPartition::New(mock_processor_, inode_);
  }

  void TearDown() override {}

  std::shared_ptr<MockOperationProcessor> mock_processor_;
  InodeSPtr inode_;
  PartitionPtr partition_;
};

TEST_F(ShardPartitionWithBoundariesTest, ShardBoundaries) {
  // With boundary "m", we have two shards: [", "m") and ["m", ")
  ASSERT_EQ(partition_->ShardSize(), 0);  // No shards loaded yet
}

TEST_F(ShardPartitionWithBoundariesTest, PutMultipleShards) {
  // Put dentries in different shards using Put with version
  // Note: Without fetched shards, dentries go to delta ops, not to Size()
  Dentry dentry1(
      GenDentry(kFsId, kParentIno, 200, "a_file", pb::mds::FileType::FILE));
  Dentry dentry2(
      GenDentry(kFsId, kParentIno, 201, "z_file", pb::mds::FileType::FILE));

  partition_->Put(dentry1, 2);
  partition_->Put(dentry2, 3);

  // Size() counts entries in shards, not delta ops
  ASSERT_EQ(partition_->Size(), 0);
}

class DirShardConstructFromDentriesTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(DirShardConstructFromDentriesTest, ConstructWithDentries) {
  std::vector<Dentry> dentries;
  for (int i = 0; i < 5; ++i) {
    dentries.emplace_back(GenDentry(kFsId, kParentIno, 200 + i,
                                    fmt::format("file{}", i),
                                    pb::mds::FileType::FILE));
  }

  Range range{"", ""};
  DirShardSPtr shard = DirShard::New(1, range, 1, dentries);

  ASSERT_EQ(shard->Size(), 5);

  for (int i = 0; i < 5; ++i) {
    Dentry out;
    ASSERT_TRUE(shard->Get(fmt::format("file{}", i), out));
    ASSERT_EQ(out.INo(), 200 + i);
  }
}

TEST_F(DirShardConstructFromDentriesTest, OutOfRangeDentry) {
  // When constructing, dentries must be within range
  // This is checked by CHECK in the constructor
  // We'll test that dentries in range are accepted
  std::vector<Dentry> dentries;
  dentries.emplace_back(
      GenDentry(kFsId, kParentIno, 200, "b", pb::mds::FileType::FILE));
  dentries.emplace_back(
      GenDentry(kFsId, kParentIno, 201, "c", pb::mds::FileType::FILE));

  Range range{"a", "d"};
  DirShardSPtr shard = DirShard::New(1, range, 1, dentries);

  ASSERT_EQ(shard->Size(), 2);

  Dentry out;
  ASSERT_TRUE(shard->Get("b", out));
  ASSERT_TRUE(shard->Get("c", out));
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
