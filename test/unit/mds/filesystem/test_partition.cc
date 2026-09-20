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
#include <initializer_list>
#include <string>
#include <utility>
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

static AttrMutationEntry GenMutation(uint64_t ino, uint32_t index,
                                     uint64_t delta_version) {
  AttrMutationEntry mutation;
  mutation.set_ino(ino);
  mutation.set_index(index);
  mutation.set_delta_version(delta_version);

  auto now_ns = utils::TimestampNs();
  mutation.set_ctime(now_ns);
  mutation.set_mtime(now_ns);
  mutation.set_atime(now_ns);

  return mutation;
}

static AttrWithMutation GenAttrWithMutation(
    uint32_t fs_id, uint64_t ino, uint64_t base_version,
    std::initializer_list<std::pair<uint32_t, uint64_t>> mutations) {
  AttrWithMutation attr_with_mutation;
  attr_with_mutation.attr =
      GenInode(fs_id, ino, pb::mds::FileType::DIRECTORY, base_version);
  for (const auto& [index, delta_version] : mutations) {
    attr_with_mutation.mutations.push_back(
        GenMutation(ino, index, delta_version));
  }

  return attr_with_mutation;
}

// Mock OperationProcessor for testing
class MockOperationProcessor : public OperationProcessor {
 public:
  MockOperationProcessor() : OperationProcessor(nullptr) {}

  bool Init() { return true; }
  bool Stop() { return true; }

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
  ASSERT_EQ(shard->VersionVec().BaseVersion(), 1);
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

TEST_F(DirShardTest, SnapshotPaginates) {
  std::vector<Dentry> initial_dentries;
  initial_dentries.emplace_back(
      GenDentry(kFsId, kParentIno, 200, "first", pb::mds::FileType::FILE));
  initial_dentries.emplace_back(GenDentry(kFsId, kParentIno, 201, "second",
                                          pb::mds::FileType::DIRECTORY));
  DirShardSPtr shard = DirShard::New(1, Range{"", ""}, 1, initial_dentries);

  std::vector<Dentry> snapshot;
  shard->Snapshot(1, 1, snapshot);

  ASSERT_EQ(1, snapshot.size());
  EXPECT_EQ("second", snapshot[0].Name());
  EXPECT_EQ(pb::mds::FileType::DIRECTORY, snapshot[0].Type());
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
  ASSERT_NE(str.find("version(1 0)"), std::string::npos);
}

TEST_F(DirShardTest, VersionVecFromBaseVersion) {
  DirShardSPtr shard =
      DirShard::New(1, Range{"", ""}, 10, std::vector<Dentry>{});

  ASSERT_EQ(shard->VersionVec().BaseVersion(), 10);
  ASSERT_EQ(shard->VersionVec().CompleteVersion(), 10);
  ASSERT_EQ(shard->VersionVec().total_delta_version, 0);
  ASSERT_EQ(shard->VersionVec().DeltaVersion(0), 0);
  ASSERT_EQ(shard->VersionString(), "10 0");
}

TEST_F(DirShardTest, VersionVecFromAttrWithMutation) {
  AttrVersionVec version_vec(
      GenAttrWithMutation(kFsId, kParentIno, 50, {{2, 3}, {5, 7}}));

  DirShardSPtr shard =
      DirShard::New(1, Range{"", ""}, version_vec, std::vector<Dentry>{});

  ASSERT_EQ(shard->VersionVec().BaseVersion(), 50);
  ASSERT_EQ(shard->VersionVec().DeltaVersion(2), 3);
  ASSERT_EQ(shard->VersionVec().DeltaVersion(5), 7);
  ASSERT_EQ(shard->VersionVec().CompleteVersion(), 60);
  ASSERT_EQ(shard->VersionString(), "50 10");
}

TEST_F(DirShardTest, SplitPreservesVersionVec) {
  AttrVersionVec version_vec(100);
  version_vec.PutIf(AttrVersion(1, 5));

  std::vector<Dentry> dentries;
  for (int i = 0; i < 10; ++i) {
    dentries.emplace_back(GenDentry(kFsId, kParentIno, 200 + i,
                                    fmt::format("file{:02d}", i),
                                    pb::mds::FileType::FILE));
  }
  DirShardSPtr shard =
      DirShard::New(1, Range{"", ""}, version_vec, dentries);

  auto [left, right] = shard->Split("file05", 2, 3);

  // both halves inherit the parent shard version
  for (const auto& half : {left, right}) {
    ASSERT_EQ(half->VersionVec().BaseVersion(), 100);
    ASSERT_EQ(half->VersionVec().DeltaVersion(1), 5);
    ASSERT_EQ(half->VersionVec().CompleteVersion(), 105);
  }

  // version vectors are value copies, mutating the source has no effect
  version_vec.PutIf(AttrVersion(1, 9));
  ASSERT_EQ(shard->VersionVec().CompleteVersion(), 105);
  ASSERT_EQ(left->VersionVec().CompleteVersion(), 105);
  ASSERT_EQ(right->VersionVec().CompleteVersion(), 105);
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

  // Create partition
  PartitionPtr partition = ShardPartition::New(
      mock_processor,
      GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY));

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
  PartitionPtr partition1 = ShardPartition::New(
      mock_processor,
      GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 1));

  // Put first
  auto result1 = cache.PutIf(partition1);
  ASSERT_EQ(result1->BaseVersion(), 1);

  // Create second partition with higher version
  PartitionPtr partition2 = ShardPartition::New(
      mock_processor,
      GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 2));
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
  PartitionPtr partition = ShardPartition::New(
      mock_processor,
      GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY));
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
    PartitionPtr partition = ShardPartition::New(
        mock_processor, GenInode(kFsId, 100 + i, pb::mds::FileType::DIRECTORY));
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
    PartitionPtr partition = ShardPartition::New(
        mock_processor, GenInode(kFsId, 100 + i, pb::mds::FileType::DIRECTORY));
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
    PartitionPtr partition = ShardPartition::New(
        mock_processor, GenInode(kFsId, 100 + i, pb::mds::FileType::DIRECTORY));
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
    PartitionPtr partition = ShardPartition::New(
        mock_processor,
        GenInode(kFsId, 1000 + i, pb::mds::FileType::DIRECTORY));
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
  PartitionPtr partition = ShardPartition::New(
      mock_processor,
      GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY));

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
    partition_ = ShardPartition::New(
        mock_processor_,
        GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 1));
  }

  void TearDown() override {}

  std::shared_ptr<MockOperationProcessor> mock_processor_;
  PartitionPtr partition_;
};

TEST_F(ShardPartitionBasicTest, BasicProperties) {
  ASSERT_EQ(partition_->FsId(), kFsId);
  ASSERT_EQ(partition_->INo(), kParentIno);
  ASSERT_EQ(partition_->BaseVersion(), 1);
  ASSERT_EQ(partition_->CompleteVersion(), 1);
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

TEST_F(ShardPartitionBasicTest, DumpPaginatesPendingDentryOperations) {
  partition_->Put(Dentry(GenDentry(kFsId, kParentIno, 200, "first",
                                   pb::mds::FileType::FILE)),
                  2);
  partition_->Put(Dentry(GenDentry(kFsId, kParentIno, 201, "second",
                                   pb::mds::FileType::DIRECTORY)),
                  3);

  Json::Value value;
  partition_->Dump(value, 0, 100, 1, 1);

  ASSERT_EQ(2, value["delta_dentry_ops_total"].asUInt64());
  ASSERT_EQ(1, value["delta_dentry_ops"].size());
  EXPECT_EQ("ADD", value["delta_dentry_ops"][0]["op"].asString());
  EXPECT_EQ("second",
            value["delta_dentry_ops"][0]["dentry"]["name"].asString());
  EXPECT_EQ("DIRECTORY",
            value["delta_dentry_ops"][0]["dentry"]["type"].asString());
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
  ASSERT_EQ(new_inode->CompleteVersion(), 2);

  // Create new partition with higher version inode
  auto partition2 = ShardPartition::New(
      mock_processor_,
      GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 2));

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
  auto partition2 = ShardPartition::New(
      mock_processor_,
      GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 2));
  auto result = cache.PutIf(partition2);
  ASSERT_EQ(result->INo(), kParentIno);

  // Note: After refresh through PutIf, the existing partition's shards are
  // cleared
}

TEST_F(ShardPartitionBasicTest, DeltaVersionTracking) {
  ASSERT_EQ(partition_->CompleteVersion(), 1);

  // Put dentry with version
  Dentry dentry(
      GenDentry(kFsId, kParentIno, 200, "file1", pb::mds::FileType::FILE));
  partition_->Put(dentry, 3);

  ASSERT_EQ(partition_->CompleteVersion(), 3);

  // Delete with higher version
  partition_->Delete("file1", 5);

  ASSERT_EQ(partition_->CompleteVersion(), 5);

  // Delete with lower version should not change delta_version
  partition_->Delete("file1", 4);

  ASSERT_EQ(partition_->CompleteVersion(), 5);
}

TEST_F(ShardPartitionBasicTest, DeleteNonExistent) {
  // Should not crash when deleting non-existent dentry
  partition_->Delete("nonexistent", 2);

  ASSERT_TRUE(partition_->Empty());
  ASSERT_EQ(partition_->Size(), 0);
}

class ShardPartitionVersionTest : public testing::Test {
 protected:
  void SetUp() override {
    mock_processor_ = std::make_shared<MockOperationProcessor>();
  }

  void TearDown() override {}

  PartitionPtr NewPartition(uint64_t version) {
    return ShardPartition::New(
        mock_processor_,
        GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, version));
  }

  std::shared_ptr<MockOperationProcessor> mock_processor_;
};

TEST_F(ShardPartitionVersionTest, BaseVersionFromAttrEntry) {
  auto partition = NewPartition(10);

  ASSERT_EQ(partition->BaseVersion(), 10);
  ASSERT_EQ(partition->CompleteVersion(), 10);
  ASSERT_EQ(partition->VersionVec().total_delta_version, 0);
  ASSERT_EQ(partition->VersionVec().ToString(), "10 0");
}

TEST_F(ShardPartitionVersionTest, BaseVersionFromAttrWithMutation) {
  auto partition = ShardPartition::New(
      mock_processor_,
      GenAttrWithMutation(kFsId, kParentIno, 50, {{2, 3}, {5, 7}}));

  ASSERT_EQ(partition->BaseVersion(), 50);
  ASSERT_EQ(partition->VersionVec().DeltaVersion(2), 3);
  ASSERT_EQ(partition->VersionVec().DeltaVersion(5), 7);
  ASSERT_EQ(partition->CompleteVersion(), 60);
  ASSERT_EQ(partition->VersionVec().ToString(), "50 10");
}

TEST_F(ShardPartitionVersionTest, PutWithBaseVersionIsMonotonic) {
  auto partition = NewPartition(10);
  Dentry dentry(
      GenDentry(kFsId, kParentIno, 200, "file1", pb::mds::FileType::FILE));

  partition->Put(dentry, 20);
  ASSERT_EQ(partition->BaseVersion(), 20);
  ASSERT_EQ(partition->CompleteVersion(), 20);

  // stale and equal base versions are rejected
  partition->Put(dentry, 15);
  ASSERT_EQ(partition->BaseVersion(), 20);
  ASSERT_EQ(partition->CompleteVersion(), 20);

  partition->Put(dentry, 20);
  ASSERT_EQ(partition->BaseVersion(), 20);
  ASSERT_EQ(partition->CompleteVersion(), 20);
}

TEST_F(ShardPartitionVersionTest, PutWithDeltaVersionAccumulates) {
  auto partition = NewPartition(100);
  Dentry dentry(
      GenDentry(kFsId, kParentIno, 200, "file1", pb::mds::FileType::FILE));

  partition->Put(dentry, AttrVersion(1, 5));
  ASSERT_EQ(partition->BaseVersion(), 100);
  ASSERT_EQ(partition->VersionVec().DeltaVersion(1), 5);
  ASSERT_EQ(partition->CompleteVersion(), 105);

  // replaying the same delta is a no-op
  partition->Put(dentry, AttrVersion(1, 5));
  ASSERT_EQ(partition->CompleteVersion(), 105);

  // stale delta is rejected
  partition->Put(dentry, AttrVersion(1, 3));
  ASSERT_EQ(partition->CompleteVersion(), 105);

  // newer delta on the same index only adds the difference
  partition->Put(dentry, AttrVersion(1, 8));
  ASSERT_EQ(partition->CompleteVersion(), 108);

  // independent index accumulates
  partition->Put(dentry, AttrVersion(2, 4));
  ASSERT_EQ(partition->VersionVec().DeltaVersion(1), 8);
  ASSERT_EQ(partition->VersionVec().DeltaVersion(2), 4);
  ASSERT_EQ(partition->CompleteVersion(), 112);
}

TEST_F(ShardPartitionVersionTest, DeleteWithDeltaVersionIsMonotonic) {
  auto partition = NewPartition(100);

  partition->Delete("file1", AttrVersion(1, 5));
  ASSERT_EQ(partition->BaseVersion(), 100);
  ASSERT_EQ(partition->CompleteVersion(), 105);

  // stale delta is rejected
  partition->Delete("file1", AttrVersion(1, 3));
  ASSERT_EQ(partition->CompleteVersion(), 105);

  // base version bump keeps the accumulated delta
  partition->Delete("file1", 200);
  ASSERT_EQ(partition->BaseVersion(), 200);
  ASSERT_EQ(partition->CompleteVersion(), 205);
}

TEST_F(ShardPartitionVersionTest, DeleteMultipleWithDeltaVersion) {
  auto partition = NewPartition(100);
  std::vector<std::string> names = {"file0", "file1"};

  partition->Delete(names, AttrVersion(2, 4));
  ASSERT_EQ(partition->BaseVersion(), 100);
  ASSERT_EQ(partition->VersionVec().DeltaVersion(2), 4);
  ASSERT_EQ(partition->CompleteVersion(), 104);

  // stale delta is rejected
  partition->Delete(names, AttrVersion(2, 3));
  ASSERT_EQ(partition->CompleteVersion(), 104);
}

TEST_F(ShardPartitionVersionTest, RefreshVersionMerges) {
  auto partition = NewPartition(10);

  partition->RefreshVersion(AttrVersion(1, 5));
  ASSERT_EQ(partition->BaseVersion(), 10);
  ASSERT_EQ(partition->CompleteVersion(), 15);

  // base version bump keeps the delta
  partition->RefreshVersion(AttrVersion(uint64_t{20}));
  ASSERT_EQ(partition->BaseVersion(), 20);
  ASSERT_EQ(partition->CompleteVersion(), 25);

  // stale version is rejected
  partition->RefreshVersion(AttrVersion(uint64_t{15}));
  ASSERT_EQ(partition->BaseVersion(), 20);
  ASSERT_EQ(partition->CompleteVersion(), 25);
}

TEST_F(ShardPartitionVersionTest, CachePutIfMergesVersionVec) {
  PartitionCache cache(kFsId);

  auto partition = NewPartition(10);
  cache.PutIf(partition);
  ASSERT_EQ(partition->BaseVersion(), 10);

  // stale base but newer delta still advances the delta
  auto stale_base = ShardPartition::New(
      mock_processor_,
      GenAttrWithMutation(kFsId, kParentIno, 5, {{1, 5}}));
  auto result = cache.PutIf(stale_base);
  ASSERT_EQ(result.get(), partition.get());
  ASSERT_EQ(partition->BaseVersion(), 10);
  ASSERT_EQ(partition->VersionVec().DeltaVersion(1), 5);
  ASSERT_EQ(partition->CompleteVersion(), 15);

  // newer base is merged, existing delta is kept
  auto newer_base = ShardPartition::New(
      mock_processor_,
      GenAttrWithMutation(kFsId, kParentIno, 20, {{1, 3}}));
  cache.PutIf(newer_base);
  ASSERT_EQ(partition->BaseVersion(), 20);
  ASSERT_EQ(partition->CompleteVersion(), 25);
}

TEST_F(ShardPartitionVersionTest, CachePutIfCoversDeltaOpsAndPrunes) {
  PartitionCache cache(kFsId);

  auto partition = cache.PutIf(NewPartition(10));
  Dentry dentry(
      GenDentry(kFsId, kParentIno, 200, "file1", pb::mds::FileType::FILE));
  partition->Put(dentry, AttrVersion(1, 5));
  ASSERT_EQ(partition->CompleteVersion(), 15);

  auto newer =
      ShardPartition::New(mock_processor_,
                          GenAttrWithMutation(kFsId, kParentIno, 10, {{1, 8}}));
  cache.PutIf(newer);

  ASSERT_EQ(partition->CompleteVersion(), 18);

  // the delta op is covered by the merged version and gets pruned
  Json::Value value;
  partition->Dump(value);
  ASSERT_EQ(value["delta_dentry_ops_total"].asUInt64(), 0);
}

class ShardPartitionWithBoundariesTest : public testing::Test {
 protected:
  void SetUp() override {
    mock_processor_ = std::make_shared<MockOperationProcessor>();

    // Create inode with shard boundaries
    pb::mds::Inode inode_proto =
        GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 1);
    inode_proto.add_shard_boundaries("m");
    partition_ = ShardPartition::New(mock_processor_, inode_proto);
  }

  void TearDown() override {}

  std::shared_ptr<MockOperationProcessor> mock_processor_;
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

// --- Performance test ---
// Run: PARTITION_PERF=1 ./test_mds --gtest_filter=ShardPartitionPerfTest.*
// Optionally override count: PERF_PUT_COUNT=1000000
TEST(ShardPartitionPerfTest, Put400Million) {
  if (getenv("MANUAL_TEST") == nullptr) {
    GTEST_SKIP() << "Skip manual test case.";
  }

  uint64_t total = 400000000ULL;  // 4亿
  if (const char* s = getenv("PERF_PUT_COUNT")) total = std::stoull(s);

  auto mock_processor = std::make_shared<MockOperationProcessor>();
  PartitionCache cache(kFsId);
  PartitionPtr partition = cache.PutIf(ShardPartition::New(
      mock_processor,
      GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, 1)));

  // 每 compact_interval 次 Put 后通过 PutIf 更高版本的 partition 触发
  // Refresh，清空 delta_dentry_ops_（模拟线上 compact），否则 4 亿条
  // delta op 会 OOM（每条数百字节，共 100GB+）。
  const uint64_t compact_interval = 100000;  // 与 dentry_op_max_count 对齐
  const uint64_t report_interval = 10000000;

  auto start_us = utils::TimestampUs();
  uint64_t last_us = start_us;

  for (uint64_t i = 1; i <= total; ++i) {
    Dentry dentry(GenDentry(kFsId, kParentIno, 1000 + i,
                            fmt::format("file{:012d}", i),
                            pb::mds::FileType::FILE));
    partition->Put(dentry, i + 1);

    if (i % compact_interval == 0) {
      // 触发 Refresh 回收 delta ops
      partition = cache.PutIf(ShardPartition::New(
          mock_processor,
          GenInode(kFsId, kParentIno, pb::mds::FileType::DIRECTORY, i + 1)));
    }

    if (i % report_interval == 0) {
      auto now_us = utils::TimestampUs();
      double interval_qps =
          report_interval * 1e6 / static_cast<double>(now_us - last_us);
      double avg_qps = i * 1e6 / static_cast<double>(now_us - start_us);
      fmt::print(
          "progress: {}/{} ({:.1f}%), interval qps: {:.0f}, avg qps: {:.0f}, "
          "elapsed: {:.1f}s\n",
          i, total, i * 100.0 / total, interval_qps, avg_qps,
          (now_us - start_us) / 1e6);
      last_us = now_us;
    }
  }

  auto elapsed_us = utils::TimestampUs() - start_us;
  double qps = total * 1e6 / static_cast<double>(elapsed_us);
  fmt::print(
      "=== ShardPartition::Put perf: total({}) elapsed({:.1f}s) qps({:.0f}) "
      "avg latency({:.0f}ns)\n",
      total, elapsed_us / 1e6, qps, elapsed_us * 1000.0 / total);

  ASSERT_EQ(partition->CompleteVersion(), total + 1);
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
