/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "client/vfs/metasystem/mds/dir_iterator.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "common/trace/trace_manager.h"
#include "dingofs/mds.pb.h"
#include "mds/filesystem/fs_info.h"
#include "test/unit/client/vfs/metasystem/mock/mock_mds_client.h"
#include "test/unit/client/vfs/test_common.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {
namespace test {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Eq;
using ::testing::Field;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgReferee;
using dingofs::client::vfs::test::MakeFileAttr;

class DirIteratorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mds::FsInfoEntry fs_info_entry;
    fs_info_ = std::make_unique<mds::FsInfo>(fs_info_entry);
    rpc_ = std::make_unique<meta::RPC>(butil::EndPoint());
    trace_manager_ = std::make_unique<TraceManager>();

    auto mds_client = std::make_unique<MockMDSClient>(
        ClientId(), *fs_info_, std::move(*rpc_), *trace_manager_);
    mock_mds_client_ = mds_client.get();
    mds_client_ = std::move(mds_client);

    ctx_ = std::make_shared<Context>("test");
  }

  static DirEntry MakeDirEntry(Ino ino, const std::string& name) {
    DirEntry entry;
    entry.ino = ino;
    entry.name = name;
    entry.attr = MakeFileAttr(ino);
    return entry;
  }

  static std::vector<DirEntry> MakeEntries(size_t count, uint64_t start_ino) {
    std::vector<DirEntry> entries;
    entries.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      entries.push_back(
          MakeDirEntry(start_ino + i, "file" + std::to_string(i)));
    }
    return entries;
  }

  std::unique_ptr<mds::FsInfo> fs_info_;
  std::unique_ptr<meta::RPC> rpc_;
  std::unique_ptr<TraceManager> trace_manager_;
  std::unique_ptr<MockMDSClient> mds_client_;
  MockMDSClient* mock_mds_client_{nullptr};
  ContextSPtr ctx_;
};

// --- 1. Empty directory returns NoData ---
TEST_F(DirIteratorTest, GetValue_EmptyDir_ReturnsNoData) {
  EXPECT_CALL(*mock_mds_client_, ReadDir(_, Eq(1), Eq(100), _, _, _, _))
      .WillOnce(DoAll(SetArgReferee<6>(std::vector<DirEntry>{}),
                      Return(Status::OK())));

  auto dir_iter = DirIterator::New(*mds_client_, 1, 100);
  DirEntry entry;
  Status s = dir_iter->GetValue(ctx_, 0, true, entry);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoData());
}

// --- 2. Sequential read within a single batch ---
TEST_F(DirIteratorTest, GetValue_SingleBatch_SequentialRead) {
  auto entries = MakeEntries(3, 10);

  EXPECT_CALL(*mock_mds_client_, ReadDir(_, Eq(1), Eq(100), "", _, _, _))
      .WillOnce(DoAll(SetArgReferee<6>(entries), Return(Status::OK())));

  auto dir_iter = DirIterator::New(*mds_client_, 1, 100);

  for (size_t i = 0; i < entries.size(); ++i) {
    DirEntry entry;
    Status s = dir_iter->GetValue(ctx_, i, true, entry);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(entry.ino, entries[i].ino);
    EXPECT_EQ(entry.name, entries[i].name);
  }
}

// --- 3. Read triggers multiple fetches ---
TEST_F(DirIteratorTest, GetValue_MultiBatch_TriggersMultipleFetches) {
  uint32_t batch_size = FLAGS_vfs_meta_read_dir_batch_size;
  auto first_batch = MakeEntries(batch_size, 1);
  auto second_batch = MakeEntries(batch_size, 1 + batch_size);

  EXPECT_CALL(*mock_mds_client_, ReadDir(_, Eq(1), Eq(100), "", _, true, _))
      .WillOnce(DoAll(SetArgReferee<6>(first_batch), Return(Status::OK())));
  EXPECT_CALL(*mock_mds_client_,
              ReadDir(_, Eq(1), Eq(100), first_batch.back().name, _, true, _))
      .WillOnce(DoAll(SetArgReferee<6>(second_batch), Return(Status::OK())));

  auto dir_iter = DirIterator::New(*mds_client_, 1, 100);

  // Read first element of second batch to trigger second fetch.
  DirEntry entry;
  Status s = dir_iter->GetValue(ctx_, batch_size, true, entry);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entry.ino, second_batch[0].ino);
}

// --- 4. with_attr flag is passed correctly ---
TEST_F(DirIteratorTest, GetValue_WithAttrFlag_PassedToReadDir) {
  auto entries = MakeEntries(2, 1);

  EXPECT_CALL(*mock_mds_client_, ReadDir(_, _, _, _, _, true, _))
      .WillOnce(DoAll(SetArgReferee<6>(entries), Return(Status::OK())));

  auto dir_iter = DirIterator::New(*mds_client_, 1, 100);
  DirEntry entry;
  Status s = dir_iter->GetValue(ctx_, 0, true, entry);
  EXPECT_TRUE(s.ok());
}

// --- 5. Backward seek uses memo and refetches ---
TEST_F(DirIteratorTest, GetValue_BackwardSeek_UsesMemoAndRefetches) {
  // Use a small batch size so offset_ advances after a full batch is consumed.
  auto prev_batch_size = FLAGS_vfs_meta_read_dir_batch_size;
  FLAGS_vfs_meta_read_dir_batch_size = 4;

  auto first_batch = MakeEntries(4, 1);   // full batch (triggers next fetch)
  auto second_batch = MakeEntries(1, 5);  // advances offset_ to 4
  auto refetch_batch = MakeEntries(2, 1); // backward seek from memo position 0

  {
    InSequence seq;
    EXPECT_CALL(*mock_mds_client_,
                ReadDir(_, Eq(1), Eq(100), std::string(""), _, _, _))
        .WillOnce(DoAll(SetArgReferee<6>(first_batch), Return(Status::OK())));
    EXPECT_CALL(*mock_mds_client_,
                ReadDir(_, Eq(1), Eq(100), first_batch.back().name, _, _, _))
        .WillOnce(DoAll(SetArgReferee<6>(second_batch), Return(Status::OK())));
    EXPECT_CALL(*mock_mds_client_,
                ReadDir(_, Eq(1), Eq(100), std::string(""), _, _, _))
        .WillOnce(DoAll(SetArgReferee<6>(refetch_batch), Return(Status::OK())));
  }

  auto dir_iter = DirIterator::New(*mds_client_, 1, 100);

  // Read offset 3 to populate first batch in cache (offset_ stays at 0).
  DirEntry entry;
  ASSERT_TRUE(dir_iter->GetValue(ctx_, 3, true, entry).ok());
  EXPECT_EQ(entry.ino, first_batch[3].ino);

  // Read offset 4 to trigger a second fetch, advancing offset_ to 4.
  ASSERT_TRUE(dir_iter->GetValue(ctx_, 4, true, entry).ok());
  EXPECT_EQ(entry.ino, second_batch[0].ino);

  // Seek backward to offset 1 — triggers SeekBackward which uses memo.
  Status s = dir_iter->GetValue(ctx_, 1, true, entry);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entry.ino, refetch_batch[1].ino);

  FLAGS_vfs_meta_read_dir_batch_size = prev_batch_size;
}

// --- 6. Fetch error is propagated ---
TEST_F(DirIteratorTest, GetValue_FetchError_Propagated) {
  EXPECT_CALL(*mock_mds_client_, ReadDir)
      .WillOnce(Return(Status::Internal("mock error")));

  auto dir_iter = DirIterator::New(*mds_client_, 1, 100);
  DirEntry entry;
  Status s = dir_iter->GetValue(ctx_, 0, true, entry);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInternal());
}

// --- 7. Reading past end of directory returns NoData ---
TEST_F(DirIteratorTest, GetValue_PastEnd_ReturnsNoData) {
  auto entries = MakeEntries(2, 1);

  // Only one ReadDir call: entries.size() < batch_size triggers NoData
  // short-circuit on the next access without a second fetch.
  EXPECT_CALL(*mock_mds_client_, ReadDir)
      .WillOnce(DoAll(SetArgReferee<6>(entries), Return(Status::OK())));

  auto dir_iter = DirIterator::New(*mds_client_, 1, 100);

  DirEntry entry;
  ASSERT_TRUE(dir_iter->GetValue(ctx_, 0, true, entry).ok());
  ASSERT_TRUE(dir_iter->GetValue(ctx_, 1, true, entry).ok());

  Status s = dir_iter->GetValue(ctx_, 2, true, entry);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNoData());
}

// --- 8. Size returns current cached entry count ---
TEST_F(DirIteratorTest, Size_ReturnsCachedEntryCount) {
  auto entries = MakeEntries(5, 1);

  EXPECT_CALL(*mock_mds_client_, ReadDir)
      .WillOnce(DoAll(SetArgReferee<6>(entries), Return(Status::OK())));

  auto dir_iter = DirIterator::New(*mds_client_, 1, 100);
  EXPECT_EQ(dir_iter->Size(), 0u);

  DirEntry entry;
  ASSERT_TRUE(dir_iter->GetValue(ctx_, 0, true, entry).ok());
  EXPECT_EQ(dir_iter->Size(), 5u);
}

// --- 9. Bytes returns estimated memory usage ---
TEST_F(DirIteratorTest, Bytes_ReturnsEstimatedMemoryUsage) {
  auto entries = MakeEntries(3, 1);

  EXPECT_CALL(*mock_mds_client_, ReadDir)
      .WillOnce(DoAll(SetArgReferee<6>(entries), Return(Status::OK())));

  auto dir_iter = DirIterator::New(*mds_client_, 1, 100);
  DirEntry entry;
  ASSERT_TRUE(dir_iter->GetValue(ctx_, 0, true, entry).ok());

  size_t expected = sizeof(DirIterator) + (3 * sizeof(DirEntry));
  EXPECT_EQ(dir_iter->Bytes(), expected);
}

// --- 10. Dump and Load round-trip ---
TEST_F(DirIteratorTest, DumpAndLoad_RoundTrip) {
  auto entries = MakeEntries(2, 1);

  EXPECT_CALL(*mock_mds_client_, ReadDir)
      .WillOnce(DoAll(SetArgReferee<6>(entries), Return(Status::OK())));

  auto dir_iter = DirIterator::New(*mds_client_, 1, 100);
  DirEntry entry;
  ASSERT_TRUE(dir_iter->GetValue(ctx_, 0, true, entry).ok());

  Json::Value dumped;
  ASSERT_TRUE(dir_iter->Dump(dumped));

  auto loaded = DirIterator::New(*mds_client_, 1, 100);
  ASSERT_TRUE(loaded->Load(dumped));

  EXPECT_EQ(loaded->INo(), dir_iter->INo());
  EXPECT_EQ(loaded->Fh(), dir_iter->Fh());
  EXPECT_EQ(loaded->Size(), dir_iter->Size());
}

// --- 11. Remember tracks requested offsets ---
TEST_F(DirIteratorTest, Remember_TracksOffsets) {
  auto dir_iter = DirIterator::New(*mds_client_, 1, 100);
  dir_iter->Remember(7);
  dir_iter->Remember(3);

  // Destroying the iterator logs stats; we just verify it doesn't crash.
  // No public API to read stats, so this is a smoke test.
  dir_iter.reset();
}

// --- 12. LastFetchTimeNs is set after construction ---
TEST_F(DirIteratorTest, LastFetchTimeNs_SetAfterConstruction) {
  auto dir_iter = DirIterator::New(*mds_client_, 1, 100);
  EXPECT_GT(dir_iter->LastFetchTimeNs(), 0u);
}

// ==================== DirIteratorManager tests ====================

class DirIteratorManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mds::FsInfoEntry fs_info_entry;
    fs_info_ = std::make_unique<mds::FsInfo>(fs_info_entry);
    rpc_ = std::make_unique<meta::RPC>(butil::EndPoint());
    trace_manager_ = std::make_unique<TraceManager>();

    auto mds_client = std::make_unique<MockMDSClient>(
        ClientId(), *fs_info_, std::move(*rpc_), *trace_manager_);
    mds_client_ = std::move(mds_client);
  }

  static DirIteratorSPtr MakeDirIterator(MDSClient& client, Ino ino,
                                         uint64_t fh) {
    return DirIterator::New(client, ino, fh);
  }

  std::unique_ptr<mds::FsInfo> fs_info_;
  std::unique_ptr<meta::RPC> rpc_;
  std::unique_ptr<TraceManager> trace_manager_;
  std::unique_ptr<MockMDSClient> mds_client_;
};

// --- 13. Put, Get, Delete lifecycle ---
TEST_F(DirIteratorManagerTest, PutGetDelete_BasicLifecycle) {
  DirIteratorManager manager;
  auto iter = MakeDirIterator(*mds_client_, 1, 100);

  manager.PutWithFunc(1, 100, iter,
                      [](const DirIteratorManager::DirIteratorSet&) {});

  auto got = manager.Get(1, 100);
  EXPECT_EQ(got, iter);

  manager.Delete(1, 100);
  EXPECT_EQ(manager.Get(1, 100), nullptr);
}

// --- 14. Get returns nullptr for missing entries ---
TEST_F(DirIteratorManagerTest, Get_Missing_ReturnsNull) {
  DirIteratorManager manager;
  EXPECT_EQ(manager.Get(1, 100), nullptr);
}

// --- 15. Size counts all iterators ---
TEST_F(DirIteratorManagerTest, Size_CountsAllIterators) {
  DirIteratorManager manager;
  auto iter1 = MakeDirIterator(*mds_client_, 1, 100);
  auto iter2 = MakeDirIterator(*mds_client_, 1, 101);

  manager.PutWithFunc(1, 100, iter1,
                      [](const DirIteratorManager::DirIteratorSet&) {});
  manager.PutWithFunc(1, 101, iter2,
                      [](const DirIteratorManager::DirIteratorSet&) {});

  EXPECT_EQ(manager.Size(), 2u);
}

// --- 16. Bytes aggregates iterator memory ---
TEST_F(DirIteratorManagerTest, Bytes_AggregatesMemory) {
  DirIteratorManager manager;
  auto iter = MakeDirIterator(*mds_client_, 1, 100);

  manager.PutWithFunc(1, 100, iter,
                      [](const DirIteratorManager::DirIteratorSet&) {});

  EXPECT_EQ(manager.Bytes(), iter->Bytes());
}

// --- 17. Summary returns JSON ---
TEST_F(DirIteratorManagerTest, Summary_ReturnsJson) {
  DirIteratorManager manager;
  auto iter = MakeDirIterator(*mds_client_, 1, 100);

  manager.PutWithFunc(1, 100, iter,
                      [](const DirIteratorManager::DirIteratorSet&) {});

  Json::Value value;
  manager.Summary(value);
  EXPECT_EQ(value["name"].asString(), "diriterator");
  EXPECT_EQ(value["count"].asUInt(), 1u);
}

// --- 18. Dump and Load round-trip ---
TEST_F(DirIteratorManagerTest, DumpAndLoad_RoundTrip) {
  DirIteratorManager manager;
  auto iter = MakeDirIterator(*mds_client_, 1, 100);

  manager.PutWithFunc(1, 100, iter,
                      [](const DirIteratorManager::DirIteratorSet&) {});

  Json::Value dumped;
  ASSERT_TRUE(manager.Dump(dumped));

  DirIteratorManager loaded_manager;
  ASSERT_TRUE(loaded_manager.Load(*mds_client_, dumped));

  EXPECT_EQ(loaded_manager.Size(), manager.Size());
  EXPECT_EQ(loaded_manager.Get(1, 100)->INo(), 1u);
  EXPECT_EQ(loaded_manager.Get(1, 100)->Fh(), 100u);
}

}  // namespace test
}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
