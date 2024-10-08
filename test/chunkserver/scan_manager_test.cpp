/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 2021-05-28
 * Author: wanghai01
 */

#include "src/chunkserver/scan_manager.h"

#include <brpc/server.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <memory>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/datastore/chunkserver_chunkfile.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/common/configuration.h"
#include "src/fs/local_filesystem.h"
#include "test/chunkserver/datastore/mock_datastore.h"
#include "test/chunkserver/mock_copyset_node.h"
#include "test/chunkserver/mock_copyset_node_manager.h"

namespace curve {
namespace chunkserver {

using ::curve::fs::FileSystemType;
using curve::fs::LocalFsFactory;
using ::google::protobuf::util::MessageDifferencer;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Mock;
using ::testing::Return;
using ::testing::ReturnPointee;
using ::testing::ReturnRef;
using ::testing::SetArgPointee;

class ScanManagerTest : public ::testing::Test {
 protected:
  void SetUp() {
    copysetNodeManager_ = new MockCopysetNodeManager();
    defaultOptions_.intervalSec = 5;
    defaultOptions_.scanSize = 4194304;
    defaultOptions_.timeoutMs = 100;
    defaultOptions_.retry = 1;
    defaultOptions_.retryIntervalUs = 100000;
    defaultOptions_.copysetNodeManager = copysetNodeManager_;
    options.maxChunkSize = 16 * 1024 * 1024;
    EXPECT_CALL(*copysetNodeManager_, GetCopysetNodeOptions())
        .Times(1)
        .WillOnce(ReturnRef(options));
    scanManager_ = new ScanManager();
    ASSERT_EQ(0, scanManager_->Init(defaultOptions_));
    // Init CSChunkFile
    std::shared_ptr<LocalFileSystem> lfs =
        LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
    ChunkOptions options;
    options.baseDir = "/";
    csChunkFile_ = new CSChunkFile(lfs, nullptr, options);
    ChunkFileMetaPage metaPage;
    metaPage.version = 2;
    csChunkFile_->SetChunkFileMetaPage(metaPage);
  }

  void TearDown() {
    delete copysetNodeManager_;
    delete scanManager_;
  }

 protected:
  ScanManagerOptions defaultOptions_;
  ChunkFileMetaPage metaPage;
  MockCopysetNodeManager* copysetNodeManager_;
  CSChunkFile* csChunkFile_;
  std::shared_ptr<MockCopysetNode> copysetNode_;
  std::shared_ptr<MockDataStore> dataStore_;
  ScanManager* scanManager_;
  CopysetNodeOptions options;
};

TEST_F(ScanManagerTest, EnqueueTest) {
  ASSERT_EQ(0, scanManager_->GetWaitJobNum());
  scanManager_->Enqueue(1, 10000);
  scanManager_->Enqueue(2, 20000);
  ASSERT_EQ(2, scanManager_->GetWaitJobNum());
  scanManager_->Dequeue();
  ASSERT_EQ(1, scanManager_->GetWaitJobNum());
  scanManager_->Dequeue();
  ASSERT_EQ(0, scanManager_->GetWaitJobNum());
}

TEST_F(ScanManagerTest, ScanJobTest) {
  scanManager_->Enqueue(1, 10000);
  ASSERT_EQ(1, scanManager_->GetWaitJobNum());
  ChunkMap chunkMap;
  chunkMap.emplace(1, csChunkFile_);

  std::vector<ScanMap> failedMap;
  std::vector<Peer> peers;
  peers.push_back(Peer());
  peers.push_back(Peer());
  peers.push_back(Peer());
  dataStore_ = std::make_shared<MockDataStore>();
  copysetNode_ = std::make_shared<MockCopysetNode>();
  EXPECT_CALL(*copysetNodeManager_, GetCopysetNode(_, _))
      .Times(3)
      .WillRepeatedly(Return(copysetNode_));
  EXPECT_CALL(*copysetNode_, GetDataStore())
      .Times(6)
      .WillRepeatedly(Return(dataStore_));
  EXPECT_CALL(*copysetNode_, GetFailedScanMap())
      .Times(1)
      .WillOnce(ReturnRef(failedMap));
  EXPECT_CALL(*copysetNode_, SetScan(_)).Times(2);
  EXPECT_CALL(*copysetNode_, SetLastScan(_)).Times(1);
  EXPECT_CALL(*dataStore_, GetChunkMap()).Times(1).WillOnce(Return(chunkMap));
  EXPECT_CALL(*copysetNode_, ListPeers(_))
      .Times(1)
      .WillOnce(SetArgPointee<0>(peers));
  EXPECT_CALL(*copysetNode_, IsLeaderTerm())
      .Times(10)
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*copysetNode_, Propose(_))
      .Times(5)
      .WillRepeatedly(
          Invoke([](const braft::Task& task) { task.done->Run(); }));

  ASSERT_EQ(0, scanManager_->Run());
  std::this_thread::sleep_for(std::chrono::seconds(1));
  scanManager_->Fini();
}

TEST_F(ScanManagerTest, CompareMapSuccessTest) {
  // make key
  ScanKey key(1, 10000);

  // make chunkmap
  std::shared_ptr<LocalFileSystem> lfs =
      LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
  ChunkOptions options;
  options.baseDir = "/";
  CSChunkFile* v1Chunk = new CSChunkFile(lfs, nullptr, options);
  ChunkFileMetaPage metaPage1;
  metaPage1.version = 1;
  v1Chunk->SetChunkFileMetaPage(metaPage1);
  ChunkMap chunkMap;
  chunkMap.emplace(1, csChunkFile_);
  chunkMap.emplace(2, v1Chunk);

  // make scan job
  std::shared_ptr<ScanJob> job = std::make_shared<ScanJob>();

  job->poolId = 1;
  job->id = 10000;
  job->type = ScanType::NewMap;
  job->chunkMap = chunkMap;
  job->task.chunkId = 1;
  job->task.offset = 12582912;
  job->task.len = 4194304;
  ASSERT_EQ(3, job->task.waitingNum);
  ASSERT_EQ(0, scanManager_->GetJobNum());
  scanManager_->SetJob(key, job);
  ASSERT_EQ(1, scanManager_->GetJobNum());

  // GenScanJobs on WaitMap status
  ScanMap* localMap = new ScanMap();
  localMap->set_logicalpoolid(1);
  localMap->set_copysetid(10000);
  localMap->set_chunkid(1);
  localMap->set_index(1);
  localMap->set_crc(100);
  localMap->set_offset(12582912);
  localMap->set_len(4194304);
  // set local scanmap
  job->type = ScanType::WaitMap;
  scanManager_->SetLocalScanMap(key, *localMap);
  ASSERT_EQ(2, job->task.waitingNum);
  ASSERT_EQ(0, job->task.followerMap.size());
  // add first follower's scanmap
  FollowScanMapRequest request;
  FollowScanMapResponse response;
  request.set_allocated_scanmap(localMap);
  scanManager_->DealFollowerScanMap(request, &response);
  ASSERT_EQ(1, job->task.waitingNum);
  ASSERT_EQ(1, job->task.followerMap.size());
  // add second follower's scanmap
  scanManager_->DealFollowerScanMap(request, &response);
  // finish scan job
  copysetNode_ = std::make_shared<MockCopysetNode>();
  EXPECT_CALL(*copysetNodeManager_, GetCopysetNode(_, _))
      .Times(1)
      .WillOnce(Return(copysetNode_));
  EXPECT_CALL(*copysetNode_, SetScan(_)).Times(1);
  EXPECT_CALL(*copysetNode_, SetLastScan(_)).Times(1);
  job->type = ScanType::Finish;
  scanManager_->GenScanJobs(key);
  ASSERT_EQ(0, scanManager_->GetJobNum());
}

TEST_F(ScanManagerTest, CompareMapFailTest) {
  // make key
  ScanKey key(1, 10000);

  // make chunkmap
  ChunkMap chunkMap;
  chunkMap.emplace(1, csChunkFile_);

  // make scan job
  std::shared_ptr<ScanJob> job = std::make_shared<ScanJob>();
  job->poolId = 1;
  job->id = 10000;
  job->type = ScanType::NewMap;
  job->chunkMap = chunkMap;
  job->task.chunkId = 1;
  job->task.offset = 12582912;
  job->task.len = 4194304;
  ASSERT_EQ(3, job->task.waitingNum);
  ASSERT_EQ(0, scanManager_->GetJobNum());
  scanManager_->SetJob(key, job);
  ASSERT_EQ(1, scanManager_->GetJobNum());

  // GenScanJobs on WaitMap status
  ScanMap* localMap = new ScanMap();
  localMap->set_logicalpoolid(1);
  localMap->set_copysetid(10000);
  localMap->set_chunkid(1);
  localMap->set_index(1);
  localMap->set_crc(100);
  localMap->set_offset(12582912);
  localMap->set_len(4194304);
  ScanMap* scanMap = new ScanMap();
  scanMap->set_logicalpoolid(10);  // uncorrect poolid
  scanMap->set_copysetid(10000);
  scanMap->set_chunkid(1);
  scanMap->set_index(1);
  scanMap->set_crc(100);
  scanMap->set_offset(12582912);
  scanMap->set_len(4194304);
  ScanMap* scanMap1 = new ScanMap();
  scanMap1->set_logicalpoolid(1);
  scanMap1->set_copysetid(10000);
  scanMap1->set_chunkid(1);
  scanMap1->set_index(10);  // uncorrect index
  scanMap1->set_crc(100);
  scanMap1->set_offset(12582912);
  scanMap1->set_len(4194304);
  ScanMap* scanMap2 = new ScanMap();
  scanMap2->set_logicalpoolid(1);
  scanMap2->set_copysetid(10000);
  scanMap2->set_chunkid(1);
  scanMap2->set_index(1);
  scanMap2->set_crc(200);
  scanMap2->set_offset(0);  // mismatch offset
  scanMap2->set_len(4194304);
  // set local scanmap
  job->type = ScanType::WaitMap;
  scanManager_->SetLocalScanMap(key, *localMap);
  ASSERT_EQ(2, job->task.waitingNum);
  ASSERT_EQ(0, job->task.followerMap.size());

  // test uncorrect poolid
  FollowScanMapRequest request;
  FollowScanMapResponse response;
  request.set_allocated_scanmap(scanMap);
  scanManager_->DealFollowerScanMap(request, &response);
  ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
            response.retcode());

  // test uncorrect offset
  request.set_allocated_scanmap(scanMap2);
  scanManager_->DealFollowerScanMap(request, &response);
  ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
            response.retcode());

  // test uncorrect index
  request.set_allocated_scanmap(localMap);
  scanManager_->DealFollowerScanMap(request, &response);
  ASSERT_EQ(1, job->task.waitingNum);
  ASSERT_EQ(1, job->task.followerMap.size());
  request.set_allocated_scanmap(scanMap1);

  std::vector<ScanMap> failedMap;
  copysetNode_ = std::make_shared<MockCopysetNode>();
  EXPECT_CALL(*copysetNodeManager_, GetCopysetNode(_, _))
      .Times(1)
      .WillOnce(Return(copysetNode_));
  EXPECT_CALL(*copysetNode_, GetFailedScanMap())
      .Times(1)
      .WillOnce(ReturnRef(failedMap));
  scanManager_->DealFollowerScanMap(request, &response);
  ASSERT_EQ(1, failedMap.size());
  ASSERT_EQ(true, MessageDifferencer::Equals(job->task.localMap, failedMap[0]));
  // finish scan job
  EXPECT_CALL(*copysetNodeManager_, GetCopysetNode(_, _))
      .Times(1)
      .WillOnce(Return(copysetNode_));
  EXPECT_CALL(*copysetNode_, SetScan(_)).Times(1);
  EXPECT_CALL(*copysetNode_, SetLastScan(_)).Times(1);
  job->type = ScanType::Finish;
  scanManager_->GenScanJobs(key);
  ASSERT_EQ(0, scanManager_->GetJobNum());
}

TEST_F(ScanManagerTest, MismatchedCRCTest) {
  // make key
  ScanKey key(1, 10000);

  // make chunkmap
  ChunkMap chunkMap;
  chunkMap.emplace(1, csChunkFile_);

  // make scan job
  std::shared_ptr<ScanJob> job = std::make_shared<ScanJob>();
  job->poolId = 1;
  job->id = 10000;
  job->type = ScanType::NewMap;
  job->chunkMap = chunkMap;
  job->task.chunkId = 1;
  job->task.offset = 12582912;
  job->task.len = 4194304;
  ASSERT_EQ(3, job->task.waitingNum);
  ASSERT_EQ(0, scanManager_->GetJobNum());
  scanManager_->SetJob(key, job);
  ASSERT_EQ(1, scanManager_->GetJobNum());

  // GenScanJobs on WaitMap status
  ScanMap* localMap = new ScanMap();
  localMap->set_logicalpoolid(1);
  localMap->set_copysetid(10000);
  localMap->set_chunkid(1);
  localMap->set_index(1);
  localMap->set_crc(100);
  localMap->set_offset(12582912);
  localMap->set_len(4194304);
  ScanMap* scanMap = new ScanMap();
  scanMap->set_logicalpoolid(1);
  scanMap->set_copysetid(10000);
  scanMap->set_chunkid(1);
  scanMap->set_index(1);
  scanMap->set_crc(200);  // uncorrect crc
  scanMap->set_offset(12582912);
  scanMap->set_len(4194304);
  // set local scanmap
  job->type = ScanType::WaitMap;
  scanManager_->SetLocalScanMap(key, *localMap);
  ASSERT_EQ(2, job->task.waitingNum);
  ASSERT_EQ(0, job->task.followerMap.size());
  // add first follower's scanmap
  FollowScanMapRequest request;
  FollowScanMapResponse response;
  request.set_allocated_scanmap(localMap);
  scanManager_->DealFollowerScanMap(request, &response);
  ASSERT_EQ(1, job->task.waitingNum);
  ASSERT_EQ(1, job->task.followerMap.size());

  std::vector<ScanMap> failedMap;
  copysetNode_ = std::make_shared<MockCopysetNode>();
  EXPECT_CALL(*copysetNodeManager_, GetCopysetNode(_, _))
      .Times(1)
      .WillOnce(Return(copysetNode_));
  EXPECT_CALL(*copysetNode_, GetFailedScanMap())
      .Times(1)
      .WillOnce(ReturnRef(failedMap));
  request.set_allocated_scanmap(scanMap);
  scanManager_->DealFollowerScanMap(request, &response);
  ASSERT_EQ(1, failedMap.size());
  ASSERT_EQ(true, MessageDifferencer::Equals(job->task.localMap, failedMap[0]));
  // finish scan job
  EXPECT_CALL(*copysetNodeManager_, GetCopysetNode(_, _))
      .Times(1)
      .WillOnce(Return(copysetNode_));
  EXPECT_CALL(*copysetNode_, SetScan(_)).Times(1);
  EXPECT_CALL(*copysetNode_, SetLastScan(_)).Times(1);
  job->type = ScanType::Finish;
  scanManager_->GenScanJobs(key);
  ASSERT_EQ(0, scanManager_->GetJobNum());
}

TEST_F(ScanManagerTest, CancelScanJobTest) {
  // test cancle scan job not started
  scanManager_->Enqueue(2, 10000);
  ASSERT_EQ(1, scanManager_->GetWaitJobNum());
  scanManager_->CancelScanJob(2, 10000);
  ASSERT_EQ(0, scanManager_->GetWaitJobNum());

  // test cancel scan job started
  ScanKey key(1, 10000);

  std::shared_ptr<ScanJob> job = std::make_shared<ScanJob>();
  job->poolId = 1;
  job->id = 10000;
  job->type = ScanType::NewMap;
  ASSERT_EQ(0, scanManager_->GetJobNum());
  scanManager_->SetJob(key, job);
  ASSERT_EQ(1, scanManager_->GetJobNum());

  // test cancel scan job
  std::vector<ScanMap> failedMap;
  copysetNode_ = std::make_shared<MockCopysetNode>();
  EXPECT_CALL(*copysetNodeManager_, GetCopysetNode(_, _))
      .Times(1)
      .WillOnce(Return(copysetNode_));
  EXPECT_CALL(*copysetNode_, SetScan(_)).Times(1);
  EXPECT_CALL(*copysetNode_, GetFailedScanMap())
      .Times(1)
      .WillOnce(ReturnRef(failedMap));
  scanManager_->CancelScanJob(1, 10000);
  ASSERT_EQ(0, scanManager_->GetJobNum());
}

TEST_F(ScanManagerTest, CancelScanJobAfterTransferLeaderTest) {
  scanManager_->Enqueue(1, 10000);
  ASSERT_EQ(1, scanManager_->GetWaitJobNum());
  ChunkMap chunkMap;
  chunkMap.emplace(1, csChunkFile_);

  std::vector<ScanMap> failedMap;
  std::vector<Peer> peers;
  peers.push_back(Peer());
  peers.push_back(Peer());
  peers.push_back(Peer());
  dataStore_ = std::make_shared<MockDataStore>();
  copysetNode_ = std::make_shared<MockCopysetNode>();
  EXPECT_CALL(*copysetNodeManager_, GetCopysetNode(_, _))
      .Times(3)
      .WillRepeatedly(Return(copysetNode_));
  EXPECT_CALL(*copysetNode_, GetDataStore())
      .Times(2)
      .WillRepeatedly(Return(dataStore_));
  EXPECT_CALL(*copysetNode_, GetFailedScanMap())
      .Times(2)
      .WillRepeatedly(ReturnRef(failedMap));
  EXPECT_CALL(*copysetNode_, SetScan(_)).Times(2);
  EXPECT_CALL(*dataStore_, GetChunkMap()).Times(1).WillOnce(Return(chunkMap));
  EXPECT_CALL(*copysetNode_, ListPeers(_))
      .Times(1)
      .WillOnce(SetArgPointee<0>(peers));
  EXPECT_CALL(*copysetNode_, IsLeaderTerm())
      .Times(3)
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));
  EXPECT_CALL(*copysetNode_, Propose(_))
      .Times(1)
      .WillRepeatedly(
          Invoke([](const braft::Task& task) { task.done->Run(); }));
  ASSERT_EQ(0, scanManager_->Run());
  std::this_thread::sleep_for(std::chrono::seconds(1));
  scanManager_->Fini();
}

}  // namespace chunkserver
}  // namespace curve
