
/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Project: dingo
 * Created Date: 2021-12-28
 * Author: xuchaojie
 */

#include <gmock/gmock-more-actions.h>
#include <gmock/gmock.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include <condition_variable>
#include <mutex>
#include <thread>

#include "dingofs/metaserver.pb.h"
#include "client/vfs_old/inode_wrapper.h"
#include "stub/rpcclient/metaserver_client.h"
#include "stub/rpcclient/task_excutor.h"
#include "utils/timeutility.h"
#include "client/vfs_old/mock_metaserver_client.h"
#include "client/vfs_old/utils.h"

namespace dingofs {
namespace client {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

using ::google::protobuf::util::MessageDifferencer;

using dingofs::stub::rpcclient::DataIndices;
using dingofs::stub::rpcclient::MetaServerClientDone;
using dingofs::stub::rpcclient::MockMetaServerClient;

class TestInodeWrapper : public ::testing::Test {
 protected:
  void SetUp() override {
    metaClient_ = std::make_shared<MockMetaServerClient>();
    inodeWrapper_ = std::make_shared<InodeWrapper>(Inode(), metaClient_);
  }

  void TearDown() override {
    metaClient_ = nullptr;
    inodeWrapper_ = nullptr;
  }

 protected:
  std::shared_ptr<InodeWrapper> inodeWrapper_;
  std::shared_ptr<MockMetaServerClient> metaClient_;
};

TEST(TestAppendS3ChunkInfoToMap, testAppendS3ChunkInfoToMap) {
  google::protobuf::Map<uint64_t, pb::metaserver::S3ChunkInfoList>
      s3ChunkInfoMap;
  pb::metaserver::S3ChunkInfo info1;
  info1.set_chunkid(1);
  info1.set_compaction(2);
  info1.set_offset(0);
  info1.set_len(1024);
  info1.set_size(65536);
  info1.set_zero(true);
  uint64_t chunkIndex1 = 1;
  AppendS3ChunkInfoToMap(chunkIndex1, info1, &s3ChunkInfoMap);
  ASSERT_EQ(1, s3ChunkInfoMap.size());
  ASSERT_EQ(1, s3ChunkInfoMap[chunkIndex1].s3chunks_size());
  ASSERT_TRUE(MessageDifferencer::Equals(
      info1, s3ChunkInfoMap[chunkIndex1].s3chunks(0)));

  // add to same chunkIndex
  pb::metaserver::S3ChunkInfo info2;
  info2.set_chunkid(2);
  info2.set_compaction(3);
  info2.set_offset(1024);
  info2.set_len(1024);
  info2.set_size(65536);
  info2.set_zero(false);
  AppendS3ChunkInfoToMap(chunkIndex1, info2, &s3ChunkInfoMap);
  ASSERT_EQ(1, s3ChunkInfoMap.size());
  ASSERT_EQ(2, s3ChunkInfoMap[chunkIndex1].s3chunks_size());
  ASSERT_TRUE(MessageDifferencer::Equals(
      info1, s3ChunkInfoMap[chunkIndex1].s3chunks(0)));
  ASSERT_TRUE(MessageDifferencer::Equals(
      info2, s3ChunkInfoMap[chunkIndex1].s3chunks(1)));

  // add to diff chunkIndex
  pb::metaserver::S3ChunkInfo info3;
  info3.set_chunkid(3);
  info3.set_compaction(4);
  info3.set_offset(2048);
  info3.set_len(1024);
  info3.set_size(65536);
  info3.set_zero(false);
  uint64_t chunkIndex2 = 2;
  AppendS3ChunkInfoToMap(chunkIndex2, info3, &s3ChunkInfoMap);
  ASSERT_EQ(2, s3ChunkInfoMap.size());
  ASSERT_EQ(2, s3ChunkInfoMap[chunkIndex1].s3chunks_size());
  ASSERT_TRUE(MessageDifferencer::Equals(
      info1, s3ChunkInfoMap[chunkIndex1].s3chunks(0)));
  ASSERT_TRUE(MessageDifferencer::Equals(
      info2, s3ChunkInfoMap[chunkIndex1].s3chunks(1)));

  ASSERT_EQ(1, s3ChunkInfoMap[chunkIndex2].s3chunks_size());
  ASSERT_TRUE(MessageDifferencer::Equals(
      info3, s3ChunkInfoMap[chunkIndex2].s3chunks(0)));
}

TEST_F(TestInodeWrapper, testSyncSuccess) {
  inodeWrapper_->MarkDirty();
  inodeWrapper_->SetLength(1024);
  inodeWrapper_->SetType(pb::metaserver::FsFileType::TYPE_S3);

  pb::metaserver::S3ChunkInfo info1;
  info1.set_chunkid(1);
  info1.set_compaction(2);
  info1.set_offset(0);
  info1.set_len(1024);
  info1.set_size(65536);
  info1.set_zero(true);
  uint64_t chunkIndex1 = 1;
  inodeWrapper_->AppendS3ChunkInfo(chunkIndex1, info1);

  EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
      .WillOnce(Return(pb::metaserver::MetaStatusCode::OK));

  DINGOFS_ERROR ret = inodeWrapper_->Sync();
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);
}

TEST_F(TestInodeWrapper, testSyncFailed) {
  inodeWrapper_->MarkDirty();
  inodeWrapper_->SetLength(1024);
  inodeWrapper_->SetType(pb::metaserver::FsFileType::TYPE_S3);

  pb::metaserver::S3ChunkInfo info1;
  info1.set_chunkid(1);
  info1.set_compaction(2);
  info1.set_offset(0);
  info1.set_len(1024);
  info1.set_size(65536);
  info1.set_zero(true);
  uint64_t chunkIndex1 = 1;
  inodeWrapper_->AppendS3ChunkInfo(chunkIndex1, info1);

  EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
      .WillOnce(Return(pb::metaserver::MetaStatusCode::NOT_FOUND));

  DINGOFS_ERROR ret = inodeWrapper_->Sync();
  ASSERT_EQ(DINGOFS_ERROR::NOTEXIST, ret);
}

TEST_F(TestInodeWrapper, TestRefreshNlink) {
  google::protobuf::uint32 nlink = 10086;
  pb::metaserver::InodeAttr attr;
  attr.set_nlink(nlink);
  EXPECT_CALL(*metaClient_, GetInodeAttr(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(attr),
                      Return(pb::metaserver::MetaStatusCode::OK)));
  inodeWrapper_->RefreshNlink();
  Inode inode = inodeWrapper_->GetInode();
  ASSERT_EQ(nlink, inode.nlink());
}

TEST_F(TestInodeWrapper, TestNeedRefreshData) {
  Inode inode;
  inode.set_inodeid(1);
  auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
  pb::metaserver::S3ChunkInfoList* s3ChunkInfoList =
      new pb::metaserver::S3ChunkInfoList();
  pb::metaserver::S3ChunkInfo* s3ChunkInfo = s3ChunkInfoList->add_s3chunks();
  s3ChunkInfo->set_chunkid(1);
  s3ChunkInfo->set_compaction(1);
  s3ChunkInfo->set_offset(0);
  s3ChunkInfo->set_len(1024);
  s3ChunkInfo->set_size(65536);
  s3ChunkInfo->set_zero(true);
  s3ChunkInfoMap->insert({1, *s3ChunkInfoList});

  auto inodeWrapper =
      std::make_shared<InodeWrapper>(inode, metaClient_, nullptr, 1, 0);

  ASSERT_TRUE(inodeWrapper->NeedRefreshData());
}

namespace {

struct FakeCallback : public MetaServerClientDone {
  void Run() override {
    {
      std::lock_guard<std::mutex> lock(mtx);
      runned = true;
    }
    cond.notify_one();
  }

  void Wait() {
    std::unique_lock<std::mutex> lock(mtx);
    cond.wait(lock, [this]() { return runned; });
  }

  std::mutex mtx;
  std::condition_variable cond;
  bool runned{false};
};

struct FakeUpdateInodeWithOutNlinkAsync {
  void operator()(uint32_t /*fsid*/, uint64_t /*inodeId*/,
                  const pb::metaserver::InodeAttr& attr,
                  MetaServerClientDone* done, DataIndices indices) const {
    (void)attr;
    (void)indices;
    std::thread th{[done]() {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      done->SetMetaStatusCode(pb::metaserver::MetaStatusCode::OK);
      done->Run();
    }};

    th.detach();
  }
};

}  // namespace

TEST_F(TestInodeWrapper, TestAsyncInode) {
  for (auto type : {pb::metaserver::FsFileType::TYPE_DIRECTORY,
                    pb::metaserver::FsFileType::TYPE_FILE,
                    pb::metaserver::FsFileType::TYPE_S3,
                    pb::metaserver::FsFileType::TYPE_SYM_LINK}) {
    for (auto dirty : {true, false}) {
      inodeWrapper_->SetType(type);
      if (!dirty) {
        inodeWrapper_->ClearDirty();
      }

      EXPECT_CALL(*metaClient_, UpdateInodeWithOutNlinkAsync_rvr(_, _, _, _, _))
          .Times(dirty ? 1 : 0)
          .WillRepeatedly(Invoke(FakeUpdateInodeWithOutNlinkAsync{}));

      FakeCallback done;
      inodeWrapper_->Async(&done);
      done.Wait();
      ASSERT_EQ(pb::metaserver::MetaStatusCode::OK, done.GetStatusCode());
    }
  }
}

TEST_F(TestInodeWrapper, TestUpdateInodeAttrIncrementally) {
  Inode inode;
  inode.set_type(pb::metaserver::FsFileType::TYPE_S3);
  inode.set_length(0);
  inode.set_atime(0);
  inode.set_atime_ns(0);
  inode.set_ctime(0);
  inode.set_ctime_ns(0);
  inode.set_mtime(0);
  inode.set_mtime_ns(0);

  InodeWrapper wrapper(std::move(inode), metaClient_);

  {
    auto lock = wrapper.GetUniqueLock();
    wrapper.UpdateTimestampLocked(kAccessTime);
  }

  EXPECT_CALL(*metaClient_, UpdateInodeAttrWithOutNlink(_, _, _, _, _))
      .WillOnce(Invoke(
          [](uint32_t /*fsId*/, uint64_t /*inodeId*/, const InodeAttr& attr,
             stub::rpcclient::S3ChunkInfoMap* /*s3info*/, bool /*internal*/
          ) {
            EXPECT_FALSE(attr.has_length());
            return pb::metaserver::MetaStatusCode::OK;
          }));

  ASSERT_EQ(DINGOFS_ERROR::OK, wrapper.Sync());

  ASSERT_FALSE(wrapper.dirty_);
  ASSERT_FALSE(wrapper.dirtyAttr_.has_atime());
  ASSERT_FALSE(wrapper.dirtyAttr_.has_atime_ns());
}

TEST_F(TestInodeWrapper, TestSetXattr) {
  inodeWrapper_->SetXattrLocked("name", "value");
  pb::metaserver::XAttr xattr = inodeWrapper_->GetXattr();
  ASSERT_TRUE(xattr.xattrinfos().find("name") != xattr.xattrinfos().end());
  ASSERT_EQ((*xattr.mutable_xattrinfos())["name"], "value");
  ASSERT_TRUE(inodeWrapper_->IsDirty());
}

}  // namespace client
}  // namespace dingofs
