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
 * Project: dingo
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>

#include "client/vfs_legacy/filesystem/defer_sync.h"
#include "client/vfs_legacy/filesystem/dir_cache.h"
#include "client/vfs_legacy/filesystem/openfile.h"
#include "client/vfs_legacy/inode_cache_manager.h"
#include "client/vfs_legacy/inode_wrapper.h"
#include "client/vfs_legacy/mock_metaserver_client.h"
#include "dingofs/metaserver.pb.h"
#include "metaserver/mock_metaserver_s3_adaptor.h"
#include "options/client/vfs_legacy/vfs_legacy_dynamic_config.h"
#include "options/client/vfs_legacy/vfs_legacy_option.h"
#include "stub/filesystem/xattr.h"
#include "stub/rpcclient/metaserver_client.h"

namespace dingofs {
namespace client {

using ::testing::_;
using ::testing::AnyOf;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

using ::dingofs::client::filesystem::DeferSync;
using ::dingofs::client::filesystem::OpenFiles;

using ::dingofs::stub::filesystem::XATTR_DIR_ENTRIES;
using ::dingofs::stub::filesystem::XATTR_DIR_FBYTES;
using ::dingofs::stub::filesystem::XATTR_DIR_FILES;
using ::dingofs::stub::filesystem::XATTR_DIR_SUBDIRS;
using dingofs::stub::rpcclient::InodeParam;
using dingofs::stub::rpcclient::MockMetaServerClient;

using dingofs::pb::metaserver::FsFileType;
using dingofs::pb::metaserver::Inode;
using dingofs::pb::metaserver::InodeAttr;
using dingofs::pb::metaserver::MetaStatusCode;
using dingofs::pb::metaserver::S3ChunkInfo;
using dingofs::pb::metaserver::S3ChunkInfoList;
using dingofs::pb::metaserver::XAttr;

class TestInodeCacheManager : public ::testing::Test {
 protected:
  TestInodeCacheManager() {}
  ~TestInodeCacheManager() {}

  virtual void SetUp() {
    dingofs::client::FLAGS_enableCto = false;
    metaClient_ = std::make_shared<MockMetaServerClient>();
    iCacheManager_ = std::make_shared<InodeCacheManagerImpl>(metaClient_);
    iCacheManager_->SetFsId(fsId_);
    RefreshDataOption option;
    option.maxDataSize = 1;
    option.refreshDataIntervalSec = 0;
    auto deferSync = std::make_shared<DeferSync>(DeferSyncOption());
    auto openFiles = std::make_shared<OpenFiles>(OpenFilesOption(), deferSync);
    iCacheManager_->Init(option, openFiles, deferSync);
  }

  virtual void TearDown() {
    metaClient_ = nullptr;
    iCacheManager_ = nullptr;
  }

 protected:
  std::shared_ptr<InodeCacheManagerImpl> iCacheManager_;
  std::shared_ptr<MockMetaServerClient> metaClient_;
  uint32_t fsId_ = 888;
  uint32_t timeout_ = 3;
};

TEST_F(TestInodeCacheManager, GetInode) {
  uint64_t inodeId = 100;
  uint64_t fileLength = 100;

  Inode inode;
  inode.set_inodeid(inodeId);
  inode.set_fsid(fsId_);
  inode.set_length(fileLength);
  inode.set_type(pb::metaserver::FsFileType::TYPE_S3);
  auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
  S3ChunkInfoList* s3ChunkInfoList = new S3ChunkInfoList();
  S3ChunkInfo* s3ChunkInfo = s3ChunkInfoList->add_s3chunks();
  s3ChunkInfo->set_chunkid(1);
  s3ChunkInfo->set_compaction(1);
  s3ChunkInfo->set_offset(0);
  s3ChunkInfo->set_len(1024);
  s3ChunkInfo->set_size(65536);
  s3ChunkInfo->set_zero(true);
  s3ChunkInfoMap->insert({1, *s3ChunkInfoList});

  // miss cache and get inode failed
  EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId, _, _))
      .WillOnce(Return(MetaStatusCode::NOT_FOUND));
  std::shared_ptr<InodeWrapper> inodeWrapper;
  DINGOFS_ERROR ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
  ASSERT_EQ(DINGOFS_ERROR::NOTEXIST, ret);

  // miss cache and get inode ok, do not need streaming
  EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(inode), SetArgPointee<3>(false),
                      Return(MetaStatusCode::OK)));
  ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);

  // miss cache and get inode ok, need streaming
  uint64_t inodeId2 = 200;
  Inode inode2 = inode;
  inode2.set_inodeid(inodeId2);
  EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId2, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(inode2), SetArgPointee<3>(true),
                      Return(MetaStatusCode::OK)));
  EXPECT_CALL(*metaClient_,
              GetOrModifyS3ChunkInfo(fsId_, inodeId2, _, true, _, _))
      .WillOnce(Return(MetaStatusCode::OK));
  ASSERT_EQ(DINGOFS_ERROR::OK,
            iCacheManager_->GetInode(inodeId2, inodeWrapper));

  // hit cache and need refresh s3info
  // FIXME (Wine93)
  /*
  EXPECT_CALL(*metaClient_,
      GetOrModifyS3ChunkInfo(fsId_, inodeId, _, true, _, _))
      .WillOnce(Return(MetaStatusCode::OK));
  ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);

  Inode out = inodeWrapper->GetInode();
  ASSERT_EQ(inodeId, out.inodeid());
  ASSERT_EQ(fsId_, out.fsid());
  ASSERT_EQ(fileLength, out.length());

  ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);

  out = inodeWrapper->GetInode();
  ASSERT_EQ(inodeId, out.inodeid());
  ASSERT_EQ(fsId_, out.fsid());
  ASSERT_EQ(fileLength, out.length());
  */
}

TEST_F(TestInodeCacheManager, GetInodeAttr) {
  uint64_t inodeId = 100;
  uint64_t parentId = 99;
  uint64_t fileLength = 100;

  std::list<InodeAttr> attrs;
  InodeAttr attr;
  attr.set_inodeid(inodeId);
  attr.set_fsid(fsId_);
  attr.set_length(fileLength);
  attr.add_parent(parentId);
  attr.set_type(FsFileType::TYPE_FILE);
  attrs.emplace_back(attr);

  // 1. get from metaserver
  InodeAttr out;
  EXPECT_CALL(*metaClient_, BatchGetInodeAttr(fsId_, _, _))
      .WillOnce(Return(MetaStatusCode::NOT_FOUND))
      .WillOnce(DoAll(SetArgPointee<2>(attrs), Return(MetaStatusCode::OK)));

  DINGOFS_ERROR ret = iCacheManager_->GetInodeAttr(inodeId, &out);
  ASSERT_EQ(DINGOFS_ERROR::NOTEXIST, ret);

  ret = iCacheManager_->GetInodeAttr(inodeId, &out);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);
  ASSERT_EQ(inodeId, out.inodeid());
  ASSERT_EQ(fsId_, out.fsid());
  ASSERT_EQ(fileLength, out.length());

  // 2. create inode and get attr from icache
  InodeParam param;
  param.fsId = fsId_;
  param.type = FsFileType::TYPE_FILE;
  Inode inode;
  inode.set_inodeid(inodeId + 1);
  inode.set_fsid(fsId_ + 1);
  inode.set_type(FsFileType::TYPE_FILE);
  std::shared_ptr<InodeWrapper> inodeWrapper;
  EXPECT_CALL(*metaClient_, CreateInode(_, _))
      .WillOnce(DoAll(SetArgPointee<1>(inode), Return(MetaStatusCode::OK)));
  ret = iCacheManager_->CreateInode(param, inodeWrapper);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);

  // ret = iCacheManager_->GetInodeAttr(inodeId + 1, &out);
  // ASSERT_EQ(DINGOFS_ERROR::OK, ret);
  // ASSERT_EQ(inodeId + 1, out.inodeid());
  // ASSERT_EQ(fsId_ + 1, out.fsid());
  // ASSERT_EQ(FsFileType::TYPE_FILE, out.type());
}

TEST_F(TestInodeCacheManager, CreateAndGetInode) {
  dingofs::client::FLAGS_enableCto = false;
  uint64_t inodeId = 100;

  InodeParam param;
  param.fsId = fsId_;
  param.type = FsFileType::TYPE_FILE;

  Inode inode;
  inode.set_inodeid(inodeId);
  inode.set_fsid(fsId_);
  inode.set_type(FsFileType::TYPE_FILE);
  EXPECT_CALL(*metaClient_, CreateInode(_, _))
      .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR))
      .WillOnce(DoAll(SetArgPointee<1>(inode), Return(MetaStatusCode::OK)));

  std::shared_ptr<InodeWrapper> inodeWrapper;
  DINGOFS_ERROR ret = iCacheManager_->CreateInode(param, inodeWrapper);
  ASSERT_EQ(DINGOFS_ERROR::UNKNOWN, ret);

  ret = iCacheManager_->CreateInode(param, inodeWrapper);
  Inode out = inodeWrapper->GetInode();
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);
  ASSERT_EQ(inodeId, out.inodeid());
  ASSERT_EQ(fsId_, out.fsid());
  ASSERT_EQ(FsFileType::TYPE_FILE, out.type());

  /* FIXME (Wine93)
  ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);

  out = inodeWrapper->GetInode();
  ASSERT_EQ(inodeId, out.inodeid());
  ASSERT_EQ(fsId_, out.fsid());
  ASSERT_EQ(FsFileType::TYPE_FILE, out.type());
  */
}

TEST_F(TestInodeCacheManager, DeleteInode) {
  uint64_t inodeId = 100;

  EXPECT_CALL(*metaClient_, DeleteInode(fsId_, inodeId))
      .WillOnce(Return(MetaStatusCode::NOT_FOUND))
      .WillOnce(Return(MetaStatusCode::OK));

  DINGOFS_ERROR ret = iCacheManager_->DeleteInode(inodeId);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);

  ret = iCacheManager_->DeleteInode(inodeId);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);
}

TEST_F(TestInodeCacheManager, ShipToFlush) {
  uint64_t inodeId = 100;
  Inode inode;
  inode.set_inodeid(inodeId);
  inode.set_fsid(fsId_);
  inode.set_type(FsFileType::TYPE_S3);

  std::shared_ptr<InodeWrapper> inodeWrapper =
      std::make_shared<InodeWrapper>(inode, metaClient_);
  inodeWrapper->MarkDirty();
  S3ChunkInfo info;
  inodeWrapper->AppendS3ChunkInfo(1, info);

  iCacheManager_->ShipToFlush(inodeWrapper);
}

TEST_F(TestInodeCacheManager, BatchGetInodeAttr) {
  uint64_t inodeId1 = 100;
  uint64_t inodeId2 = 200;
  uint64_t fileLength = 100;

  // in
  std::set<uint64_t> inodeIds;
  inodeIds.emplace(inodeId1);
  inodeIds.emplace(inodeId2);

  // out
  std::list<InodeAttr> attrs;
  InodeAttr attr;
  attr.set_inodeid(inodeId1);
  attr.set_fsid(fsId_);
  attr.set_length(fileLength);
  attrs.emplace_back(attr);
  attr.set_inodeid(inodeId2);
  attrs.emplace_back(attr);

  EXPECT_CALL(*metaClient_, BatchGetInodeAttr(fsId_, inodeIds, _))
      .WillOnce(Return(MetaStatusCode::NOT_FOUND))
      .WillOnce(DoAll(SetArgPointee<2>(attrs), Return(MetaStatusCode::OK)));

  std::list<InodeAttr> getAttrs;
  DINGOFS_ERROR ret = iCacheManager_->BatchGetInodeAttr(&inodeIds, &getAttrs);
  ASSERT_EQ(DINGOFS_ERROR::NOTEXIST, ret);

  ret = iCacheManager_->BatchGetInodeAttr(&inodeIds, &getAttrs);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);
  ASSERT_EQ(getAttrs.size(), 2);
  ASSERT_THAT(getAttrs.begin()->inodeid(), AnyOf(inodeId1, inodeId2));
  ASSERT_EQ(getAttrs.begin()->fsid(), fsId_);
  ASSERT_EQ(getAttrs.begin()->length(), fileLength);
}

TEST_F(TestInodeCacheManager, BatchGetInodeAttrAsync) {
  uint64_t parentId = 1;
  uint64_t inodeId1 = 100;
  uint64_t inodeId2 = 200;

  // in
  std::set<uint64_t> inodeIds;
  inodeIds.emplace(inodeId1);
  inodeIds.emplace(inodeId2);

  // out
  Inode inode;
  inode.set_inodeid(inodeId1);

  std::vector<std::vector<uint64_t>> inodeGroups;
  inodeGroups.emplace_back(std::vector<uint64_t>{inodeId2});

  std::map<uint64_t, InodeAttr> attrs;

  google::protobuf::RepeatedPtrField<InodeAttr> inodeAttrs;
  inodeAttrs.Add()->set_inodeid(inodeId2);

  // fill icache
  EXPECT_CALL(*metaClient_, GetInode(_, inodeId1, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(inode), Return(MetaStatusCode::OK)));
  std::shared_ptr<InodeWrapper> wrapper;
  iCacheManager_->GetInode(inodeId1, wrapper);

  /*  FIXME (Wine93)
  EXPECT_CALL(*metaClient_, SplitRequestInodes(_, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(inodeGroups),
              Return(true)));
  EXPECT_CALL(*metaClient_, BatchGetInodeAttrAsync(_, _, _))
      .WillOnce(Invoke([inodeAttrs](uint32_t fsId,
          const std::vector<uint64_t> &inodeIds,
          MetaServerClientDone *done) {
              done->SetMetaStatusCode(MetaStatusCode::OK);
              static_cast<BatchGetInodeAttrDone *>(done)
                  ->SetInodeAttrs(inodeAttrs);
              done->Run();
              return MetaStatusCode::OK;
          }));

  DINGOFS_ERROR ret = iCacheManager_->BatchGetInodeAttrAsync(parentId,
      &inodeIds, &attrs);

  ASSERT_EQ(DINGOFS_ERROR::OK, ret);
  ASSERT_EQ(attrs.size(), 2);
  ASSERT_TRUE(attrs.find(inodeId1) != attrs.end());
  ASSERT_TRUE(attrs.find(inodeId2) != attrs.end());
  */
}

TEST_F(TestInodeCacheManager, BatchGetXAttr) {
  uint64_t inodeId1 = 100;
  uint64_t inodeId2 = 200;

  // in
  std::set<uint64_t> inodeIds;
  inodeIds.emplace(inodeId1);
  inodeIds.emplace(inodeId2);

  // out
  std::list<XAttr> xattrs;
  XAttr xattr;
  xattr.set_fsid(fsId_);
  xattr.set_inodeid(inodeId1);
  xattr.mutable_xattrinfos()->insert({XATTR_DIR_FILES, "1"});
  xattr.mutable_xattrinfos()->insert({XATTR_DIR_SUBDIRS, "1"});
  xattr.mutable_xattrinfos()->insert({XATTR_DIR_ENTRIES, "2"});
  xattr.mutable_xattrinfos()->insert({XATTR_DIR_FBYTES, "100"});
  xattrs.emplace_back(xattr);
  xattr.set_inodeid(inodeId2);
  xattr.mutable_xattrinfos()->find(XATTR_DIR_FBYTES)->second = "200";
  xattrs.emplace_back(xattr);

  EXPECT_CALL(*metaClient_, BatchGetXAttr(fsId_, inodeIds, _))
      .WillOnce(Return(MetaStatusCode::NOT_FOUND))
      .WillOnce(DoAll(SetArgPointee<2>(xattrs), Return(MetaStatusCode::OK)));

  std::list<XAttr> getXAttrs;
  DINGOFS_ERROR ret = iCacheManager_->BatchGetXAttr(&inodeIds, &getXAttrs);
  ASSERT_EQ(DINGOFS_ERROR::NOTEXIST, ret);

  ret = iCacheManager_->BatchGetXAttr(&inodeIds, &getXAttrs);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);
  ASSERT_EQ(getXAttrs.size(), 2);
  ASSERT_THAT(getXAttrs.begin()->inodeid(), AnyOf(inodeId1, inodeId2));
  ASSERT_EQ(getXAttrs.begin()->fsid(), fsId_);
  ASSERT_THAT(getXAttrs.begin()->xattrinfos().find(XATTR_DIR_FBYTES)->second,
              AnyOf("100", "200"));
}

TEST_F(TestInodeCacheManager, CreateAndGetInodeWhenTimeout) {
  dingofs::client::FLAGS_enableCto = false;
  uint64_t inodeId = 100;

  InodeParam param;
  param.fsId = fsId_;
  param.type = FsFileType::TYPE_FILE;

  Inode inode;
  inode.set_inodeid(inodeId);
  inode.set_fsid(fsId_);
  inode.set_type(FsFileType::TYPE_FILE);
  EXPECT_CALL(*metaClient_, CreateInode(_, _))
      .WillOnce(DoAll(SetArgPointee<1>(inode), Return(MetaStatusCode::OK)));

  std::shared_ptr<InodeWrapper> inodeWrapper;
  DINGOFS_ERROR ret = iCacheManager_->CreateInode(param, inodeWrapper);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);

  Inode out = inodeWrapper->GetInode();
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);
  ASSERT_EQ(inodeId, out.inodeid());
  ASSERT_EQ(fsId_, out.fsid());
  ASSERT_EQ(FsFileType::TYPE_FILE, out.type());

  sleep(timeout_);
  // 1. inode dirty, get from icache
  inodeWrapper->MarkDirty();
  ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);

  // 2. not dirty get from metaserver
  inodeWrapper->ClearDirty();
  EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(inode), SetArgPointee<3>(false),
                      Return(MetaStatusCode::OK)));
  ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
  ASSERT_EQ(DINGOFS_ERROR::OK, ret);
}
}  // namespace client
}  // namespace dingofs
