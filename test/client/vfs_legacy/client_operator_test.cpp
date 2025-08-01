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

/**
 * Project: Dingofs
 * Created Date: 2021-09-11
 * Author: Jingli Chen (Wine93)
 */

#include "client/vfs_legacy/client_operator.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "dingofs/metaserver.pb.h"
#include "client/vfs_legacy/mock_dentry_cache_mamager.h"
#include "client/vfs_legacy/mock_inode_cache_manager.h"
#include "client/vfs_legacy/mock_metaserver_client.h"
#include "stub/rpcclient/mock_mds_client.h"

namespace dingofs {
namespace client {

using dingofs::pb::mds::FSStatusCode;
using dingofs::pb::metaserver::MetaStatusCode;
using dingofs::stub::rpcclient::MockMdsClient;
using dingofs::stub::rpcclient::MockMetaServerClient;

using ::testing::DoAll;
using ::testing::SetArgPointee;

class ClientOperatorTest : public ::testing::Test {
 protected:
  ClientOperatorTest() {
    fsId_ = 1;
    fsname_ = "/test";
    parentId_ = 10;
    name_ = "A";
    newParentId_ = 20;
    newname_ = "B";
    dentryManager_ = std::make_shared<MockDentryCacheManager>();
    inodeManager_ = std::make_shared<MockInodeCacheManager>();
    metaClient_ = std::make_shared<MockMetaServerClient>();
    mdsClient_ = std::make_shared<MockMdsClient>();
    renameOp_ = std::make_shared<RenameOperator>(
        fsId_, fsname_, parentId_, name_, newParentId_, newname_,
        dentryManager_, inodeManager_, metaClient_, mdsClient_, false);
  }

  ~ClientOperatorTest() {}

  void SetUp() override {}

  void TearDown() override {}

 protected:
  uint32_t fsId_;
  std::string fsname_;
  uint64_t parentId_;
  std::string name_;
  uint64_t newParentId_;
  std::string newname_;
  std::shared_ptr<MockDentryCacheManager> dentryManager_;
  std::shared_ptr<MockInodeCacheManager> inodeManager_;
  std::shared_ptr<MockMetaServerClient> metaClient_;
  std::shared_ptr<MockMdsClient> mdsClient_;
  std::shared_ptr<RenameOperator> renameOp_;
};

TEST_F(ClientOperatorTest, GetTxId) {
  // CASE 1: get src txid fail
  EXPECT_CALL(*metaClient_, GetTxId(_, _, _, _))
      .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));
  auto rc = renameOp_->GetTxId();
  ASSERT_EQ(rc, DINGOFS_ERROR::UNKNOWN);

  // CASE 2: get dst txid fail
  EXPECT_CALL(*metaClient_, GetTxId(_, _, _, _))
      .WillOnce(Return(MetaStatusCode::OK))
      .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));
  rc = renameOp_->GetTxId();
  ASSERT_EQ(rc, DINGOFS_ERROR::UNKNOWN);

  // CASE 3: get txid success
  EXPECT_CALL(*metaClient_, GetTxId(_, _, _, _))
      .WillOnce(Return(MetaStatusCode::OK))
      .WillOnce(Return(MetaStatusCode::OK));
  rc = renameOp_->GetTxId();
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);
}

TEST_F(ClientOperatorTest, Precheck) {
  // CASE 1: get src dentry fail
  EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
      .WillOnce(Return(DINGOFS_ERROR::UNKNOWN));

  auto rc = renameOp_->Precheck();
  ASSERT_EQ(rc, DINGOFS_ERROR::UNKNOWN);

  // CASE 2: get dst dentry fail
  EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
      .WillOnce(Return(DINGOFS_ERROR::OK))
      .WillOnce(Return(DINGOFS_ERROR::UNKNOWN));

  rc = renameOp_->Precheck();
  ASSERT_EQ(rc, DINGOFS_ERROR::UNKNOWN);

  // CASE 3: check success
  EXPECT_CALL(*dentryManager_, GetDentry(_, _, _))
      .WillOnce(Return(DINGOFS_ERROR::OK))
      .WillOnce(Return(DINGOFS_ERROR::NOTEXIST));

  rc = renameOp_->Precheck();
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);
}

TEST_F(ClientOperatorTest, PrepareTx) {
  // CASE 1: PrepareTx fail (same partition)
  EXPECT_CALL(*metaClient_, GetTxId(_, _, _, _))
      .WillRepeatedly(DoAll(SetArgPointee<2>(1), Return(MetaStatusCode::OK)));
  ASSERT_EQ(renameOp_->GetTxId(), DINGOFS_ERROR::OK);

  EXPECT_CALL(*metaClient_, PrepareRenameTx(_))
      .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

  auto rc = renameOp_->PrepareTx();
  ASSERT_EQ(rc, DINGOFS_ERROR::UNKNOWN);

  // CASE 2: PrepareTx success (same partition)
  EXPECT_CALL(*metaClient_, PrepareRenameTx(_))
      .WillOnce(Return(MetaStatusCode::OK));

  rc = renameOp_->PrepareTx();
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);

  // CASE 3: PrepareTx fail (different partition)
  EXPECT_CALL(*metaClient_, GetTxId(_, _, _, _))
      .WillOnce(DoAll(SetArgPointee<2>(1), Return(MetaStatusCode::OK)))
      .WillOnce(DoAll(SetArgPointee<2>(2), Return(MetaStatusCode::OK)));
  ASSERT_EQ(renameOp_->GetTxId(), DINGOFS_ERROR::OK);

  EXPECT_CALL(*metaClient_, PrepareRenameTx(_))
      .WillOnce(Return(MetaStatusCode::OK))
      .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

  rc = renameOp_->PrepareTx();
  ASSERT_EQ(rc, DINGOFS_ERROR::UNKNOWN);

  EXPECT_CALL(*metaClient_, PrepareRenameTx(_))
      .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

  rc = renameOp_->PrepareTx();
  ASSERT_EQ(rc, DINGOFS_ERROR::UNKNOWN);

  // CASE 4: PrepareTx success (different partition)
  EXPECT_CALL(*metaClient_, PrepareRenameTx(_))
      .Times(2)
      .WillRepeatedly(Return(MetaStatusCode::OK));

  rc = renameOp_->PrepareTx();
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);
}

TEST_F(ClientOperatorTest, CommitTx) {
  // CASE 1: CommitTx fail
  EXPECT_CALL(*mdsClient_, CommitTx(_))
      .WillOnce(Return(FSStatusCode::UNKNOWN_ERROR));

  auto rc = renameOp_->CommitTx();
  ASSERT_EQ(rc, DINGOFS_ERROR::INTERNAL);

  // CASE 2: CommitTx success
  EXPECT_CALL(*mdsClient_, CommitTx(_)).WillOnce(Return(FSStatusCode::OK));

  rc = renameOp_->CommitTx();
  ASSERT_EQ(rc, DINGOFS_ERROR::OK);
}

}  // namespace client
}  // namespace dingofs
