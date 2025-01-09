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
 * @Project: dingo
 * @Date: 2021-06-10 10:04:37
 * @Author: chenwei
 */
#include "mds/fs_manager.h"

#include <brpc/channel.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gmock/gmock.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "aws/mock_s3_adapter.h"
#include "common/define.h"
#include "mds/mock/mock_cli2.h"
#include "mds/mock/mock_metaserver.h"
#include "mds/mock/mock_topology.h"
#include "mds/topology/topology_storage_codec.h"
#include "mds/topology/topology_storge_etcd.h"
#include "proto/common.pb.h"
#include "proto/mds.pb.h"

using ::dingofs::aws::MockS3Adapter;
using ::dingofs::mds::topology::DefaultIdGenerator;
using ::dingofs::mds::topology::DefaultTokenGenerator;
using ::dingofs::mds::topology::FsIdType;
using ::dingofs::mds::topology::MockEtcdClient;
using ::dingofs::mds::topology::MockTopologyManager;
using ::dingofs::mds::topology::TopologyIdGenerator;
using ::dingofs::mds::topology::TopologyImpl;
using ::dingofs::mds::topology::TopologyStorageCodec;
using ::dingofs::mds::topology::TopologyStorageEtcd;
using ::dingofs::mds::topology::TopologyTokenGenerator;
using ::dingofs::mds::topology::TopoStatusCode;
using ::dingofs::metaserver::MockMetaserverService;
using ::dingofs::metaserver::copyset::MockCliService2;

using ::dingofs::pb::common::FSType;
using ::dingofs::pb::common::S3Info;
using ::dingofs::pb::common::Volume;
using ::dingofs::pb::mds::FsDetail;
using ::dingofs::pb::mds::FsInfo;
using ::dingofs::pb::mds::FsStatsData;
using ::dingofs::pb::mds::FsStatus;
using ::dingofs::pb::mds::GetFsStatsRequest;
using ::dingofs::pb::mds::GetFsStatsResponse;
using ::dingofs::pb::mds::SetFsStatsRequest;
using ::dingofs::pb::mds::SetFsStatsResponse;
using ::dingofs::pb::metaserver::CreateRootInodeRequest;
using ::dingofs::pb::metaserver::CreateRootInodeResponse;
using ::dingofs::pb::metaserver::MetaStatusCode;
using ::dingofs::pb::metaserver::copyset::GetLeaderRequest2;
using ::dingofs::pb::metaserver::copyset::GetLeaderResponse2;

using ::google::protobuf::util::MessageDifferencer;

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

namespace dingofs {
namespace mds {
class FSManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    addr_ = "127.0.0.1:6704";
    MetaserverOptions metaserverOptions;
    metaserverOptions.metaserverAddr = addr_;
    metaserverOptions.rpcTimeoutMs = 500;
    fsStorage_ = std::make_shared<MemoryFsStorage>();
    metaserverClient_ = std::make_shared<MetaserverClient>(metaserverOptions);
    // init mock topology manager
    std::shared_ptr<TopologyIdGenerator> idGenerator_ =
        std::make_shared<DefaultIdGenerator>();
    std::shared_ptr<TopologyTokenGenerator> tokenGenerator_ =
        std::make_shared<DefaultTokenGenerator>();

    auto etcdClient_ = std::make_shared<MockEtcdClient>();
    auto codec = std::make_shared<TopologyStorageCodec>();
    auto topoStorage_ =
        std::make_shared<TopologyStorageEtcd>(etcdClient_, codec);
    topoManager_ = std::make_shared<MockTopologyManager>(
        std::make_shared<TopologyImpl>(idGenerator_, tokenGenerator_,
                                       topoStorage_),
        metaserverClient_);
    // init fsmanager
    FsManagerOption fsManagerOption;
    fsManagerOption.backEndThreadRunInterSec = 1;
    fsManagerOption.clientTimeoutSec = 1;
    s3Adapter_ = std::make_shared<MockS3Adapter>();
    fsManager_ =
        std::make_shared<FsManager>(fsStorage_, metaserverClient_, topoManager_,
                                    s3Adapter_, nullptr, fsManagerOption);
    ASSERT_TRUE(fsManager_->Init());

    ASSERT_EQ(0, server_.AddService(&mockMetaserverService_,
                                    brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server_.AddService(&mockCliService2_,
                                    brpc::SERVER_DOESNT_OWN_SERVICE));

    ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
  }

  void TearDown() override {
    server_.Stop(0);
    server_.Join();
    fsManager_->Uninit();
  }

  static bool CompareVolume(const Volume& first, const Volume& second) {
    return MessageDifferencer::Equals(first, second);
  }

  static bool CompareVolumeFs(const FsInfo& first, const FsInfo& second) {
    return first.fsid() == second.fsid() && first.fsname() == second.fsname() &&
           first.rootinodeid() == second.rootinodeid() &&
           first.capacity() == second.capacity() &&
           first.blocksize() == second.blocksize() &&
           first.fstype() == second.fstype() && first.detail().has_volume() &&
           second.detail().has_volume() &&
           CompareVolume(first.detail().volume(), second.detail().volume());
  }

  static bool CompareS3Info(const S3Info& first, const S3Info& second) {
    return MessageDifferencer::Equals(first, second);
  }

  static bool CompareS3Fs(const FsInfo& first, const FsInfo& second) {
    return first.fsid() == second.fsid() && first.fsname() == second.fsname() &&
           first.rootinodeid() == second.rootinodeid() &&
           first.capacity() == second.capacity() &&
           first.blocksize() == second.blocksize() &&
           first.fstype() == second.fstype() && first.detail().has_s3info() &&
           second.detail().has_s3info() &&
           CompareS3Info(first.detail().s3info(), second.detail().s3info());
    return MessageDifferencer::Equals(first, second);
  }

 protected:
  std::shared_ptr<FsManager> fsManager_;
  std::shared_ptr<FsStorage> fsStorage_;
  std::shared_ptr<MetaserverClient> metaserverClient_;
  MockMetaserverService mockMetaserverService_;
  MockCliService2 mockCliService2_;
  std::shared_ptr<MockTopologyManager> topoManager_;
  brpc::Server server_;
  std::shared_ptr<MockS3Adapter> s3Adapter_;
  std::string addr_;
};

template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void RpcService(google::protobuf::RpcController* cntl_base,
                const RpcRequestType* request, RpcResponseType* response,
                google::protobuf::Closure* done) {
  if (RpcFailed) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    cntl->SetFailed(112, "Not connected to");
  }
  done->Run();
}

TEST_F(FSManagerTest, backgroud_thread_test) {
  fsManager_->Run();
  fsManager_->Run();
  fsManager_->Run();
  fsManager_->Uninit();
  fsManager_->Uninit();
  fsManager_->Run();
  fsManager_->Uninit();
}

TEST_F(FSManagerTest, background_thread_deletefs_test) {
  fsManager_->Run();
  std::string addr = addr_;
  std::string leader = addr_ + ":0";
  FSStatusCode ret;
  uint64_t blockSize = 4096;
  bool enableSumInDir = false;

  pb::mds::CreateFsRequest req;
  req.set_blocksize(blockSize);
  req.set_enablesumindir(enableSumInDir);
  req.set_owner("test");
  req.set_capacity((uint64_t)100 * 1024 * 1024 * 1024);

  // create volume fs ok
  std::set<std::string> addrs;
  addrs.emplace(addr);

  // create s3 test
  std::string fsName2 = "fs2";
  dingofs::pb::common::S3Info s3Info;
  FsInfo s3FsInfo;
  s3Info.set_ak("ak");
  s3Info.set_sk("sk");
  s3Info.set_endpoint("endpoint");
  s3Info.set_bucketname("bucketname");
  s3Info.set_blocksize(4096);
  s3Info.set_chunksize(4096);
  CreateRootInodeResponse response2;
  FsDetail detail2;
  detail2.set_allocated_s3info(new S3Info(s3Info));

  // create s3 fs ok
  GetLeaderResponse2 getLeaderResponse;
  getLeaderResponse.mutable_leader()->set_address(leader);

  EXPECT_CALL(*topoManager_, CreatePartitionsAndGetMinPartition(_, _))
      .WillOnce(Return(TopoStatusCode::TOPO_OK));
  EXPECT_CALL(*topoManager_, GetCopysetMembers(_, _, _))
      .WillOnce(
          DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
  EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
      .WillOnce(
          DoAll(SetArgPointee<2>(getLeaderResponse),
                Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));

  CreateRootInodeResponse response;
  response.set_statuscode(MetaStatusCode::OK);
  EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
      .WillOnce(DoAll(
          SetArgPointee<2>(response),
          Invoke(RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));
  EXPECT_CALL(*s3Adapter_, BucketExist()).WillOnce(Return(true));

  req.set_fsname(fsName2);
  req.set_fstype(FSType::TYPE_S3);
  req.set_allocated_fsdetail(new FsDetail(detail2));
  ret = fsManager_->CreateFs(&req, &s3FsInfo);
  ASSERT_EQ(ret, FSStatusCode::OK);
  ASSERT_EQ(s3FsInfo.fsid(), 0);
  ASSERT_EQ(s3FsInfo.fsname(), fsName2);
  ASSERT_EQ(s3FsInfo.status(), FsStatus::INITED);
  ASSERT_EQ(s3FsInfo.rootinodeid(), ROOTINODEID);
  ASSERT_EQ(s3FsInfo.capacity(), (uint64_t)100 * 1024 * 1024 * 1024);
  ASSERT_EQ(s3FsInfo.blocksize(), blockSize);
  ASSERT_EQ(s3FsInfo.mountnum(), 0);
  ASSERT_EQ(s3FsInfo.fstype(), FSType::TYPE_S3);

  // TEST GetFsInfo
  FsInfo fsInfo2;
  ret = fsManager_->GetFsInfo(fsName2, &fsInfo2);
  ASSERT_EQ(ret, FSStatusCode::OK);
  ASSERT_TRUE(CompareS3Fs(fsInfo2, s3FsInfo));

  // TEST DeleteFs, delete fs2
  std::list<PartitionInfo> list;
  std::list<PartitionInfo> list2;
  std::list<PartitionInfo> list3;

  PartitionInfo partition;
  uint32_t poolId2 = 4;
  uint32_t copysetId2 = 5;
  uint32_t partitionId2 = 6;
  partition.set_poolid(poolId2);
  partition.set_copysetid(copysetId2);
  partition.set_partitionid(partitionId2);
  partition.set_status(PartitionStatus::DELETING);
  std::list<PartitionInfo> list4;
  list4.push_back(partition);

  EXPECT_CALL(*topoManager_, ListPartitionOfFs(fsInfo2.fsid(), _))
      .WillOnce(SetArgPointee<1>(list4))
      .WillOnce(SetArgPointee<1>(list));

  ret = fsManager_->DeleteFs(fsName2);
  ASSERT_EQ(ret, FSStatusCode::OK);

  sleep(3);

  FsInfo fsInfo;
  ret = fsManager_->GetFsInfo(fsInfo2.fsid(), &fsInfo);
  ASSERT_EQ(ret, FSStatusCode::NOT_FOUND);
}

TEST_F(FSManagerTest, test_refreshSession) {
  topology::PartitionTxId tmp;
  tmp.set_partitionid(1);
  tmp.set_txid(1);
  std::string fsName = "fs1";
  pb::mds::Mountpoint mountpoint;
  mountpoint.set_hostname("127.0.0.1");
  mountpoint.set_port(9000);
  mountpoint.set_path("/mnt");

  {
    LOG(INFO) << "### case1: partition txid need update ###";
    pb::mds::RefreshSessionRequest request;
    pb::mds::RefreshSessionResponse response;
    std::vector<topology::PartitionTxId> txidlist({std::move(tmp)});
    *request.mutable_txids() = {txidlist.begin(), txidlist.end()};
    request.set_fsname(fsName);
    *request.mutable_mountpoint() = mountpoint;
    EXPECT_CALL(*topoManager_, GetLatestPartitionsTxId(_, _))
        .WillOnce(SetArgPointee<1>(txidlist));
    fsManager_->RefreshSession(&request, &response);
    ASSERT_EQ(1, response.latesttxidlist_size());
  }
  {
    LOG(INFO) << "### case2: partition txid do not need update ###";
    pb::mds::RefreshSessionResponse response;
    pb::mds::RefreshSessionRequest request;
    request.set_fsname(fsName);
    *request.mutable_mountpoint() = mountpoint;
    fsManager_->RefreshSession(&request, &response);
    ASSERT_EQ(0, response.latesttxidlist_size());
  }
}

TEST_F(FSManagerTest, GetLatestTxId_ParamFsId) {
  // CASE 1: GetLatestTxId without fsid param
  {
    pb::mds::GetLatestTxIdRequest request;
    pb::mds::GetLatestTxIdResponse response;
    fsManager_->GetLatestTxId(&request, &response);
    ASSERT_EQ(response.statuscode(), FSStatusCode::PARAM_ERROR);
  }

  // CASE 2: GetLatestTxId with fsid
  {
    pb::mds::GetLatestTxIdRequest request;
    pb::mds::GetLatestTxIdResponse response;
    request.set_fsid(1);
    EXPECT_CALL(*topoManager_, ListPartitionOfFs(_, _))
        .WillOnce(Invoke([&](FsIdType fsId, std::list<PartitionInfo>* list) {
          if (fsId != 1) {
            return;
          }
          PartitionInfo partition;
          partition.set_fsid(0);
          partition.set_poolid(0);
          partition.set_copysetid(0);
          partition.set_partitionid(0);
          partition.set_start(0);
          partition.set_end(0);
          partition.set_txid(0);
          list->push_back(partition);
        }));
    fsManager_->GetLatestTxId(&request, &response);
    ASSERT_EQ(response.statuscode(), FSStatusCode::OK);
    ASSERT_EQ(response.txids_size(), 1);
  }
}

TEST_F(FSManagerTest, SetFsStats) {
  {
    SetFsStatsRequest request;
    SetFsStatsResponse response;

    FsStatsData fsstatsdata;
    fsstatsdata.set_readbytes(8192);
    fsstatsdata.set_writebytes(16384);
    fsstatsdata.set_readqps(10);
    fsstatsdata.set_writeqps(20);
    fsstatsdata.set_s3readbytes(8192);
    fsstatsdata.set_s3writebytes(16384);
    fsstatsdata.set_s3readqps(30);
    fsstatsdata.set_s3writeqps(40);

    request.set_fsname("dingofs");
    request.mutable_fsstatsdata()->CopyFrom(fsstatsdata);

    fsManager_->SetFsStats(&request, &response);
    ASSERT_EQ(response.statuscode(), FSStatusCode::OK);
  }
}

TEST_F(FSManagerTest, GetFsStats) {
  {
    GetFsStatsRequest request;
    GetFsStatsResponse response;

    request.set_fsname("dingofs");

    fsManager_->GetFsStats(&request, &response);
    ASSERT_EQ(response.statuscode(), FSStatusCode::OK);
  }
}

}  // namespace mds
}  // namespace dingofs
