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
 * @Date: 2021-06-10 10:47:07
 * @Author: chenwei
 */

#include "mds/mds_service.h"

#include <brpc/channel.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include <string>

#include "aws/mock_s3_adapter.h"
#include "dingofs/mds.pb.h"
#include "mds/fake_metaserver.h"
#include "mds/mock/mock_cli2.h"
#include "mds/mock/mock_kvstorage_client.h"
#include "mds/mock/mock_topology.h"
#include "mds/topology/topology_storage_codec.h"
#include "mds/topology/topology_storge_etcd.h"

using ::dingofs::dataaccess::aws::MockS3Adapter;
using ::dingofs::mds::topology::DefaultIdGenerator;
using ::dingofs::mds::topology::DefaultTokenGenerator;
using ::dingofs::mds::topology::MockEtcdClient;
using ::dingofs::mds::topology::MockTopologyManager;
using ::dingofs::mds::topology::TopologyIdGenerator;
using ::dingofs::mds::topology::TopologyImpl;
using ::dingofs::mds::topology::TopologyStorageCodec;
using ::dingofs::mds::topology::TopologyStorageEtcd;
using ::dingofs::mds::topology::TopologyTokenGenerator;
using ::dingofs::mds::topology::TopoStatusCode;
using ::dingofs::metaserver::FakeMetaserverImpl;
using ::dingofs::metaserver::copyset::MockCliService2;

using ::dingofs::pb::common::FSType;
using ::dingofs::pb::common::S3Info;
using ::dingofs::pb::common::Volume;
using ::dingofs::pb::mds::FsInfo;
using ::dingofs::pb::mds::Mountpoint;
using ::dingofs::pb::mds::RefreshSessionRequest;
using ::dingofs::pb::mds::RefreshSessionResponse;
using ::dingofs::pb::metaserver::copyset::GetLeaderRequest2;
using ::dingofs::pb::metaserver::copyset::GetLeaderResponse2;

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

using ::dingofs::dataaccess::aws::MockS3Adapter;
using ::google::protobuf::util::MessageDifferencer;

namespace dingofs {
namespace mds {
class MdsServiceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    kvstorage = std::make_shared<MockKVStorageClient>();

    MetaserverOptions metaserver_options;
    metaserver_options.metaserverAddr = "127.0.0.1:6703";
    metaserver_options.rpcTimeoutMs = 5000;
    fs_storage = std::make_shared<MemoryFsStorage>();
    metaserver_client = std::make_shared<MetaserverClient>(metaserver_options);
    // init mock topology manager
    std::shared_ptr<TopologyIdGenerator> id_generator =
        std::make_shared<DefaultIdGenerator>();
    std::shared_ptr<TopologyTokenGenerator> token_generator =
        std::make_shared<DefaultTokenGenerator>();

    auto etcd_client = std::make_shared<MockEtcdClient>();
    auto codec = std::make_shared<TopologyStorageCodec>();
    auto topo_storage =
        std::make_shared<TopologyStorageEtcd>(etcd_client, codec);
    topo_manager = std::make_shared<MockTopologyManager>(
        std::make_shared<TopologyImpl>(id_generator, token_generator,
                                       topo_storage),
        metaserver_client);

    // init fsmanager
    FsManagerOption fs_manager_option;
    fs_manager_option.backEndThreadRunInterSec = 1;
    s3_adapter = std::make_shared<MockS3Adapter>();
    fs_manager =
        std::make_shared<FsManager>(fs_storage, metaserver_client, topo_manager,
                                    s3_adapter, nullptr, fs_manager_option);
    ASSERT_TRUE(fs_manager->Init());
  }

  static bool CompareVolume(const Volume& first, const Volume& second) {
#define COMPARE_FIELD(field)                   \
  (first.has_##field() && second.has_##field() \
       ? first.field() == second.field()       \
       : true)

    return COMPARE_FIELD(volumesize) && COMPARE_FIELD(blocksize) &&
           COMPARE_FIELD(volumename) && COMPARE_FIELD(user) &&
           COMPARE_FIELD(password);
  }

  static bool CompareFs(const FsInfo& first, const FsInfo& second) {
    return first.fsid() == second.fsid() && first.fsname() == second.fsname() &&
           first.rootinodeid() == second.rootinodeid() &&
           first.capacity() == second.capacity() &&
           first.blocksize() == second.blocksize() &&
           CompareVolume(first.detail().volume(), second.detail().volume());
  }

  std::shared_ptr<FsManager> fs_manager;
  std::shared_ptr<FsStorage> fs_storage;
  std::shared_ptr<MetaserverClient> metaserver_client;
  std::shared_ptr<MockKVStorageClient> kvstorage;
  std::shared_ptr<MockTopologyManager> topo_manager;
  std::shared_ptr<MockS3Adapter> s3_adapter;
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

TEST_F(MdsServiceTest, test1) {
  brpc::Server server;
  // add metaserver service
  MdsServiceImpl mds_service(fs_manager, nullptr);
  ASSERT_EQ(server.AddService(&mds_service, brpc::SERVER_DOESNT_OWN_SERVICE),
            0);

  FakeMetaserverImpl metaserver_service;
  ASSERT_EQ(
      server.AddService(&metaserver_service, brpc::SERVER_DOESNT_OWN_SERVICE),
      0);

  MockCliService2 mock_cli_service2;
  ASSERT_EQ(
      server.AddService(&mock_cli_service2, brpc::SERVER_DOESNT_OWN_SERVICE),
      0);

  // start rpc server
  brpc::ServerOptions option;
  std::string addr = "127.0.0.1:6703";
  std::string leader = "127.0.0.1:6703:0";
  ASSERT_EQ(server.Start(addr.c_str(), &option), 0);

  // init client
  brpc::Channel channel;
  ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);

  pb::mds::MdsService_Stub stub(&channel);
  brpc::Controller cntl;

  // test CreateFS
  GetLeaderResponse2 get_leader_response;
  get_leader_response.mutable_leader()->set_address(leader);

  std::set<std::string> addrs;
  addrs.emplace(addr);

  // create s3 fsinfo1
  FsInfo fsinfo1;
  {
    pb::mds::CreateFsRequest create_request;
    pb::mds::CreateFsResponse create_response;

    const auto capacity = 100ULL << 30;
    create_request.set_capacity(capacity);
    create_request.set_owner("test");

    create_request.set_blocksize(4096);
    create_request.set_enablesumindir(false);
    create_request.set_fsname("fs1");
    create_request.set_fstype(FSType::TYPE_S3);

    S3Info s3info;
    s3info.set_ak("ak");
    s3info.set_sk("sk");
    s3info.set_endpoint("endpoint");
    s3info.set_bucketname("bucketname");
    s3info.set_blocksize(4096);
    s3info.set_chunksize(4096);
    create_request.mutable_fsdetail()->mutable_s3info()->CopyFrom(s3info);

    EXPECT_CALL(*topo_manager, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    EXPECT_CALL(*topo_manager, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
    EXPECT_CALL(mock_cli_service2, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(get_leader_response),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));
    EXPECT_CALL(*s3_adapter, BucketExist()).WillOnce(Return(true));

    cntl.set_timeout_ms(5000);
    stub.CreateFs(&cntl, &create_request, &create_response, nullptr);
    if (!cntl.Failed()) {
      ASSERT_EQ(create_response.statuscode(), FSStatusCode::OK);
      ASSERT_TRUE(create_response.has_fsinfo());
      fsinfo1 = create_response.fsinfo();
    } else {
      LOG(ERROR) << "error = " << cntl.ErrorText();
      ASSERT_TRUE(false);
    }
  }

  // create s3 fsinfo2
  cntl.Reset();
  FsInfo fsinfo2;

  {
    pb::mds::CreateFsRequest create_request;
    pb::mds::CreateFsResponse create_response;

    // create s3 fs, s3info not set
    const auto capacity = 100ULL << 30;
    create_request.set_capacity(capacity);
    create_request.set_owner("test");

    create_request.set_blocksize(4096);
    create_request.set_enablesumindir(false);
    create_request.set_fsname("fs2");
    create_request.set_fstype(FSType::TYPE_S3);
    create_request.mutable_fsdetail();
    stub.CreateFs(&cntl, &create_request, &create_response, nullptr);
    if (!cntl.Failed()) {
      ASSERT_EQ(create_response.statuscode(), FSStatusCode::PARAM_ERROR);
    } else {
      LOG(ERROR) << "error = " << cntl.ErrorText();
      ASSERT_TRUE(false);
    }
  }

  {
    // create s3 fs, OK

    cntl.Reset();
    pb::mds::CreateFsRequest create_request;
    pb::mds::CreateFsResponse create_response;

    const auto capacity = 100ULL << 30;
    create_request.set_capacity(capacity);
    create_request.set_owner("test");

    create_request.set_blocksize(4096);
    create_request.set_enablesumindir(false);

    create_request.set_fsname("fs2");
    create_request.set_fstype(FSType::TYPE_S3);
    S3Info s3info;
    s3info.set_ak("ak");
    s3info.set_sk("sk");
    s3info.set_endpoint("endpoint");
    s3info.set_bucketname("bucketname");
    s3info.set_blocksize(4096);
    s3info.set_chunksize(4096);
    create_request.mutable_fsdetail()->mutable_s3info()->CopyFrom(s3info);

    EXPECT_CALL(*topo_manager, CreatePartitionsAndGetMinPartition(_, _))
        .WillOnce(Return(TopoStatusCode::TOPO_OK));
    EXPECT_CALL(*topo_manager, GetCopysetMembers(_, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(addrs), Return(TopoStatusCode::TOPO_OK)));
    EXPECT_CALL(mock_cli_service2, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(get_leader_response),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));
    EXPECT_CALL(*s3_adapter, BucketExist()).WillOnce(Return(true));

    cntl.set_timeout_ms(5000);
    stub.CreateFs(&cntl, &create_request, &create_response, nullptr);
    if (!cntl.Failed()) {
      ASSERT_EQ(create_response.statuscode(), FSStatusCode::OK);
      ASSERT_TRUE(create_response.has_fsinfo());
      fsinfo2 = create_response.fsinfo();
    } else {
      LOG(ERROR) << "error = " << cntl.ErrorText();
      ASSERT_TRUE(false);
    }
  }

  // test MountFs
  cntl.Reset();
  pb::mds::Mountpoint mount_point;
  mount_point.set_hostname("host1");
  mount_point.set_port(9000);
  mount_point.set_path("/a/b/c");
  mount_point.set_cto(false);
  pb::mds::MountFsRequest mount_request;
  pb::mds::MountFsResponse mount_response;
  mount_request.set_fsname("fs1");
  mount_request.set_allocated_mountpoint(new pb::mds::Mountpoint(mount_point));
  stub.MountFs(&cntl, &mount_request, &mount_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(mount_response.statuscode(), FSStatusCode::OK);
    ASSERT_TRUE(mount_response.has_fsinfo());
    ASSERT_TRUE(CompareFs(mount_response.fsinfo(), fsinfo1));
    ASSERT_EQ(mount_response.fsinfo().mountnum(), 1);
    ASSERT_EQ(mount_response.fsinfo().mountpoints_size(), 1);
    ASSERT_EQ(MessageDifferencer::Equals(mount_response.fsinfo().mountpoints(0),
                                         mount_point),
              true);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  cntl.Reset();
  stub.MountFs(&cntl, &mount_request, &mount_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(mount_response.statuscode(), FSStatusCode::MOUNT_POINT_CONFLICT);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  cntl.Reset();
  pb::mds::Mountpoint mount_point2;
  mount_point2.set_hostname("host1");
  mount_point2.set_port(9000);
  mount_point2.set_path("/a/b/d");
  mount_point2.set_cto(false);
  mount_request.set_allocated_mountpoint(new Mountpoint(mount_point2));
  stub.MountFs(&cntl, &mount_request, &mount_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(mount_response.statuscode(), FSStatusCode::OK);
    ASSERT_TRUE(mount_response.has_fsinfo());
    ASSERT_TRUE(CompareFs(mount_response.fsinfo(), fsinfo1));
    ASSERT_EQ(mount_response.fsinfo().mountnum(), 2);
    ASSERT_EQ(mount_response.fsinfo().mountpoints_size(), 2);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  cntl.Reset();
  Mountpoint mount_point3;
  mount_point3.set_hostname("host2");
  mount_point3.set_port(9000);
  mount_point3.set_path("/a/b/d");
  mount_point3.set_cto(false);
  mount_request.set_allocated_mountpoint(new Mountpoint(mount_point3));
  stub.MountFs(&cntl, &mount_request, &mount_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(mount_response.statuscode(), FSStatusCode::OK);
    ASSERT_TRUE(mount_response.has_fsinfo());
    ASSERT_TRUE(CompareFs(mount_response.fsinfo(), fsinfo1));
    ASSERT_EQ(mount_response.fsinfo().mountnum(), 3);
    ASSERT_EQ(mount_response.fsinfo().mountpoints_size(), 3);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  cntl.Reset();
  mount_point.set_hostname("host2");
  mount_point.set_port(9000);
  mount_point.set_path("/a/b/c");
  mount_point.set_cto(false);
  mount_request.set_allocated_mountpoint(new Mountpoint(mount_point));
  stub.MountFs(&cntl, &mount_request, &mount_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(mount_response.statuscode(), FSStatusCode::OK);
    ASSERT_TRUE(mount_response.has_fsinfo());
    ASSERT_TRUE(CompareFs(mount_response.fsinfo(), fsinfo1));
    ASSERT_EQ(mount_response.fsinfo().mountnum(), 4);
    ASSERT_EQ(mount_response.fsinfo().mountpoints_size(), 4);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // TEST GetFsInfo
  // no fsid and no fsname
  cntl.Reset();
  pb::mds::GetFsInfoRequest get_request;
  pb::mds::GetFsInfoResponse get_response;
  stub.GetFsInfo(&cntl, &get_request, &get_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(get_response.statuscode(), FSStatusCode::PARAM_ERROR);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // fsid1
  cntl.Reset();
  get_request.set_fsid(fsinfo1.fsid());
  stub.GetFsInfo(&cntl, &get_request, &get_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(get_response.statuscode(), FSStatusCode::OK);
    ASSERT_TRUE(get_response.has_fsinfo());
    ASSERT_TRUE(CompareFs(get_response.fsinfo(), fsinfo1));
    ASSERT_EQ(get_response.fsinfo().mountnum(), 4);
    ASSERT_EQ(get_response.fsinfo().mountpoints_size(), 4);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // fsid2
  cntl.Reset();
  get_request.set_fsid(fsinfo2.fsid());
  stub.GetFsInfo(&cntl, &get_request, &get_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(get_response.statuscode(), FSStatusCode::OK);
    ASSERT_TRUE(get_response.has_fsinfo());
    ASSERT_TRUE(CompareFs(get_response.fsinfo(), fsinfo2));
    ASSERT_EQ(get_response.fsinfo().mountnum(), 0);
    ASSERT_EQ(get_response.fsinfo().mountpoints_size(), 0);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // wrong fsid
  cntl.Reset();
  get_request.set_fsid(fsinfo2.fsid() + 1);
  stub.GetFsInfo(&cntl, &get_request, &get_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(get_response.statuscode(), FSStatusCode::NOT_FOUND);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // fsname1
  cntl.Reset();
  get_request.clear_fsid();
  get_request.set_fsname(fsinfo1.fsname());
  stub.GetFsInfo(&cntl, &get_request, &get_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(get_response.statuscode(), FSStatusCode::OK);
    ASSERT_TRUE(get_response.has_fsinfo());
    ASSERT_TRUE(CompareFs(get_response.fsinfo(), fsinfo1));
    ASSERT_EQ(get_response.fsinfo().mountnum(), 4);
    ASSERT_EQ(get_response.fsinfo().mountpoints_size(), 4);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // fsname2
  cntl.Reset();
  get_request.clear_fsid();
  get_request.set_fsname(fsinfo2.fsname());
  stub.GetFsInfo(&cntl, &get_request, &get_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(get_response.statuscode(), FSStatusCode::OK);
    ASSERT_TRUE(get_response.has_fsinfo());
    ASSERT_TRUE(CompareFs(get_response.fsinfo(), fsinfo2));
    ASSERT_EQ(get_response.fsinfo().mountnum(), 0);
    ASSERT_EQ(get_response.fsinfo().mountpoints_size(), 0);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // wrong fsname conflict
  cntl.Reset();
  get_request.clear_fsid();
  get_request.set_fsname("wrongName");
  stub.GetFsInfo(&cntl, &get_request, &get_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(get_response.statuscode(), FSStatusCode::NOT_FOUND);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // both fsid and fsname
  cntl.Reset();
  get_request.set_fsid(fsinfo2.fsid());
  get_request.set_fsname(fsinfo2.fsname());
  stub.GetFsInfo(&cntl, &get_request, &get_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(get_response.statuscode(), FSStatusCode::OK);
    ASSERT_TRUE(get_response.has_fsinfo());
    ASSERT_TRUE(CompareFs(get_response.fsinfo(), fsinfo2));
    ASSERT_EQ(get_response.fsinfo().mountnum(), 0);
    ASSERT_EQ(get_response.fsinfo().mountpoints_size(), 0);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // fsid and fsname conflict
  cntl.Reset();
  get_request.set_fsid(fsinfo2.fsid());
  get_request.set_fsname(fsinfo1.fsname());
  stub.GetFsInfo(&cntl, &get_request, &get_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(get_response.statuscode(), FSStatusCode::PARAM_ERROR);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // fsid and fsname conflict
  cntl.Reset();
  get_request.set_fsid(fsinfo1.fsid());
  get_request.set_fsname(fsinfo2.fsname());
  stub.GetFsInfo(&cntl, &get_request, &get_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(get_response.statuscode(), FSStatusCode::PARAM_ERROR);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // TEST unmount
  cntl.Reset();
  pb::mds::UmountFsRequest umount_request;
  pb::mds::UmountFsResponse umount_response;
  umount_request.set_fsname(fsinfo1.fsname());
  mount_point.set_hostname("host1");
  mount_point.set_port(9000);
  mount_point.set_path("/a/b/c");
  umount_request.set_allocated_mountpoint(new Mountpoint(mount_point));
  stub.UmountFs(&cntl, &umount_request, &umount_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(umount_response.statuscode(), FSStatusCode::OK);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  cntl.Reset();
  stub.UmountFs(&cntl, &umount_request, &umount_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(umount_response.statuscode(),
              FSStatusCode::MOUNT_POINT_NOT_EXIST);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  cntl.Reset();
  mount_point.set_hostname("host2");
  mount_point.set_port(9000);
  mount_point.set_path("/a/b/c");
  umount_request.set_allocated_mountpoint(new Mountpoint(mount_point));
  stub.UmountFs(&cntl, &umount_request, &umount_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(umount_response.statuscode(), FSStatusCode::OK);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  cntl.Reset();
  get_request.clear_fsid();
  get_request.set_fsname(fsinfo1.fsname());
  stub.GetFsInfo(&cntl, &get_request, &get_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(get_response.statuscode(), FSStatusCode::OK);
    ASSERT_TRUE(get_response.has_fsinfo());
    ASSERT_TRUE(CompareFs(get_response.fsinfo(), fsinfo1));
    ASSERT_EQ(get_response.fsinfo().mountnum(), 2);
    ASSERT_EQ(get_response.fsinfo().mountpoints_size(), 2);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // test refresh session
  cntl.Reset();
  RefreshSessionRequest refresh_session_request;
  RefreshSessionResponse refresh_session_response;
  topology::PartitionTxId tmp;
  tmp.set_partitionid(1);
  tmp.set_txid(1);
  std::vector<topology::PartitionTxId> partition_list({std::move(tmp)});
  std::string fs_name = "fs1";
  Mountpoint mountpoint;
  mountpoint.set_hostname("127.0.0.1");
  mountpoint.set_port(9000);
  mountpoint.set_path("/mnt");
  *refresh_session_request.mutable_txids() = {partition_list.begin(),
                                              partition_list.end()};
  refresh_session_request.set_fsname(fs_name);
  *refresh_session_request.mutable_mountpoint() = mountpoint;
  EXPECT_CALL(*topo_manager, GetLatestPartitionsTxId(_, _))
      .WillOnce(SetArgPointee<1>(partition_list));
  stub.RefreshSession(&cntl, &refresh_session_request,
                      &refresh_session_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(refresh_session_response.statuscode(), FSStatusCode::OK);
    ASSERT_EQ(1, refresh_session_response.latesttxidlist_size());
    std::pair<std::string, uint64_t> tpair;
    std::string mountpath = "127.0.0.1:9000:/mnt";
    ASSERT_TRUE(fs_manager->GetClientAliveTime(mountpath, &tpair));
    ASSERT_EQ(fs_name, tpair.first);
    // RefreshSession will add a mountpoint to fs1
    cntl.Reset();
    pb::mds::UmountFsRequest umount_request;
    pb::mds::UmountFsResponse umount_response;
    umount_request.set_fsname("fs1");
    mount_point.set_hostname("127.0.0.1");
    mount_point.set_port(9000);
    mount_point.set_path("/mnt");
    umount_request.set_allocated_mountpoint(new Mountpoint(mount_point));
    stub.UmountFs(&cntl, &umount_request, &umount_response, nullptr);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // test delete fs
  cntl.Reset();
  pb::mds::DeleteFsRequest delete_request;
  pb::mds::DeleteFsResponse delete_response;
  delete_request.set_fsname(fsinfo2.fsname());
  stub.DeleteFs(&cntl, &delete_request, &delete_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(delete_response.statuscode(), FSStatusCode::OK);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  cntl.Reset();
  delete_request.set_fsname(fsinfo1.fsname());
  stub.DeleteFs(&cntl, &delete_request, &delete_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(delete_response.statuscode(), FSStatusCode::FS_BUSY);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  cntl.Reset();
  mount_point.set_hostname("host1");
  mount_point.set_port(9000);
  mount_point.set_path("/a/b/d");
  umount_request.set_allocated_mountpoint(new Mountpoint(mount_point));
  stub.UmountFs(&cntl, &umount_request, &umount_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(umount_response.statuscode(), FSStatusCode::OK);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  cntl.Reset();
  mount_point.set_hostname("host2");
  mount_point.set_port(9000);
  mount_point.set_path("/a/b/d");
  umount_request.set_allocated_mountpoint(new Mountpoint(mount_point));
  stub.UmountFs(&cntl, &umount_request, &umount_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(umount_response.statuscode(), FSStatusCode::OK);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  cntl.Reset();
  delete_request.set_fsname(fsinfo1.fsname());
  stub.DeleteFs(&cntl, &delete_request, &delete_response, nullptr);
  if (!cntl.Failed()) {
    ASSERT_EQ(delete_response.statuscode(), FSStatusCode::OK);
  } else {
    LOG(ERROR) << "error = " << cntl.ErrorText();
    ASSERT_TRUE(false);
  }

  // stop rpc server
  server.Stop(10);
  server.Join();
}
}  // namespace mds
}  // namespace dingofs
