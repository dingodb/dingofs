/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"){}
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
 * Created Date: Fri Jun 11 2021
 * Author: lixiaocui
 */

#include "stub/rpcclient/base_client.h"

#include <cstdint>

#include "dingofs/cachegroup.pb.h"

namespace dingofs {
namespace stub {
namespace rpcclient {

using pb::mds::AllocateS3ChunkRequest;
using pb::mds::AllocateS3ChunkResponse;
using pb::mds::CommitTxRequest;
using pb::mds::CommitTxResponse;
using pb::mds::GetFsInfoRequest;
using pb::mds::GetFsInfoResponse;
using pb::mds::GetLatestTxIdRequest;
using pb::mds::GetLatestTxIdResponse;
using pb::mds::MountFsRequest;
using pb::mds::MountFsResponse;
using pb::mds::Mountpoint;
using pb::mds::RefreshSessionRequest;
using pb::mds::RefreshSessionResponse;
using pb::mds::SetFsStatsRequest;
using pb::mds::SetFsStatsResponse;
using pb::mds::UmountFsRequest;
using pb::mds::UmountFsResponse;

using pb::mds::cachegroup::CacheGroupMember;
using pb::mds::cachegroup::CacheGroupMemberService_Stub;
using pb::mds::cachegroup::HeartbeatRequest;
using pb::mds::cachegroup::HeartbeatResponse;
using pb::mds::cachegroup::JoinCacheGroupRequest;
using pb::mds::cachegroup::JoinCacheGroupResponse;
using pb::mds::cachegroup::LeaveCacheGroupRequest;
using pb::mds::cachegroup::LeaveCacheGroupResponse;
using pb::mds::cachegroup::LoadMembersRequest;
using pb::mds::cachegroup::LoadMembersResponse;
using pb::mds::cachegroup::ReweightMemberRequest;
using pb::mds::cachegroup::ReweightMemberResponse;

using pb::mds::topology::AllocOrGetMemcacheClusterRequest;
using pb::mds::topology::AllocOrGetMemcacheClusterResponse;
using pb::mds::topology::CreatePartitionRequest;
using pb::mds::topology::CreatePartitionResponse;
using pb::mds::topology::GetCopysetOfPartitionRequest;
using pb::mds::topology::GetCopysetOfPartitionResponse;
using pb::mds::topology::GetMetaServerInfoRequest;

using pb::mds::topology::GetMetaServerInfoResponse;
using pb::mds::topology::GetMetaServerListInCopySetsRequest;
using pb::mds::topology::GetMetaServerListInCopySetsResponse;
using pb::mds::topology::ListPartitionRequest;
using pb::mds::topology::ListPartitionResponse;

using common::CopysetID;
using common::LogicPoolID;

void MDSBaseClient::MountFs(const std::string& fsName,
                            const Mountpoint& mountPt,
                            MountFsResponse* response, brpc::Controller* cntl,
                            brpc::Channel* channel) {
  MountFsRequest request;
  request.set_fsname(fsName);
  request.set_allocated_mountpoint(new Mountpoint(mountPt));
  dingofs::pb::mds::MdsService_Stub stub(channel);
  stub.MountFs(cntl, &request, response, nullptr);
}

void MDSBaseClient::UmountFs(const std::string& fsName,
                             const Mountpoint& mountPt,
                             UmountFsResponse* response, brpc::Controller* cntl,
                             brpc::Channel* channel) {
  UmountFsRequest request;
  request.set_fsname(fsName);
  request.set_allocated_mountpoint(new Mountpoint(mountPt));
  dingofs::pb::mds::MdsService_Stub stub(channel);
  stub.UmountFs(cntl, &request, response, nullptr);
}

void MDSBaseClient::GetFsInfo(const std::string& fsName,
                              GetFsInfoResponse* response,
                              brpc::Controller* cntl, brpc::Channel* channel) {
  GetFsInfoRequest request;
  request.set_fsname(fsName);
  dingofs::pb::mds::MdsService_Stub stub(channel);
  stub.GetFsInfo(cntl, &request, response, nullptr);
}

void MDSBaseClient::GetFsInfo(uint32_t fsId, GetFsInfoResponse* response,
                              brpc::Controller* cntl, brpc::Channel* channel) {
  GetFsInfoRequest request;
  request.set_fsid(fsId);
  dingofs::pb::mds::MdsService_Stub stub(channel);
  stub.GetFsInfo(cntl, &request, response, nullptr);
}

void MDSBaseClient::GetMetaServerInfo(uint32_t port, std::string ip,
                                      GetMetaServerInfoResponse* response,
                                      brpc::Controller* cntl,
                                      brpc::Channel* channel) {
  GetMetaServerInfoRequest request;
  request.set_hostip(ip);
  request.set_port(port);

  dingofs::pb::mds::topology::TopologyService_Stub stub(channel);
  stub.GetMetaServer(cntl, &request, response, nullptr);
}

void MDSBaseClient::GetMetaServerListInCopysets(
    const LogicPoolID& logicalpooid, const std::vector<CopysetID>& copysetidvec,
    GetMetaServerListInCopySetsResponse* response, brpc::Controller* cntl,
    brpc::Channel* channel) {
  GetMetaServerListInCopySetsRequest request;
  request.set_poolid(logicalpooid);
  for (auto copysetid : copysetidvec) {
    request.add_copysetid(copysetid);
  }

  dingofs::pb::mds::topology::TopologyService_Stub stub(channel);
  stub.GetMetaServerListInCopysets(cntl, &request, response, nullptr);
}

void MDSBaseClient::CreatePartition(uint32_t fsID, uint32_t count,
                                    CreatePartitionResponse* response,
                                    brpc::Controller* cntl,
                                    brpc::Channel* channel) {
  CreatePartitionRequest request;
  request.set_fsid(fsID);
  request.set_count(count);

  dingofs::pb::mds::topology::TopologyService_Stub stub(channel);
  stub.CreatePartition(cntl, &request, response, nullptr);
}

void MDSBaseClient::GetCopysetOfPartitions(
    const std::vector<uint32_t>& partitionIDList,
    GetCopysetOfPartitionResponse* response, brpc::Controller* cntl,
    brpc::Channel* channel) {
  GetCopysetOfPartitionRequest request;
  for (auto partitionId : partitionIDList) {
    request.add_partitionid(partitionId);
  }

  dingofs::pb::mds::topology::TopologyService_Stub stub(channel);
  stub.GetCopysetOfPartition(cntl, &request, response, nullptr);
}

void MDSBaseClient::ListPartition(uint32_t fsID,
                                  ListPartitionResponse* response,
                                  brpc::Controller* cntl,
                                  brpc::Channel* channel) {
  ListPartitionRequest request;
  request.set_fsid(fsID);

  dingofs::pb::mds::topology::TopologyService_Stub stub(channel);
  stub.ListPartition(cntl, &request, response, nullptr);
}

void MDSBaseClient::AllocS3ChunkId(uint32_t fsId, uint32_t idNum,
                                   AllocateS3ChunkResponse* response,
                                   brpc::Controller* cntl,
                                   brpc::Channel* channel) {
  AllocateS3ChunkRequest request;
  request.set_fsid(fsId);
  request.set_chunkidnum(idNum);

  dingofs::pb::mds::MdsService_Stub stub(channel);
  stub.AllocateS3Chunk(cntl, &request, response, nullptr);
}

void MDSBaseClient::RefreshSession(const RefreshSessionRequest& request,
                                   RefreshSessionResponse* response,
                                   brpc::Controller* cntl,
                                   brpc::Channel* channel) {
  dingofs::pb::mds::MdsService_Stub stub(channel);
  stub.RefreshSession(cntl, &request, response, nullptr);
}

void MDSBaseClient::GetLatestTxId(const GetLatestTxIdRequest& request,
                                  GetLatestTxIdResponse* response,
                                  brpc::Controller* cntl,
                                  brpc::Channel* channel) {
  dingofs::pb::mds::MdsService_Stub stub(channel);
  stub.GetLatestTxId(cntl, &request, response, nullptr);
}

void MDSBaseClient::CommitTx(const CommitTxRequest& request,
                             CommitTxResponse* response, brpc::Controller* cntl,
                             brpc::Channel* channel) {
  dingofs::pb::mds::MdsService_Stub stub(channel);
  stub.CommitTx(cntl, &request, response, nullptr);
}

void MDSBaseClient::AllocOrGetMemcacheCluster(
    uint32_t fsId, AllocOrGetMemcacheClusterResponse* response,
    brpc::Controller* cntl, brpc::Channel* channel) {
  AllocOrGetMemcacheClusterRequest request;
  request.set_fsid(fsId);

  dingofs::pb::mds::topology::TopologyService_Stub stub(channel);
  stub.AllocOrGetMemcacheCluster(cntl, &request, response, nullptr);
}

void MDSBaseClient::SetFsStats(const SetFsStatsRequest& request,
                               SetFsStatsResponse* response,
                               brpc::Controller* cntl, brpc::Channel* channel) {
  dingofs::pb::mds::MdsService_Stub stub(channel);
  stub.SetFsStats(cntl, &request, response, nullptr);
}

// cache group
void MDSBaseClient::JoinCacheGroup(
    const std::string& group_name, const std::string& ip, uint32_t port,
    uint32_t weight, uint64_t replace_id,
    pb::mds::cachegroup::JoinCacheGroupResponse* response,
    brpc::Controller* cntl, brpc::Channel* channel) {
  JoinCacheGroupRequest request;
  request.set_group_name(group_name);
  request.set_ip(ip);
  request.set_port(port);
  request.set_weight(weight);
  request.set_replace_id(replace_id);

  CacheGroupMemberService_Stub stub(channel);
  stub.JoinCacheGroup(cntl, &request, response, nullptr);
}

void MDSBaseClient::LeaveCacheGroup(
    const std::string& group_name, const std::string& ip, uint32_t port,
    pb::mds::cachegroup::LeaveCacheGroupResponse* response,
    brpc::Controller* cntl, brpc::Channel* channel) {
  LeaveCacheGroupRequest request;
  request.set_group_name(group_name);
  request.set_ip(ip);
  request.set_port(port);

  CacheGroupMemberService_Stub stub(channel);
  stub.LeaveCacheGroup(cntl, &request, response, nullptr);
}

void MDSBaseClient::SendCacheGroupHeartbeat(
    const std::string& ip, uint32_t port,
    pb::mds::cachegroup::HeartbeatResponse* response, brpc::Controller* cntl,
    brpc::Channel* channel) {
  HeartbeatRequest request;
  request.set_ip(ip);
  request.set_port(port);

  CacheGroupMemberService_Stub stub(channel);
  stub.Heartbeat(cntl, &request, response, nullptr);
}

void MDSBaseClient::LoadCacheGroupMembers(const std::string& group_name,
                                          LoadMembersResponse* response,
                                          brpc::Controller* cntl,
                                          brpc::Channel* channel) {
  LoadMembersRequest request;
  request.set_group_name(group_name);

  CacheGroupMemberService_Stub stub(channel);
  stub.LoadMembers(cntl, &request, response, nullptr);
}

void MDSBaseClient::ReweightCacheGroupMember(uint64_t member_id,
                                             uint32_t weight,
                                             ReweightMemberResponse* response,
                                             brpc::Controller* cntl,
                                             brpc::Channel* channel) {
  ReweightMemberRequest request;
  request.set_member_id(member_id);
  request.set_weight(weight);

  CacheGroupMemberService_Stub stub(channel);
  stub.ReweightMember(cntl, &request, response, nullptr);
}

}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs
