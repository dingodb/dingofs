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
using pb::mds::cachegroup::AddMemberRequest;
using pb::mds::cachegroup::AddMemberResponse;
using pb::mds::cachegroup::CacheGroupMember;
using pb::mds::cachegroup::CacheGroupMemberService_Stub;
using pb::mds::cachegroup::HeartbeatRequest;
using pb::mds::cachegroup::HeartbeatResponse;
using pb::mds::cachegroup::LoadMembersRequest;
using pb::mds::cachegroup::LoadMembersResponse;
using pb::mds::cachegroup::RegisterMemberRequest;
using pb::mds::cachegroup::RegisterMemberResponse;
using pb::mds::cachegroup::ReweightMemberRequest;
using pb::mds::cachegroup::ReweightMemberResponse;
using pb::mds::space::AcquireBlockGroupRequest;
using pb::mds::space::AcquireBlockGroupResponse;
using pb::mds::space::AllocateBlockGroupRequest;
using pb::mds::space::AllocateBlockGroupResponse;
using pb::mds::space::ReleaseBlockGroupRequest;
using pb::mds::space::ReleaseBlockGroupResponse;
using pb::mds::space::SpaceService_Stub;
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

// TODO(all): do we really need pass `fsId` all the time?
//            each dingo-fuse process only mount one filesystem
void MDSBaseClient::AllocateVolumeBlockGroup(
    uint32_t fsId, uint32_t count, const std::string& owner,
    AllocateBlockGroupResponse* response, brpc::Controller* cntl,
    brpc::Channel* channel) {
  AllocateBlockGroupRequest request;
  request.set_fsid(fsId);
  request.set_count(count);
  request.set_owner(owner);

  SpaceService_Stub stub(channel);
  stub.AllocateBlockGroup(cntl, &request, response, nullptr);
}

void MDSBaseClient::AcquireVolumeBlockGroup(uint32_t fsId,
                                            uint64_t blockGroupOffset,
                                            const std::string& owner,
                                            AcquireBlockGroupResponse* response,
                                            brpc::Controller* cntl,
                                            brpc::Channel* channel) {
  AcquireBlockGroupRequest request;
  request.set_fsid(fsId);
  request.set_owner(owner);
  request.set_blockgroupoffset(blockGroupOffset);

  SpaceService_Stub stub(channel);
  stub.AcquireBlockGroup(cntl, &request, response, nullptr);
}

void MDSBaseClient::ReleaseVolumeBlockGroup(
    uint32_t fsId, const std::string& owner,
    const std::vector<dingofs::pb::mds::space::BlockGroup>& blockGroups,
    ReleaseBlockGroupResponse* response, brpc::Controller* cntl,
    brpc::Channel* channel) {
  ReleaseBlockGroupRequest request;
  request.set_fsid(fsId);
  request.set_owner(owner);

  google::protobuf::RepeatedPtrField<dingofs::pb::mds::space::BlockGroup>
      groups(blockGroups.begin(), blockGroups.end());

  request.mutable_blockgroups()->Swap(&groups);

  SpaceService_Stub stub(channel);
  stub.ReleaseBlockGroup(cntl, &request, response, nullptr);
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
void MDSBaseClient::RegisterCacheGroupMember(uint64_t old_id,
                                             RegisterMemberResponse* response,
                                             brpc::Controller* cntl,
                                             brpc::Channel* channel) {
  RegisterMemberRequest request;
  request.set_old_id(old_id);

  CacheGroupMemberService_Stub stub(channel);
  stub.RegisterMember(cntl, &request, response, nullptr);
}

void MDSBaseClient::AddCacheGroupMember(const std::string& group_name,
                                        const CacheGroupMember& member,
                                        AddMemberResponse* response,
                                        brpc::Controller* cntl,
                                        brpc::Channel* channel) {
  AddMemberRequest request;
  request.set_group_name(group_name);
  *request.mutable_member() = member;

  CacheGroupMemberService_Stub stub(channel);
  stub.AddMember(cntl, &request, response, nullptr);
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

void MDSBaseClient::ReweightCacheGroupMember(const std::string& group_name,
                                             uint64_t member_id,
                                             uint32_t weight,
                                             ReweightMemberResponse* response,
                                             brpc::Controller* cntl,
                                             brpc::Channel* channel) {
  ReweightMemberRequest request;
  request.set_group_name(group_name);
  request.set_member_id(member_id);
  request.set_weight(weight);

  CacheGroupMemberService_Stub stub(channel);
  stub.ReweightMember(cntl, &request, response, nullptr);
}

void MDSBaseClient::SendCacheGroupHeartbeat(
    const std::string& group_name, uint64_t member_id,
    const HeartbeatRequest::Statistic& stat, HeartbeatResponse* response,
    brpc::Controller* cntl, brpc::Channel* channel) {
  HeartbeatRequest request;
  request.set_group_name(group_name);
  request.set_member_id(member_id);
  *request.mutable_stat() = stat;

  CacheGroupMemberService_Stub stub(channel);
  stub.Heartbeat(cntl, &request, response, nullptr);
}

}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs
