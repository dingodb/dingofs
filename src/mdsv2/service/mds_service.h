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

#ifndef DINGOFS_MDSV2_SERVICE_MDS_H_
#define DINGOFS_MDSV2_SERVICE_MDS_H_

#include <cstdint>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/cachegroup/member_manager.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/filesystem/filesystem.h"
#include "mdsv2/service/service_helper.h"
#include "mdsv2/statistics/fs_stat.h"

namespace dingofs {
namespace mdsv2 {

class MDSServiceImpl;
using MDSServiceImplUPtr = std::unique_ptr<MDSServiceImpl>;

class MDSServiceImpl : public pb::mdsv2::MDSService {
 public:
  MDSServiceImpl(FileSystemSetSPtr file_system, GcProcessorSPtr gc_processor, FsStatsUPtr fs_stat,
                 CacheGroupMemberManagerSPtr cache_group_manager);
  ~MDSServiceImpl() override = default;

  MDSServiceImpl(const MDSServiceImpl&) = delete;
  MDSServiceImpl& operator=(const MDSServiceImpl&) = delete;

  static MDSServiceImplUPtr New(FileSystemSetSPtr file_system, GcProcessorSPtr gc_processor, FsStatsUPtr fs_stat,
                                CacheGroupMemberManagerSPtr cache_group_manager) {
    return std::make_unique<MDSServiceImpl>(file_system, gc_processor, std::move(fs_stat), cache_group_manager);
  }

  bool Init();
  void Destroy();

  void Echo(google::protobuf::RpcController* controller, const pb::mdsv2::EchoRequest* request,
            pb::mdsv2::EchoResponse* response, google::protobuf::Closure* done) override;

  // mds
  void Heartbeat(google::protobuf::RpcController* controller, const pb::mdsv2::HeartbeatRequest* request,
                 pb::mdsv2::HeartbeatResponse* response, google::protobuf::Closure* done) override;
  void GetMDSList(google::protobuf::RpcController* controller, const pb::mdsv2::GetMDSListRequest* request,
                  pb::mdsv2::GetMDSListResponse* response, google::protobuf::Closure* done) override;

  // fs interface
  void CreateFs(google::protobuf::RpcController* controller, const pb::mdsv2::CreateFsRequest* request,
                pb::mdsv2::CreateFsResponse* response, google::protobuf::Closure* done) override;
  void MountFs(google::protobuf::RpcController* controller, const pb::mdsv2::MountFsRequest* request,
               pb::mdsv2::MountFsResponse* response, google::protobuf::Closure* done) override;
  void UmountFs(google::protobuf::RpcController* controller, const pb::mdsv2::UmountFsRequest* request,
                pb::mdsv2::UmountFsResponse* response, google::protobuf::Closure* done) override;
  void DeleteFs(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteFsRequest* request,
                pb::mdsv2::DeleteFsResponse* response, google::protobuf::Closure* done) override;
  void GetFsInfo(google::protobuf::RpcController* controller, const pb::mdsv2::GetFsInfoRequest* request,
                 pb::mdsv2::GetFsInfoResponse* response, google::protobuf::Closure* done) override;
  void ListFsInfo(google::protobuf::RpcController* controller, const pb::mdsv2::ListFsInfoRequest* request,
                  pb::mdsv2::ListFsInfoResponse* response, google::protobuf::Closure* done) override;
  void UpdateFsInfo(google::protobuf::RpcController* controller, const pb::mdsv2::UpdateFsInfoRequest* request,
                    pb::mdsv2::UpdateFsInfoResponse* response, google::protobuf::Closure* done) override;

  // dentry interface
  void GetDentry(google::protobuf::RpcController* controller, const pb::mdsv2::GetDentryRequest* request,
                 pb::mdsv2::GetDentryResponse* response, google::protobuf::Closure* done) override;
  void ListDentry(google::protobuf::RpcController* controller, const pb::mdsv2::ListDentryRequest* request,
                  pb::mdsv2::ListDentryResponse* response, google::protobuf::Closure* done) override;

  // inode interface
  void GetInode(google::protobuf::RpcController* controller, const pb::mdsv2::GetInodeRequest* request,
                pb::mdsv2::GetInodeResponse* response, google::protobuf::Closure* done) override;
  void BatchGetInode(google::protobuf::RpcController* controller, const pb::mdsv2::BatchGetInodeRequest* request,
                     pb::mdsv2::BatchGetInodeResponse* response, google::protobuf::Closure* done) override;
  void BatchGetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::BatchGetXAttrRequest* request,
                     pb::mdsv2::BatchGetXAttrResponse* response, google::protobuf::Closure* done) override;

  // high level interface
  void Lookup(google::protobuf::RpcController* controller, const pb::mdsv2::LookupRequest* request,
              pb::mdsv2::LookupResponse* response, google::protobuf::Closure* done) override;

  void MkNod(google::protobuf::RpcController* controller, const pb::mdsv2::MkNodRequest* request,
             pb::mdsv2::MkNodResponse* response, google::protobuf::Closure* done) override;

  void MkDir(google::protobuf::RpcController* controller, const pb::mdsv2::MkDirRequest* request,
             pb::mdsv2::MkDirResponse* response, google::protobuf::Closure* done) override;
  void RmDir(google::protobuf::RpcController* controller, const pb::mdsv2::RmDirRequest* request,
             pb::mdsv2::RmDirResponse* response, google::protobuf::Closure* done) override;
  void ReadDir(google::protobuf::RpcController* controller, const pb::mdsv2::ReadDirRequest* request,
               pb::mdsv2::ReadDirResponse* response, google::protobuf::Closure* done) override;

  void Open(google::protobuf::RpcController* controller, const pb::mdsv2::OpenRequest* request,
            pb::mdsv2::OpenResponse* response, google::protobuf::Closure* done) override;

  void Release(google::protobuf::RpcController* controller, const pb::mdsv2::ReleaseRequest* request,
               pb::mdsv2::ReleaseResponse* response, google::protobuf::Closure* done) override;

  void Link(google::protobuf::RpcController* controller, const pb::mdsv2::LinkRequest* request,
            pb::mdsv2::LinkResponse* response, google::protobuf::Closure* done) override;
  void UnLink(google::protobuf::RpcController* controller, const pb::mdsv2::UnLinkRequest* request,
              pb::mdsv2::UnLinkResponse* response, google::protobuf::Closure* done) override;

  void Symlink(google::protobuf::RpcController* controller, const pb::mdsv2::SymlinkRequest* request,
               pb::mdsv2::SymlinkResponse* response, google::protobuf::Closure* done) override;
  void ReadLink(google::protobuf::RpcController* controller, const pb::mdsv2::ReadLinkRequest* request,
                pb::mdsv2::ReadLinkResponse* response, google::protobuf::Closure* done) override;

  void GetAttr(google::protobuf::RpcController* controller, const pb::mdsv2::GetAttrRequest* request,
               pb::mdsv2::GetAttrResponse* response, google::protobuf::Closure* done) override;

  void SetAttr(google::protobuf::RpcController* controller, const pb::mdsv2::SetAttrRequest* request,
               pb::mdsv2::SetAttrResponse* response, google::protobuf::Closure* done) override;

  void GetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::GetXAttrRequest* request,
                pb::mdsv2::GetXAttrResponse* response, google::protobuf::Closure* done) override;

  void SetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::SetXAttrRequest* request,
                pb::mdsv2::SetXAttrResponse* response, google::protobuf::Closure* done) override;

  void RemoveXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::RemoveXAttrRequest* request,
                   pb::mdsv2::RemoveXAttrResponse* response, google::protobuf::Closure* done) override;

  void ListXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::ListXAttrRequest* request,
                 pb::mdsv2::ListXAttrResponse* response, google::protobuf::Closure* done) override;

  void Rename(google::protobuf::RpcController* controller, const pb::mdsv2::RenameRequest* request,
              pb::mdsv2::RenameResponse* response, google::protobuf::Closure* done) override;

  // slice
  void AllocSliceId(google::protobuf::RpcController* controller, const pb::mdsv2::AllocSliceIdRequest* request,
                    pb::mdsv2::AllocSliceIdResponse* response, google::protobuf::Closure* done) override;

  void WriteSlice(google::protobuf::RpcController* controller, const pb::mdsv2::WriteSliceRequest* request,
                  pb::mdsv2::WriteSliceResponse* response, google::protobuf::Closure* done) override;

  void ReadSlice(google::protobuf::RpcController* controller, const pb::mdsv2::ReadSliceRequest* request,
                 pb::mdsv2::ReadSliceResponse* response, google::protobuf::Closure* done) override;

  // fallocate
  void Fallocate(google::protobuf::RpcController* controller, const pb::mdsv2::FallocateRequest* request,
                 pb::mdsv2::FallocateResponse* response, google::protobuf::Closure* done) override;

  // compact interface
  void CompactChunk(google::protobuf::RpcController* controller, const pb::mdsv2::CompactChunkRequest* request,
                    pb::mdsv2::CompactChunkResponse* response, google::protobuf::Closure* done) override;
  void CleanTrashSlice(google::protobuf::RpcController* controller, const pb::mdsv2::CleanTrashSliceRequest* request,
                       pb::mdsv2::CleanTrashSliceResponse* response, google::protobuf::Closure* done) override;
  void CleanDelFile(google::protobuf::RpcController* controller, const pb::mdsv2::CleanDelFileRequest* request,
                    pb::mdsv2::CleanDelFileResponse* response, google::protobuf::Closure* done) override;

  // quota interface
  void SetFsQuota(google::protobuf::RpcController* controller, const pb::mdsv2::SetFsQuotaRequest* request,
                  pb::mdsv2::SetFsQuotaResponse* response, google::protobuf::Closure* done) override;

  void GetFsQuota(google::protobuf::RpcController* controller, const pb::mdsv2::GetFsQuotaRequest* request,
                  pb::mdsv2::GetFsQuotaResponse* response, google::protobuf::Closure* done) override;

  void SetDirQuota(google::protobuf::RpcController* controller, const pb::mdsv2::SetDirQuotaRequest* request,
                   pb::mdsv2::SetDirQuotaResponse* response, google::protobuf::Closure* done) override;
  void GetDirQuota(google::protobuf::RpcController* controller, const pb::mdsv2::GetDirQuotaRequest* request,
                   pb::mdsv2::GetDirQuotaResponse* response, google::protobuf::Closure* done) override;
  void DeleteDirQuota(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteDirQuotaRequest* request,
                      pb::mdsv2::DeleteDirQuotaResponse* response, google::protobuf::Closure* done) override;

  void LoadDirQuotas(google::protobuf::RpcController* controller, const pb::mdsv2::LoadDirQuotasRequest* request,
                     pb::mdsv2::LoadDirQuotasResponse* response, google::protobuf::Closure* done) override;

  // fs statistics
  void SetFsStats(google::protobuf::RpcController* controller, const pb::mdsv2::SetFsStatsRequest* request,
                  pb::mdsv2::SetFsStatsResponse* response, google::protobuf::Closure* done) override;
  void GetFsStats(google::protobuf::RpcController* controller, const pb::mdsv2::GetFsStatsRequest* request,
                  pb::mdsv2::GetFsStatsResponse* response, google::protobuf::Closure* done) override;
  void GetFsPerSecondStats(google::protobuf::RpcController* controller,
                           const pb::mdsv2::GetFsPerSecondStatsRequest* request,
                           pb::mdsv2::GetFsPerSecondStatsResponse* response, google::protobuf::Closure* done) override;

  // internal interface
  void CheckAlive(google::protobuf::RpcController* controller, const pb::mdsv2::CheckAliveRequest* request,
                  pb::mdsv2::CheckAliveResponse* response, google::protobuf::Closure* done) override;

  void NotifyBuddy(google::protobuf::RpcController* controller, const pb::mdsv2::NotifyBuddyRequest* request,
                   pb::mdsv2::NotifyBuddyResponse* response, google::protobuf::Closure* done) override;

  void JoinFs(google::protobuf::RpcController* controller, const pb::mdsv2::JoinFsRequest* request,
              pb::mdsv2::JoinFsResponse* response, google::protobuf::Closure* done) override;

  void QuitFs(google::protobuf::RpcController* controller, const pb::mdsv2::QuitFsRequest* request,
              pb::mdsv2::QuitFsResponse* response, google::protobuf::Closure* done) override;

  void StopMds(google::protobuf::RpcController* controller, const pb::mdsv2::StopMdsRequest* request,
               pb::mdsv2::StopMdsResponse* response, google::protobuf::Closure* done) override;

  // cache group member interface
  void JoinCacheGroup(google::protobuf::RpcController* controller, const pb::mdsv2::JoinCacheGroupRequest* request,
                      pb::mdsv2::JoinCacheGroupResponse* response, google::protobuf::Closure* done) override;

  void LeaveCacheGroup(google::protobuf::RpcController* controller, const pb::mdsv2::LeaveCacheGroupRequest* request,
                       pb::mdsv2::LeaveCacheGroupResponse* response, google::protobuf::Closure* done) override;

  void ListGroups(google::protobuf::RpcController* controller, const pb::mdsv2::ListGroupsRequest* request,
                  pb::mdsv2::ListGroupsResponse* response, google::protobuf::Closure* done) override;

  void ReweightMember(google::protobuf::RpcController* controller, const pb::mdsv2::ReweightMemberRequest* request,
                      pb::mdsv2::ReweightMemberResponse* response, google::protobuf::Closure* done) override;

  void ListMembers(google::protobuf::RpcController* controller, const pb::mdsv2::ListMembersRequest* request,
                   pb::mdsv2::ListMembersResponse* response, google::protobuf::Closure* done) override;

  void UnlockMember(google::protobuf::RpcController* controller, const pb::mdsv2::UnLockMemberRequest* request,
                    pb::mdsv2::UnLockMemberResponse* response, google::protobuf::Closure* done) override;

  void DeleteMember(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteMemberRequest* request,
                    pb::mdsv2::DeleteMemberResponse* response, google::protobuf::Closure* done) override;

  void DescribeByJson(Json::Value& value);

 private:
  friend class DebugServiceImpl;
  Status GenFsId(int64_t& fs_id);
  inline FileSystemSPtr GetFileSystem(uint32_t fs_id);

  WorkerSetUPtr& GetReadWorkerSet() { return read_worker_set_; }
  WorkerSetUPtr& GetWriteWorkerSet() { return write_worker_set_; }

  // mds
  static void DoHeartbeat(google::protobuf::RpcController* controller, const pb::mdsv2::HeartbeatRequest* request,
                          pb::mdsv2::HeartbeatResponse* response, TraceClosure* done);
  static void DoGetMDSList(google::protobuf::RpcController* controller, const pb::mdsv2::GetMDSListRequest* request,
                           pb::mdsv2::GetMDSListResponse* response, TraceClosure* done);

  // fs interface
  void DoCreateFs(google::protobuf::RpcController* controller, const pb::mdsv2::CreateFsRequest* request,
                  pb::mdsv2::CreateFsResponse* response, TraceClosure* done);
  void DoMountFs(google::protobuf::RpcController* controller, const pb::mdsv2::MountFsRequest* request,
                 pb::mdsv2::MountFsResponse* response, TraceClosure* done);
  void DoUmountFs(google::protobuf::RpcController* controller, const pb::mdsv2::UmountFsRequest* request,
                  pb::mdsv2::UmountFsResponse* response, TraceClosure* done);
  void DoDeleteFs(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteFsRequest* request,
                  pb::mdsv2::DeleteFsResponse* response, TraceClosure* done);
  void DoGetFsInfo(google::protobuf::RpcController* controller, const pb::mdsv2::GetFsInfoRequest* request,
                   pb::mdsv2::GetFsInfoResponse* response, TraceClosure* done);
  void DoListFsInfo(google::protobuf::RpcController* controller, const pb::mdsv2::ListFsInfoRequest* request,
                    pb::mdsv2::ListFsInfoResponse* response, google::protobuf::Closure* done);
  void DoUpdateFsInfo(google::protobuf::RpcController* controller, const pb::mdsv2::UpdateFsInfoRequest* request,
                      pb::mdsv2::UpdateFsInfoResponse* response, google::protobuf::Closure* done);

  // dentry interface
  void DoGetDentry(google::protobuf::RpcController* controller, const pb::mdsv2::GetDentryRequest* request,
                   pb::mdsv2::GetDentryResponse* response, google::protobuf::Closure* done);
  void DoListDentry(google::protobuf::RpcController* controller, const pb::mdsv2::ListDentryRequest* request,
                    pb::mdsv2::ListDentryResponse* response, google::protobuf::Closure* done);

  // inode interface
  void DoGetInode(google::protobuf::RpcController* controller, const pb::mdsv2::GetInodeRequest* request,
                  pb::mdsv2::GetInodeResponse* response, google::protobuf::Closure* done);
  void DoBatchGetInode(google::protobuf::RpcController* controller, const pb::mdsv2::BatchGetInodeRequest* request,
                       pb::mdsv2::BatchGetInodeResponse* response, google::protobuf::Closure* done);
  void DoBatchGetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::BatchGetXAttrRequest* request,
                       pb::mdsv2::BatchGetXAttrResponse* response, google::protobuf::Closure* done);

  // high level interface
  void DoLookup(google::protobuf::RpcController* controller, const pb::mdsv2::LookupRequest* request,
                pb::mdsv2::LookupResponse* response, TraceClosure* done);
  void DoMkNod(google::protobuf::RpcController* controller, const pb::mdsv2::MkNodRequest* request,
               pb::mdsv2::MkNodResponse* response, TraceClosure* done);
  void DoMkDir(google::protobuf::RpcController* controller, const pb::mdsv2::MkDirRequest* request,
               pb::mdsv2::MkDirResponse* response, TraceClosure* done);
  void DoRmDir(google::protobuf::RpcController* controller, const pb::mdsv2::RmDirRequest* request,
               pb::mdsv2::RmDirResponse* response, TraceClosure* done);
  void DoReadDir(google::protobuf::RpcController* controller, const pb::mdsv2::ReadDirRequest* request,
                 pb::mdsv2::ReadDirResponse* response, TraceClosure* done);

  void DoOpen(google::protobuf::RpcController* controller, const pb::mdsv2::OpenRequest* request,
              pb::mdsv2::OpenResponse* response, TraceClosure* done);
  void DoRelease(google::protobuf::RpcController* controller, const pb::mdsv2::ReleaseRequest* request,
                 pb::mdsv2::ReleaseResponse* response, TraceClosure* done);

  void DoLink(google::protobuf::RpcController* controller, const pb::mdsv2::LinkRequest* request,
              pb::mdsv2::LinkResponse* response, TraceClosure* done);
  void DoUnLink(google::protobuf::RpcController* controller, const pb::mdsv2::UnLinkRequest* request,
                pb::mdsv2::UnLinkResponse* response, TraceClosure* done);
  void DoSymlink(google::protobuf::RpcController* controller, const pb::mdsv2::SymlinkRequest* request,
                 pb::mdsv2::SymlinkResponse* response, TraceClosure* done);
  void DoReadLink(google::protobuf::RpcController* controller, const pb::mdsv2::ReadLinkRequest* request,
                  pb::mdsv2::ReadLinkResponse* response, TraceClosure* done);

  void DoGetAttr(google::protobuf::RpcController* controller, const pb::mdsv2::GetAttrRequest* request,
                 pb::mdsv2::GetAttrResponse* response, TraceClosure* done);

  void DoSetAttr(google::protobuf::RpcController* controller, const pb::mdsv2::SetAttrRequest* request,
                 pb::mdsv2::SetAttrResponse* response, TraceClosure* done);

  void DoGetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::GetXAttrRequest* request,
                  pb::mdsv2::GetXAttrResponse* response, TraceClosure* done);

  void DoSetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::SetXAttrRequest* request,
                  pb::mdsv2::SetXAttrResponse* response, TraceClosure* done);

  void DoRemoveXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::RemoveXAttrRequest* request,
                     pb::mdsv2::RemoveXAttrResponse* response, TraceClosure* done);

  void DoListXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::ListXAttrRequest* request,
                   pb::mdsv2::ListXAttrResponse* response, TraceClosure* done);

  void DoRename(google::protobuf::RpcController* controller, const pb::mdsv2::RenameRequest* request,
                pb::mdsv2::RenameResponse* response, TraceClosure* done);

  void DoAllocSliceId(google::protobuf::RpcController* controller, const pb::mdsv2::AllocSliceIdRequest* request,
                      pb::mdsv2::AllocSliceIdResponse* response, TraceClosure* done);
  void DoWriteSlice(google::protobuf::RpcController* controller, const pb::mdsv2::WriteSliceRequest* request,
                    pb::mdsv2::WriteSliceResponse* response, TraceClosure* done);
  void DoReadSlice(google::protobuf::RpcController* controller, const pb::mdsv2::ReadSliceRequest* request,
                   pb::mdsv2::ReadSliceResponse* response, TraceClosure* done);

  void DoFallocate(google::protobuf::RpcController* controller, const pb::mdsv2::FallocateRequest* request,
                   pb::mdsv2::FallocateResponse* response, TraceClosure* done);

  void DoCompactChunk(google::protobuf::RpcController* controller, const pb::mdsv2::CompactChunkRequest* request,
                      pb::mdsv2::CompactChunkResponse* response, TraceClosure* done);
  void DoCleanTrashSlice(google::protobuf::RpcController* controller, const pb::mdsv2::CleanTrashSliceRequest* request,
                         pb::mdsv2::CleanTrashSliceResponse* response, TraceClosure* done);
  void DoCleanDelFile(google::protobuf::RpcController* controller, const pb::mdsv2::CleanDelFileRequest* request,
                      pb::mdsv2::CleanDelFileResponse* response, TraceClosure* done);

  void DoNotifyBuddy(google::protobuf::RpcController* controller, const pb::mdsv2::NotifyBuddyRequest* request,
                     pb::mdsv2::NotifyBuddyResponse* response, TraceClosure* done);

  // quota interface
  void DoSetFsQuota(google::protobuf::RpcController* controller, const pb::mdsv2::SetFsQuotaRequest* request,
                    pb::mdsv2::SetFsQuotaResponse* response, TraceClosure* done);
  void DoGetFsQuota(google::protobuf::RpcController* controller, const pb::mdsv2::GetFsQuotaRequest* request,
                    pb::mdsv2::GetFsQuotaResponse* response, TraceClosure* done);
  void DoSetDirQuota(google::protobuf::RpcController* controller, const pb::mdsv2::SetDirQuotaRequest* request,
                     pb::mdsv2::SetDirQuotaResponse* response, TraceClosure* done);
  void DoGetDirQuota(google::protobuf::RpcController* controller, const pb::mdsv2::GetDirQuotaRequest* request,
                     pb::mdsv2::GetDirQuotaResponse* response, TraceClosure* done);
  void DoDeleteDirQuota(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteDirQuotaRequest* request,
                        pb::mdsv2::DeleteDirQuotaResponse* response, TraceClosure* done);
  void DoLoadDirQuotas(google::protobuf::RpcController* controller, const pb::mdsv2::LoadDirQuotasRequest* request,
                       pb::mdsv2::LoadDirQuotasResponse* response, TraceClosure* done);

  // fs statistics
  void DoSetFsStats(google::protobuf::RpcController* controller, const pb::mdsv2::SetFsStatsRequest* request,
                    pb::mdsv2::SetFsStatsResponse* response, TraceClosure* done);
  void DoGetFsStats(google::protobuf::RpcController* controller, const pb::mdsv2::GetFsStatsRequest* request,
                    pb::mdsv2::GetFsStatsResponse* response, TraceClosure* done);
  void DoGetFsPerSecondStats(google::protobuf::RpcController* controller,
                             const pb::mdsv2::GetFsPerSecondStatsRequest* request,
                             pb::mdsv2::GetFsPerSecondStatsResponse* response, TraceClosure* done);

  // cache group member interface
  void DoJoinCacheGroup(google::protobuf::RpcController* controller, const pb::mdsv2::JoinCacheGroupRequest* request,
                        pb::mdsv2::JoinCacheGroupResponse* response, TraceClosure* done);

  void DoLeaveCacheGroup(google::protobuf::RpcController* controller, const pb::mdsv2::LeaveCacheGroupRequest* request,
                         pb::mdsv2::LeaveCacheGroupResponse* response, TraceClosure* done);

  void DoListGroups(google::protobuf::RpcController* controller, const pb::mdsv2::ListGroupsRequest* request,
                    pb::mdsv2::ListGroupsResponse* response, TraceClosure* done);

  void DoReweightMember(google::protobuf::RpcController* controller, const pb::mdsv2::ReweightMemberRequest* request,
                        pb::mdsv2::ReweightMemberResponse* response, TraceClosure* done);

  void DoListMembers(google::protobuf::RpcController* controller, const pb::mdsv2::ListMembersRequest* request,
                     pb::mdsv2::ListMembersResponse* response, TraceClosure* done);

  void DoUnlockMember(google::protobuf::RpcController* controller, const pb::mdsv2::UnLockMemberRequest* request,
                      pb::mdsv2::UnLockMemberResponse* response, TraceClosure* done);

  void DoDeleteMember(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteMemberRequest* request,
                      pb::mdsv2::DeleteMemberResponse* response, TraceClosure* done);

  // file system set
  FileSystemSetSPtr file_system_set_;

  // gc
  GcProcessorSPtr gc_processor_;

  // fs stats
  FsStatsUPtr fs_stat_;

  // cache group member
  CacheGroupMemberManagerSPtr cache_group_manager_;

  // Run service request.
  WorkerSetUPtr read_worker_set_;
  WorkerSetUPtr write_worker_set_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_SERVICE_MDS_H_