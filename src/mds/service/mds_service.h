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

#ifndef DINGOFS_MDS_SERVICE_MDS_H_
#define DINGOFS_MDS_SERVICE_MDS_H_

#include <cstdint>

#include "dingofs/mds.pb.h"
#include "mds/cachegroup/member_manager.h"
#include "mds/common/runnable.h"
#include "mds/filesystem/filesystem.h"
#include "mds/service/service_helper.h"
#include "mds/statistics/fs_stat.h"

namespace dingofs {
namespace mds {

class MDSServiceImpl;
using MDSServiceImplUPtr = std::unique_ptr<MDSServiceImpl>;

class MDSServiceImpl : public pb::mds::MDSService {
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

  void Echo(google::protobuf::RpcController* controller, const pb::mds::EchoRequest* request,
            pb::mds::EchoResponse* response, google::protobuf::Closure* done) override;

  // mds
  void Heartbeat(google::protobuf::RpcController* controller, const pb::mds::HeartbeatRequest* request,
                 pb::mds::HeartbeatResponse* response, google::protobuf::Closure* done) override;
  void GetMDSList(google::protobuf::RpcController* controller, const pb::mds::GetMDSListRequest* request,
                  pb::mds::GetMDSListResponse* response, google::protobuf::Closure* done) override;

  // fs interface
  void CreateFs(google::protobuf::RpcController* controller, const pb::mds::CreateFsRequest* request,
                pb::mds::CreateFsResponse* response, google::protobuf::Closure* done) override;
  void MountFs(google::protobuf::RpcController* controller, const pb::mds::MountFsRequest* request,
               pb::mds::MountFsResponse* response, google::protobuf::Closure* done) override;
  void UmountFs(google::protobuf::RpcController* controller, const pb::mds::UmountFsRequest* request,
                pb::mds::UmountFsResponse* response, google::protobuf::Closure* done) override;
  void DeleteFs(google::protobuf::RpcController* controller, const pb::mds::DeleteFsRequest* request,
                pb::mds::DeleteFsResponse* response, google::protobuf::Closure* done) override;
  void GetFsInfo(google::protobuf::RpcController* controller, const pb::mds::GetFsInfoRequest* request,
                 pb::mds::GetFsInfoResponse* response, google::protobuf::Closure* done) override;
  void ListFsInfo(google::protobuf::RpcController* controller, const pb::mds::ListFsInfoRequest* request,
                  pb::mds::ListFsInfoResponse* response, google::protobuf::Closure* done) override;
  void UpdateFsInfo(google::protobuf::RpcController* controller, const pb::mds::UpdateFsInfoRequest* request,
                    pb::mds::UpdateFsInfoResponse* response, google::protobuf::Closure* done) override;

  // dentry interface
  void GetDentry(google::protobuf::RpcController* controller, const pb::mds::GetDentryRequest* request,
                 pb::mds::GetDentryResponse* response, google::protobuf::Closure* done) override;
  void ListDentry(google::protobuf::RpcController* controller, const pb::mds::ListDentryRequest* request,
                  pb::mds::ListDentryResponse* response, google::protobuf::Closure* done) override;

  // inode interface
  void GetInode(google::protobuf::RpcController* controller, const pb::mds::GetInodeRequest* request,
                pb::mds::GetInodeResponse* response, google::protobuf::Closure* done) override;
  void BatchGetInode(google::protobuf::RpcController* controller, const pb::mds::BatchGetInodeRequest* request,
                     pb::mds::BatchGetInodeResponse* response, google::protobuf::Closure* done) override;
  void BatchGetXAttr(google::protobuf::RpcController* controller, const pb::mds::BatchGetXAttrRequest* request,
                     pb::mds::BatchGetXAttrResponse* response, google::protobuf::Closure* done) override;

  // high level interface
  void Lookup(google::protobuf::RpcController* controller, const pb::mds::LookupRequest* request,
              pb::mds::LookupResponse* response, google::protobuf::Closure* done) override;

  void BatchCreate(google::protobuf::RpcController* controller, const pb::mds::BatchCreateRequest* request,
                   pb::mds::BatchCreateResponse* response, google::protobuf::Closure* done) override;

  void MkNod(google::protobuf::RpcController* controller, const pb::mds::MkNodRequest* request,
             pb::mds::MkNodResponse* response, google::protobuf::Closure* done) override;

  void MkDir(google::protobuf::RpcController* controller, const pb::mds::MkDirRequest* request,
             pb::mds::MkDirResponse* response, google::protobuf::Closure* done) override;
  void RmDir(google::protobuf::RpcController* controller, const pb::mds::RmDirRequest* request,
             pb::mds::RmDirResponse* response, google::protobuf::Closure* done) override;
  void ReadDir(google::protobuf::RpcController* controller, const pb::mds::ReadDirRequest* request,
               pb::mds::ReadDirResponse* response, google::protobuf::Closure* done) override;

  void Open(google::protobuf::RpcController* controller, const pb::mds::OpenRequest* request,
            pb::mds::OpenResponse* response, google::protobuf::Closure* done) override;

  void Release(google::protobuf::RpcController* controller, const pb::mds::ReleaseRequest* request,
               pb::mds::ReleaseResponse* response, google::protobuf::Closure* done) override;

  void Link(google::protobuf::RpcController* controller, const pb::mds::LinkRequest* request,
            pb::mds::LinkResponse* response, google::protobuf::Closure* done) override;
  void UnLink(google::protobuf::RpcController* controller, const pb::mds::UnLinkRequest* request,
              pb::mds::UnLinkResponse* response, google::protobuf::Closure* done) override;

  void Symlink(google::protobuf::RpcController* controller, const pb::mds::SymlinkRequest* request,
               pb::mds::SymlinkResponse* response, google::protobuf::Closure* done) override;
  void ReadLink(google::protobuf::RpcController* controller, const pb::mds::ReadLinkRequest* request,
                pb::mds::ReadLinkResponse* response, google::protobuf::Closure* done) override;

  void GetAttr(google::protobuf::RpcController* controller, const pb::mds::GetAttrRequest* request,
               pb::mds::GetAttrResponse* response, google::protobuf::Closure* done) override;

  void SetAttr(google::protobuf::RpcController* controller, const pb::mds::SetAttrRequest* request,
               pb::mds::SetAttrResponse* response, google::protobuf::Closure* done) override;

  void GetXAttr(google::protobuf::RpcController* controller, const pb::mds::GetXAttrRequest* request,
                pb::mds::GetXAttrResponse* response, google::protobuf::Closure* done) override;

  void SetXAttr(google::protobuf::RpcController* controller, const pb::mds::SetXAttrRequest* request,
                pb::mds::SetXAttrResponse* response, google::protobuf::Closure* done) override;

  void RemoveXAttr(google::protobuf::RpcController* controller, const pb::mds::RemoveXAttrRequest* request,
                   pb::mds::RemoveXAttrResponse* response, google::protobuf::Closure* done) override;

  void ListXAttr(google::protobuf::RpcController* controller, const pb::mds::ListXAttrRequest* request,
                 pb::mds::ListXAttrResponse* response, google::protobuf::Closure* done) override;

  void Rename(google::protobuf::RpcController* controller, const pb::mds::RenameRequest* request,
              pb::mds::RenameResponse* response, google::protobuf::Closure* done) override;

  // slice
  void AllocSliceId(google::protobuf::RpcController* controller, const pb::mds::AllocSliceIdRequest* request,
                    pb::mds::AllocSliceIdResponse* response, google::protobuf::Closure* done) override;

  void WriteSlice(google::protobuf::RpcController* controller, const pb::mds::WriteSliceRequest* request,
                  pb::mds::WriteSliceResponse* response, google::protobuf::Closure* done) override;

  void ReadSlice(google::protobuf::RpcController* controller, const pb::mds::ReadSliceRequest* request,
                 pb::mds::ReadSliceResponse* response, google::protobuf::Closure* done) override;

  // fallocate
  void Fallocate(google::protobuf::RpcController* controller, const pb::mds::FallocateRequest* request,
                 pb::mds::FallocateResponse* response, google::protobuf::Closure* done) override;

  // compact interface
  void CompactChunk(google::protobuf::RpcController* controller, const pb::mds::CompactChunkRequest* request,
                    pb::mds::CompactChunkResponse* response, google::protobuf::Closure* done) override;
  void CleanTrashSlice(google::protobuf::RpcController* controller, const pb::mds::CleanTrashSliceRequest* request,
                       pb::mds::CleanTrashSliceResponse* response, google::protobuf::Closure* done) override;
  void CleanDelFile(google::protobuf::RpcController* controller, const pb::mds::CleanDelFileRequest* request,
                    pb::mds::CleanDelFileResponse* response, google::protobuf::Closure* done) override;

  // quota interface
  void SetFsQuota(google::protobuf::RpcController* controller, const pb::mds::SetFsQuotaRequest* request,
                  pb::mds::SetFsQuotaResponse* response, google::protobuf::Closure* done) override;

  void GetFsQuota(google::protobuf::RpcController* controller, const pb::mds::GetFsQuotaRequest* request,
                  pb::mds::GetFsQuotaResponse* response, google::protobuf::Closure* done) override;

  void SetDirQuota(google::protobuf::RpcController* controller, const pb::mds::SetDirQuotaRequest* request,
                   pb::mds::SetDirQuotaResponse* response, google::protobuf::Closure* done) override;
  void GetDirQuota(google::protobuf::RpcController* controller, const pb::mds::GetDirQuotaRequest* request,
                   pb::mds::GetDirQuotaResponse* response, google::protobuf::Closure* done) override;
  void DeleteDirQuota(google::protobuf::RpcController* controller, const pb::mds::DeleteDirQuotaRequest* request,
                      pb::mds::DeleteDirQuotaResponse* response, google::protobuf::Closure* done) override;

  void LoadDirQuotas(google::protobuf::RpcController* controller, const pb::mds::LoadDirQuotasRequest* request,
                     pb::mds::LoadDirQuotasResponse* response, google::protobuf::Closure* done) override;

  // fs statistics
  void SetFsStats(google::protobuf::RpcController* controller, const pb::mds::SetFsStatsRequest* request,
                  pb::mds::SetFsStatsResponse* response, google::protobuf::Closure* done) override;
  void GetFsStats(google::protobuf::RpcController* controller, const pb::mds::GetFsStatsRequest* request,
                  pb::mds::GetFsStatsResponse* response, google::protobuf::Closure* done) override;
  void GetFsPerSecondStats(google::protobuf::RpcController* controller,
                           const pb::mds::GetFsPerSecondStatsRequest* request,
                           pb::mds::GetFsPerSecondStatsResponse* response, google::protobuf::Closure* done) override;

  // internal interface
  void CheckAlive(google::protobuf::RpcController* controller, const pb::mds::CheckAliveRequest* request,
                  pb::mds::CheckAliveResponse* response, google::protobuf::Closure* done) override;

  void NotifyBuddy(google::protobuf::RpcController* controller, const pb::mds::NotifyBuddyRequest* request,
                   pb::mds::NotifyBuddyResponse* response, google::protobuf::Closure* done) override;

  void JoinFs(google::protobuf::RpcController* controller, const pb::mds::JoinFsRequest* request,
              pb::mds::JoinFsResponse* response, google::protobuf::Closure* done) override;

  void QuitFs(google::protobuf::RpcController* controller, const pb::mds::QuitFsRequest* request,
              pb::mds::QuitFsResponse* response, google::protobuf::Closure* done) override;

  void StopMds(google::protobuf::RpcController* controller, const pb::mds::StopMdsRequest* request,
               pb::mds::StopMdsResponse* response, google::protobuf::Closure* done) override;

  // cache group member interface
  void JoinCacheGroup(google::protobuf::RpcController* controller, const pb::mds::JoinCacheGroupRequest* request,
                      pb::mds::JoinCacheGroupResponse* response, google::protobuf::Closure* done) override;

  void LeaveCacheGroup(google::protobuf::RpcController* controller, const pb::mds::LeaveCacheGroupRequest* request,
                       pb::mds::LeaveCacheGroupResponse* response, google::protobuf::Closure* done) override;

  void ListGroups(google::protobuf::RpcController* controller, const pb::mds::ListGroupsRequest* request,
                  pb::mds::ListGroupsResponse* response, google::protobuf::Closure* done) override;

  void ReweightMember(google::protobuf::RpcController* controller, const pb::mds::ReweightMemberRequest* request,
                      pb::mds::ReweightMemberResponse* response, google::protobuf::Closure* done) override;

  void ListMembers(google::protobuf::RpcController* controller, const pb::mds::ListMembersRequest* request,
                   pb::mds::ListMembersResponse* response, google::protobuf::Closure* done) override;

  void UnlockMember(google::protobuf::RpcController* controller, const pb::mds::UnLockMemberRequest* request,
                    pb::mds::UnLockMemberResponse* response, google::protobuf::Closure* done) override;

  void DeleteMember(google::protobuf::RpcController* controller, const pb::mds::DeleteMemberRequest* request,
                    pb::mds::DeleteMemberResponse* response, google::protobuf::Closure* done) override;

  void DescribeByJson(Json::Value& value);

 private:
  friend class DebugServiceImpl;
  Status GenFsId(int64_t& fs_id);

  inline FileSystemSPtr GetFileSystem(uint32_t fs_id);

  WorkerSetUPtr& GetReadWorkerSet() { return read_worker_set_; }
  WorkerSetUPtr& GetWriteWorkerSet() { return write_worker_set_; }

  // mds
  static void DoHeartbeat(google::protobuf::RpcController* controller, const pb::mds::HeartbeatRequest* request,
                          pb::mds::HeartbeatResponse* response, TraceClosure* done);
  static void DoGetMDSList(google::protobuf::RpcController* controller, const pb::mds::GetMDSListRequest* request,
                           pb::mds::GetMDSListResponse* response, TraceClosure* done);

  // fs interface
  void DoCreateFs(google::protobuf::RpcController* controller, const pb::mds::CreateFsRequest* request,
                  pb::mds::CreateFsResponse* response, TraceClosure* done);
  void DoMountFs(google::protobuf::RpcController* controller, const pb::mds::MountFsRequest* request,
                 pb::mds::MountFsResponse* response, TraceClosure* done);
  void DoUmountFs(google::protobuf::RpcController* controller, const pb::mds::UmountFsRequest* request,
                  pb::mds::UmountFsResponse* response, TraceClosure* done);
  void DoDeleteFs(google::protobuf::RpcController* controller, const pb::mds::DeleteFsRequest* request,
                  pb::mds::DeleteFsResponse* response, TraceClosure* done);
  void DoGetFsInfo(google::protobuf::RpcController* controller, const pb::mds::GetFsInfoRequest* request,
                   pb::mds::GetFsInfoResponse* response, TraceClosure* done);
  void DoListFsInfo(google::protobuf::RpcController* controller, const pb::mds::ListFsInfoRequest* request,
                    pb::mds::ListFsInfoResponse* response, TraceClosure* done);
  void DoUpdateFsInfo(google::protobuf::RpcController* controller, const pb::mds::UpdateFsInfoRequest* request,
                      pb::mds::UpdateFsInfoResponse* response, TraceClosure* done);

  // dentry interface
  void DoGetDentry(google::protobuf::RpcController* controller, const pb::mds::GetDentryRequest* request,
                   pb::mds::GetDentryResponse* response, TraceClosure* done);
  void DoListDentry(google::protobuf::RpcController* controller, const pb::mds::ListDentryRequest* request,
                    pb::mds::ListDentryResponse* response, TraceClosure* done);

  // inode interface
  void DoGetInode(google::protobuf::RpcController* controller, const pb::mds::GetInodeRequest* request,
                  pb::mds::GetInodeResponse* response, TraceClosure* done);
  void DoBatchGetInode(google::protobuf::RpcController* controller, const pb::mds::BatchGetInodeRequest* request,
                       pb::mds::BatchGetInodeResponse* response, TraceClosure* done);
  void DoBatchGetXAttr(google::protobuf::RpcController* controller, const pb::mds::BatchGetXAttrRequest* request,
                       pb::mds::BatchGetXAttrResponse* response, TraceClosure* done);

  // high level interface
  void DoLookup(google::protobuf::RpcController* controller, const pb::mds::LookupRequest* request,
                pb::mds::LookupResponse* response, TraceClosure* done);

  void DoBatchCreate(google::protobuf::RpcController* controller, const pb::mds::BatchCreateRequest* request,
                     pb::mds::BatchCreateResponse* response, TraceClosure* done);
  void DoMkNod(google::protobuf::RpcController* controller, const pb::mds::MkNodRequest* request,
               pb::mds::MkNodResponse* response, TraceClosure* done);
  void DoMkDir(google::protobuf::RpcController* controller, const pb::mds::MkDirRequest* request,
               pb::mds::MkDirResponse* response, TraceClosure* done);
  void DoRmDir(google::protobuf::RpcController* controller, const pb::mds::RmDirRequest* request,
               pb::mds::RmDirResponse* response, TraceClosure* done);
  void DoReadDir(google::protobuf::RpcController* controller, const pb::mds::ReadDirRequest* request,
                 pb::mds::ReadDirResponse* response, TraceClosure* done);

  void DoOpen(google::protobuf::RpcController* controller, const pb::mds::OpenRequest* request,
              pb::mds::OpenResponse* response, TraceClosure* done);
  void DoRelease(google::protobuf::RpcController* controller, const pb::mds::ReleaseRequest* request,
                 pb::mds::ReleaseResponse* response, TraceClosure* done);

  void DoLink(google::protobuf::RpcController* controller, const pb::mds::LinkRequest* request,
              pb::mds::LinkResponse* response, TraceClosure* done);
  void DoUnLink(google::protobuf::RpcController* controller, const pb::mds::UnLinkRequest* request,
                pb::mds::UnLinkResponse* response, TraceClosure* done);
  void DoSymlink(google::protobuf::RpcController* controller, const pb::mds::SymlinkRequest* request,
                 pb::mds::SymlinkResponse* response, TraceClosure* done);
  void DoReadLink(google::protobuf::RpcController* controller, const pb::mds::ReadLinkRequest* request,
                  pb::mds::ReadLinkResponse* response, TraceClosure* done);

  void DoGetAttr(google::protobuf::RpcController* controller, const pb::mds::GetAttrRequest* request,
                 pb::mds::GetAttrResponse* response, TraceClosure* done);

  void DoSetAttr(google::protobuf::RpcController* controller, const pb::mds::SetAttrRequest* request,
                 pb::mds::SetAttrResponse* response, TraceClosure* done);

  void DoGetXAttr(google::protobuf::RpcController* controller, const pb::mds::GetXAttrRequest* request,
                  pb::mds::GetXAttrResponse* response, TraceClosure* done);

  void DoSetXAttr(google::protobuf::RpcController* controller, const pb::mds::SetXAttrRequest* request,
                  pb::mds::SetXAttrResponse* response, TraceClosure* done);

  void DoRemoveXAttr(google::protobuf::RpcController* controller, const pb::mds::RemoveXAttrRequest* request,
                     pb::mds::RemoveXAttrResponse* response, TraceClosure* done);

  void DoListXAttr(google::protobuf::RpcController* controller, const pb::mds::ListXAttrRequest* request,
                   pb::mds::ListXAttrResponse* response, TraceClosure* done);

  void DoRename(google::protobuf::RpcController* controller, const pb::mds::RenameRequest* request,
                pb::mds::RenameResponse* response, TraceClosure* done);

  void DoAllocSliceId(google::protobuf::RpcController* controller, const pb::mds::AllocSliceIdRequest* request,
                      pb::mds::AllocSliceIdResponse* response, TraceClosure* done);
  void DoWriteSlice(google::protobuf::RpcController* controller, const pb::mds::WriteSliceRequest* request,
                    pb::mds::WriteSliceResponse* response, TraceClosure* done);
  void DoReadSlice(google::protobuf::RpcController* controller, const pb::mds::ReadSliceRequest* request,
                   pb::mds::ReadSliceResponse* response, TraceClosure* done);

  void DoFallocate(google::protobuf::RpcController* controller, const pb::mds::FallocateRequest* request,
                   pb::mds::FallocateResponse* response, TraceClosure* done);

  void DoCompactChunk(google::protobuf::RpcController* controller, const pb::mds::CompactChunkRequest* request,
                      pb::mds::CompactChunkResponse* response, TraceClosure* done);
  void DoCleanTrashSlice(google::protobuf::RpcController* controller, const pb::mds::CleanTrashSliceRequest* request,
                         pb::mds::CleanTrashSliceResponse* response, TraceClosure* done);
  void DoCleanDelFile(google::protobuf::RpcController* controller, const pb::mds::CleanDelFileRequest* request,
                      pb::mds::CleanDelFileResponse* response, TraceClosure* done);

  void DoNotifyBuddy(google::protobuf::RpcController* controller, const pb::mds::NotifyBuddyRequest* request,
                     pb::mds::NotifyBuddyResponse* response, TraceClosure* done);

  // quota interface
  void DoSetFsQuota(google::protobuf::RpcController* controller, const pb::mds::SetFsQuotaRequest* request,
                    pb::mds::SetFsQuotaResponse* response, TraceClosure* done);
  void DoGetFsQuota(google::protobuf::RpcController* controller, const pb::mds::GetFsQuotaRequest* request,
                    pb::mds::GetFsQuotaResponse* response, TraceClosure* done);
  void DoSetDirQuota(google::protobuf::RpcController* controller, const pb::mds::SetDirQuotaRequest* request,
                     pb::mds::SetDirQuotaResponse* response, TraceClosure* done);
  void DoGetDirQuota(google::protobuf::RpcController* controller, const pb::mds::GetDirQuotaRequest* request,
                     pb::mds::GetDirQuotaResponse* response, TraceClosure* done);
  void DoDeleteDirQuota(google::protobuf::RpcController* controller, const pb::mds::DeleteDirQuotaRequest* request,
                        pb::mds::DeleteDirQuotaResponse* response, TraceClosure* done);
  void DoLoadDirQuotas(google::protobuf::RpcController* controller, const pb::mds::LoadDirQuotasRequest* request,
                       pb::mds::LoadDirQuotasResponse* response, TraceClosure* done);

  // fs statistics
  void DoSetFsStats(google::protobuf::RpcController* controller, const pb::mds::SetFsStatsRequest* request,
                    pb::mds::SetFsStatsResponse* response, TraceClosure* done);
  void DoGetFsStats(google::protobuf::RpcController* controller, const pb::mds::GetFsStatsRequest* request,
                    pb::mds::GetFsStatsResponse* response, TraceClosure* done);
  void DoGetFsPerSecondStats(google::protobuf::RpcController* controller,
                             const pb::mds::GetFsPerSecondStatsRequest* request,
                             pb::mds::GetFsPerSecondStatsResponse* response, TraceClosure* done);

  // cache group member interface
  void DoJoinCacheGroup(google::protobuf::RpcController* controller, const pb::mds::JoinCacheGroupRequest* request,
                        pb::mds::JoinCacheGroupResponse* response, TraceClosure* done);

  void DoLeaveCacheGroup(google::protobuf::RpcController* controller, const pb::mds::LeaveCacheGroupRequest* request,
                         pb::mds::LeaveCacheGroupResponse* response, TraceClosure* done);

  void DoListGroups(google::protobuf::RpcController* controller, const pb::mds::ListGroupsRequest* request,
                    pb::mds::ListGroupsResponse* response, TraceClosure* done);

  void DoReweightMember(google::protobuf::RpcController* controller, const pb::mds::ReweightMemberRequest* request,
                        pb::mds::ReweightMemberResponse* response, TraceClosure* done);

  void DoListMembers(google::protobuf::RpcController* controller, const pb::mds::ListMembersRequest* request,
                     pb::mds::ListMembersResponse* response, TraceClosure* done);

  void DoUnlockMember(google::protobuf::RpcController* controller, const pb::mds::UnLockMemberRequest* request,
                      pb::mds::UnLockMemberResponse* response, TraceClosure* done);

  void DoDeleteMember(google::protobuf::RpcController* controller, const pb::mds::DeleteMemberRequest* request,
                      pb::mds::DeleteMemberResponse* response, TraceClosure* done);

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

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_SERVICE_MDS_H_