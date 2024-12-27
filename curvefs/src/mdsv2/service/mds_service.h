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

#ifndef DINGOFS_MDSV2_SERVICE_H_
#define DINGOFS_MDSV2_SERVICE_H_

#include <cstdint>

#include "curvefs/proto/mdsv2.pb.h"
#include "curvefs/src/mdsv2/common/runnable.h"
#include "curvefs/src/mdsv2/filesystem/filesystem.h"

namespace dingofs {

namespace mdsv2 {

class MDSServiceImpl : public pb::mds::MDSService {
 public:
  MDSServiceImpl(WorkerSetPtr read_worker_set, WorkerSetPtr write_worker_set, FileSystemPtr file_system,
                 IdGeneratorPtr id_generator);

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

  // dentry interface
  void CreateDentry(google::protobuf::RpcController* controller, const pb::mds::CreateDentryRequest* request,
                    pb::mds::CreateDentryResponse* response, google::protobuf::Closure* done) override;
  void DeleteDentry(google::protobuf::RpcController* controller, const pb::mds::DeleteDentryRequest* request,
                    pb::mds::DeleteDentryResponse* response, google::protobuf::Closure* done) override;
  void GetDentry(google::protobuf::RpcController* controller, const pb::mds::GetDentryRequest* request,
                 pb::mds::GetDentryResponse* response, google::protobuf::Closure* done) override;
  void ListDentry(google::protobuf::RpcController* controller, const pb::mds::ListDentryRequest* request,
                  pb::mds::ListDentryResponse* response, google::protobuf::Closure* done) override;

  // inode interface
  void CreateInode(google::protobuf::RpcController* controller, const pb::mds::CreateInodeRequest* request,
                   pb::mds::CreateInodeResponse* response, google::protobuf::Closure* done) override;

  void CreateRootInode(google::protobuf::RpcController* controller, const pb::mds::CreateRootInodeRequest* request,
                       pb::mds::CreateRootInodeResponse* response, google::protobuf::Closure* done) override;
  void DeleteInode(google::protobuf::RpcController* controller, const pb::mds::DeleteInodeRequest* request,
                   pb::mds::DeleteInodeResponse* response, google::protobuf::Closure* done) override;
  void UpdateInode(google::protobuf::RpcController* controller, const pb::mds::UpdateInodeRequest* request,
                   pb::mds::UpdateInodeResponse* response, google::protobuf::Closure* done) override;
  void UpdateS3Chunk(google::protobuf::RpcController* controller, const pb::mds::UpdateS3ChunkRequest* request,
                     pb::mds::UpdateS3ChunkResponse* response, google::protobuf::Closure* done) override;
  void GetInode(google::protobuf::RpcController* controller, const pb::mds::GetInodeRequest* request,
                pb::mds::GetInodeResponse* response, google::protobuf::Closure* done) override;
  void BatchGetXAttr(google::protobuf::RpcController* controller, const pb::mds::BatchGetXAttrRequest* request,
                     pb::mds::BatchGetXAttrResponse* response, google::protobuf::Closure* done) override;

  // high level interface
  void MkNod(google::protobuf::RpcController* controller, const pb::mds::MkNodRequest* request,
             pb::mds::MkNodResponse* response, google::protobuf::Closure* done) override;

  void MkDir(google::protobuf::RpcController* controller, const pb::mds::MkDirRequest* request,
             pb::mds::MkDirResponse* response, google::protobuf::Closure* done) override;
  void RmDir(google::protobuf::RpcController* controller, const pb::mds::RmDirRequest* request,
             pb::mds::RmDirResponse* response, google::protobuf::Closure* done) override;

  void Link(google::protobuf::RpcController* controller, const pb::mds::LinkRequest* request,
            pb::mds::LinkResponse* response, google::protobuf::Closure* done) override;
  void UnLink(google::protobuf::RpcController* controller, const pb::mds::UnLinkRequest* request,
              pb::mds::UnLinkResponse* response, google::protobuf::Closure* done) override;

  void Symlink(google::protobuf::RpcController* controller, const pb::mds::SymlinkRequest* request,
               pb::mds::SymlinkResponse* response, google::protobuf::Closure* done) override;
  void ReadLink(google::protobuf::RpcController* controller, const pb::mds::ReadLinkRequest* request,
                pb::mds::ReadLinkResponse* response, google::protobuf::Closure* done) override;
  void Rename(google::protobuf::RpcController* controller, const pb::mds::RenameRequest* request,
              pb::mds::RenameResponse* response, google::protobuf::Closure* done) override;

  // quota interface
  void SetFsQuota(google::protobuf::RpcController* controller, const pb::mds::SetFsQuotaRequest* request,
                  pb::mds::SetFsQuotaResponse* response, google::protobuf::Closure* done) override;

  void GetFsQuota(google::protobuf::RpcController* controller, const pb::mds::GetFsQuotaRequest* request,
                  pb::mds::GetFsQuotaResponse* response, google::protobuf::Closure* done) override;
  void FlushFsUsage(google::protobuf::RpcController* controller, const pb::mds::FlushFsUsageRequest* request,
                    pb::mds::FlushFsUsageResponse* response, google::protobuf::Closure* done) override;

  void SetDirQuota(google::protobuf::RpcController* controller, const pb::mds::SetDirQuotaRequest* request,
                   pb::mds::SetDirQuotaResponse* response, google::protobuf::Closure* done) override;

  void GetDirQuota(google::protobuf::RpcController* controller, const pb::mds::GetDirQuotaRequest* request,
                   pb::mds::GetDirQuotaResponse* response, google::protobuf::Closure* done) override;
  void DeleteDirQuota(google::protobuf::RpcController* controller, const pb::mds::DeleteDirQuotaRequest* request,
                      pb::mds::DeleteDirQuotaResponse* response, google::protobuf::Closure* done) override;

  void LoadDirQuotas(google::protobuf::RpcController* controller, const pb::mds::LoadDirQuotasRequest* request,
                     pb::mds::LoadDirQuotasResponse* response, google::protobuf::Closure* done) override;
  void FlushDirUsages(google::protobuf::RpcController* controller, const pb::mds::FlushDirUsagesRequest* request,
                      pb::mds::FlushDirUsagesResponse* response, google::protobuf::Closure* done) override;

 private:
  Status GenFsId(int64_t& fs_id);

  void DoCreateFs(google::protobuf::RpcController* controller, const pb::mds::CreateFsRequest* request,
                  pb::mds::CreateFsResponse* response, google::protobuf::Closure* done);
  void DoMountFs(google::protobuf::RpcController* controller, const pb::mds::MountFsRequest* request,
                 pb::mds::MountFsResponse* response, google::protobuf::Closure* done);
  void DoUmountFs(google::protobuf::RpcController* controller, const pb::mds::UmountFsRequest* request,
                  pb::mds::UmountFsResponse* response, google::protobuf::Closure* done);
  void DoDeleteFs(google::protobuf::RpcController* controller, const pb::mds::DeleteFsRequest* request,
                  pb::mds::DeleteFsResponse* response, google::protobuf::Closure* done);
  void DoGetFsInfo(google::protobuf::RpcController* controller, const pb::mds::GetFsInfoRequest* request,
                   pb::mds::GetFsInfoResponse* response, google::protobuf::Closure* done);

  IdGeneratorPtr id_generator_;
  FileSystemPtr file_system_;

  // Run service request.
  WorkerSetPtr read_worker_set_;
  WorkerSetPtr write_worker_set_;
};

}  // namespace mdsv2

}  // namespace dingofs

#endif  // DINGOFS_MDSV2_SERVICE_H_