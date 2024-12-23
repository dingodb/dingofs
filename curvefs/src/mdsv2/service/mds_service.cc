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

#include "curvefs/src/mdsv2/service/mds_service.h"

namespace dingofs {

namespace mdsv2 {

MDSServiceImpl::MDSServiceImpl() = default;

// fs interface
void MDSServiceImpl::CreateFs(google::protobuf::RpcController* controller, const pb::mds::CreateFsRequest* request,
                              pb::mds::CreateFsResponse* response, google::protobuf::Closure* done) {}

void MDSServiceImpl::MountFs(google::protobuf::RpcController* controller, const pb::mds::MountFsRequest* request,
                             pb::mds::MountFsResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::UmountFs(google::protobuf::RpcController* controller, const pb::mds::UmountFsRequest* request,
                              pb::mds::UmountFsResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::DeleteFs(google::protobuf::RpcController* controller, const pb::mds::DeleteFsRequest* request,
                              pb::mds::DeleteFsResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::GetFsInfo(google::protobuf::RpcController* controller, const pb::mds::GetFsInfoRequest* request,
                               pb::mds::GetFsInfoResponse* response, google::protobuf::Closure* done) {}

// dentry interface
void MDSServiceImpl::CreateDentry(google::protobuf::RpcController* controller,
                                  const pb::mds::CreateDentryRequest* request, pb::mds::CreateDentryResponse* response,
                                  google::protobuf::Closure* done) {}
void MDSServiceImpl::DeleteDentry(google::protobuf::RpcController* controller,
                                  const pb::mds::DeleteDentryRequest* request, pb::mds::DeleteDentryResponse* response,
                                  google::protobuf::Closure* done) {}
void MDSServiceImpl::GetDentry(google::protobuf::RpcController* controller, const pb::mds::GetDentryRequest* request,
                               pb::mds::GetDentryResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::ListDentry(google::protobuf::RpcController* controller, const pb::mds::ListDentryRequest* request,
                                pb::mds::ListDentryResponse* response, google::protobuf::Closure* done) {}

// inode interface
void MDSServiceImpl::CreateInode(google::protobuf::RpcController* controller,
                                 const pb::mds::CreateInodeRequest* request, pb::mds::CreateInodeResponse* response,
                                 google::protobuf::Closure* done) {}

void MDSServiceImpl::CreateRootInode(google::protobuf::RpcController* controller,
                                     const pb::mds::CreateRootInodeRequest* request,
                                     pb::mds::CreateRootInodeResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::DeleteInode(google::protobuf::RpcController* controller,
                                 const pb::mds::DeleteInodeRequest* request, pb::mds::DeleteInodeResponse* response,
                                 google::protobuf::Closure* done) {}
void MDSServiceImpl::UpdateInode(google::protobuf::RpcController* controller,
                                 const pb::mds::UpdateInodeRequest* request, pb::mds::UpdateInodeResponse* response,
                                 google::protobuf::Closure* done) {}
void MDSServiceImpl::UpdateS3Chunk(google::protobuf::RpcController* controller,
                                   const pb::mds::UpdateS3ChunkRequest* request,
                                   pb::mds::UpdateS3ChunkResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::GetInode(google::protobuf::RpcController* controller, const pb::mds::GetInodeRequest* request,
                              pb::mds::GetInodeResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::BatchGetXAttr(google::protobuf::RpcController* controller,
                                   const pb::mds::BatchGetXAttrRequest* request,
                                   pb::mds::BatchGetXAttrResponse* response, google::protobuf::Closure* done) {}

// high level interface
void MDSServiceImpl::MkNod(google::protobuf::RpcController* controller, const pb::mds::MkNodRequest* request,
                           pb::mds::MkNodResponse* response, google::protobuf::Closure* done) {}

void MDSServiceImpl::MkDir(google::protobuf::RpcController* controller, const pb::mds::MkDirRequest* request,
                           pb::mds::MkDirResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::RmDir(google::protobuf::RpcController* controller, const pb::mds::RmDirRequest* request,
                           pb::mds::RmDirResponse* response, google::protobuf::Closure* done) {}

void MDSServiceImpl::Link(google::protobuf::RpcController* controller, const pb::mds::LinkRequest* request,
                          pb::mds::LinkResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::UnLink(google::protobuf::RpcController* controller, const pb::mds::UnLinkRequest* request,
                            pb::mds::UnLinkResponse* response, google::protobuf::Closure* done) {}

void MDSServiceImpl::Symlink(google::protobuf::RpcController* controller, const pb::mds::SymlinkRequest* request,
                             pb::mds::SymlinkResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::ReadLink(google::protobuf::RpcController* controller, const pb::mds::ReadLinkRequest* request,
                              pb::mds::ReadLinkResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::Rename(google::protobuf::RpcController* controller, const pb::mds::RenameRequest* request,
                            pb::mds::RenameResponse* response, google::protobuf::Closure* done) {}

// quota interface
void MDSServiceImpl::SetFsQuota(google::protobuf::RpcController* controller, const pb::mds::SetFsQuotaRequest* request,
                                pb::mds::SetFsQuotaResponse* response, google::protobuf::Closure* done) {}

void MDSServiceImpl::GetFsQuota(google::protobuf::RpcController* controller, const pb::mds::GetFsQuotaRequest* request,
                                pb::mds::GetFsQuotaResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::FlushFsUsage(google::protobuf::RpcController* controller,
                                  const pb::mds::FlushFsUsageRequest* request, pb::mds::FlushFsUsageResponse* response,
                                  google::protobuf::Closure* done) {}

void MDSServiceImpl::SetDirQuota(google::protobuf::RpcController* controller,
                                 const pb::mds::SetDirQuotaRequest* request, pb::mds::SetDirQuotaResponse* response,
                                 google::protobuf::Closure* done) {}

void MDSServiceImpl::GetDirQuota(google::protobuf::RpcController* controller,
                                 const pb::mds::GetDirQuotaRequest* request, pb::mds::GetDirQuotaResponse* response,
                                 google::protobuf::Closure* done) {}
void MDSServiceImpl::DeleteDirQuota(google::protobuf::RpcController* controller,
                                    const pb::mds::DeleteDirQuotaRequest* request,
                                    pb::mds::DeleteDirQuotaResponse* response, google::protobuf::Closure* done) {}

void MDSServiceImpl::LoadDirQuotas(google::protobuf::RpcController* controller,
                                   const pb::mds::LoadDirQuotasRequest* request,
                                   pb::mds::LoadDirQuotasResponse* response, google::protobuf::Closure* done) {}
void MDSServiceImpl::FlushDirUsages(google::protobuf::RpcController* controller,
                                    const pb::mds::FlushDirUsagesRequest* request,
                                    pb::mds::FlushDirUsagesResponse* response, google::protobuf::Closure* done) {}

}  // namespace mdsv2

}  // namespace dingofs