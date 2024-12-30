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

#include "dingofs/src/mdsv2/service/mds_service.h"

#include <cstdint>
#include <map>
#include <string>

#include "brpc/controller.h"
#include "dingofs/proto/error.pb.h"
#include "dingofs/proto/mdsv2.pb.h"
#include "dingofs/src/mdsv2/filesystem/filesystem.h"
#include "dingofs/src/mdsv2/filesystem/inode.h"
#include "dingofs/src/mdsv2/service/service_helper.h"

namespace dingofs {

namespace mdsv2 {

MDSServiceImpl::MDSServiceImpl(WorkerSetPtr read_worker_set, WorkerSetPtr write_worker_set,
                               FileSystemSetPtr file_system_set)
    : read_worker_set_(read_worker_set), write_worker_set_(write_worker_set), file_system_set_(file_system_set) {}

FileSystemPtr MDSServiceImpl::GetFileSystem(uint32_t fs_id) { return file_system_set_->GetFileSystem(fs_id); }

static Status ValidateCreateFsRequest(const pb::mdsv2::CreateFsRequest* request) {
  if (request->fs_name().empty()) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "fs name is empty");
  }

  return Status::OK();
}

void MDSServiceImpl::DoCreateFs(google::protobuf::RpcController* controller, const pb::mdsv2::CreateFsRequest* request,
                                pb::mdsv2::CreateFsResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  FileSystemSet::CreateFsParam param;
  param.fs_name = request->fs_name();
  param.block_size = request->block_size();
  param.fs_type = request->fs_type();
  param.fs_detail = request->fs_detail();
  param.enable_sum_in_dir = request->enable_sum_in_dir();
  param.owner = request->owner();
  param.capacity = request->capacity();
  param.recycle_time_hour = request->recycle_time_hour();

  int64_t fs_id = 0;
  auto status = file_system_set_->CreateFs(param, fs_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

// fs interface
void MDSServiceImpl::CreateFs(google::protobuf::RpcController* controller, const pb::mdsv2::CreateFsRequest* request,
                              pb::mdsv2::CreateFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  auto status = ValidateCreateFsRequest(request);
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoCreateFs(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoMountFs(google::protobuf::RpcController* controller, const pb::mdsv2::MountFsRequest* request,
                               pb::mdsv2::MountFsResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto status = file_system_set_->MountFs(request->fs_name(), request->mount_point());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::MountFs(google::protobuf::RpcController* controller, const pb::mdsv2::MountFsRequest* request,
                             pb::mdsv2::MountFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoMountFs(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoUmountFs(google::protobuf::RpcController* controller, const pb::mdsv2::UmountFsRequest* request,
                                pb::mdsv2::UmountFsResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto status = file_system_set_->UmountFs(request->fs_name(), request->mount_point());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::UmountFs(google::protobuf::RpcController* controller, const pb::mdsv2::UmountFsRequest* request,
                              pb::mdsv2::UmountFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoUmountFs(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoDeleteFs(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteFsRequest* request,
                                pb::mdsv2::DeleteFsResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto status = file_system_set_->DeleteFs(request->fs_name());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::DeleteFs(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteFsRequest* request,
                              pb::mdsv2::DeleteFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoDeleteFs(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetFsInfo(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::GetFsInfoRequest* request, pb::mdsv2::GetFsInfoResponse* response,
                                 google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  pb::mdsv2::FsInfo fs_info;
  auto status = file_system_set_->GetFsInfo(request->fs_name(), fs_info);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_fs_info()->Swap(&fs_info);
}

void MDSServiceImpl::GetFsInfo(google::protobuf::RpcController* controller, const pb::mdsv2::GetFsInfoRequest* request,
                               pb::mdsv2::GetFsInfoResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetFsInfo(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

// dentry interface
void MDSServiceImpl::CreateDentry(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::CreateDentryRequest* request,
                                  pb::mdsv2::CreateDentryResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

void MDSServiceImpl::DeleteDentry(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::DeleteDentryRequest* request,
                                  pb::mdsv2::DeleteDentryResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}
void MDSServiceImpl::GetDentry(google::protobuf::RpcController* controller, const pb::mdsv2::GetDentryRequest* request,
                               pb::mdsv2::GetDentryResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

void MDSServiceImpl::ListDentry(google::protobuf::RpcController* controller,
                                const pb::mdsv2::ListDentryRequest* request, pb::mdsv2::ListDentryResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

// inode interface
void MDSServiceImpl::CreateInode(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::CreateInodeRequest* request, pb::mdsv2::CreateInodeResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

void MDSServiceImpl::DeleteInode(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::DeleteInodeRequest* request, pb::mdsv2::DeleteInodeResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

void MDSServiceImpl::UpdateInode(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::UpdateInodeRequest* request, pb::mdsv2::UpdateInodeResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

void MDSServiceImpl::UpdateS3Chunk(google::protobuf::RpcController* controller,
                                   const pb::mdsv2::UpdateS3ChunkRequest* request,
                                   pb::mdsv2::UpdateS3ChunkResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

void MDSServiceImpl::GetInode(google::protobuf::RpcController* controller, const pb::mdsv2::GetInodeRequest* request,
                              pb::mdsv2::GetInodeResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

void MDSServiceImpl::BatchGetXAttr(google::protobuf::RpcController* controller,
                                   const pb::mdsv2::BatchGetXAttrRequest* request,
                                   pb::mdsv2::BatchGetXAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

// high level interface

void MDSServiceImpl::DoLookup(google::protobuf::RpcController* controller, const pb::mdsv2::LookupRequest* request,
                              pb::mdsv2::LookupResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  InodePtr inode;
  auto status = file_system->GetInode(request->parent_inode_id(), request->name(), inode);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  ServiceHelper::Inode2PBInode(inode, response->mutable_inode());
}

void MDSServiceImpl::Lookup(google::protobuf::RpcController* controller, const pb::mdsv2::LookupRequest* request,
                            pb::mdsv2::LookupResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoLookup(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoMkNod(google::protobuf::RpcController* controller, const pb::mdsv2::MkNodRequest* request,
                             pb::mdsv2::MkNodResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = file_system->MkNod(request);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::MkNod(google::protobuf::RpcController* controller, const pb::mdsv2::MkNodRequest* request,
                           pb::mdsv2::MkNodResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoMkNod(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoMkDir(google::protobuf::RpcController* controller, const pb::mdsv2::MkDirRequest* request,
                             pb::mdsv2::MkDirResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = file_system->MkDir(request);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::MkDir(google::protobuf::RpcController* controller, const pb::mdsv2::MkDirRequest* request,
                           pb::mdsv2::MkDirResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoMkDir(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoRmDir(google::protobuf::RpcController* controller, const pb::mdsv2::RmDirRequest* request,
                             pb::mdsv2::RmDirResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = file_system->RmDir(request);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::RmDir(google::protobuf::RpcController* controller, const pb::mdsv2::RmDirRequest* request,
                           pb::mdsv2::RmDirResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoRmDir(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoLink(google::protobuf::RpcController* controller, const pb::mdsv2::LinkRequest* request,
                            pb::mdsv2::LinkResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = file_system->Link(request->parent_inode_id(), request->name(), request->inode_id());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::Link(google::protobuf::RpcController* controller, const pb::mdsv2::LinkRequest* request,
                          pb::mdsv2::LinkResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoLink(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoUnLink(google::protobuf::RpcController* controller, const pb::mdsv2::UnLinkRequest* request,
                              pb::mdsv2::UnLinkResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = file_system->UnLink(request->parent_inode_id(), request->name());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::UnLink(google::protobuf::RpcController* controller, const pb::mdsv2::UnLinkRequest* request,
                            pb::mdsv2::UnLinkResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoUnLink(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSymlink(google::protobuf::RpcController* controller, const pb::mdsv2::SymlinkRequest* request,
                               pb::mdsv2::SymlinkResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = file_system->Symlink(request);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::Symlink(google::protobuf::RpcController* controller, const pb::mdsv2::SymlinkRequest* request,
                             pb::mdsv2::SymlinkResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoSymlink(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoReadLink(google::protobuf::RpcController* controller, const pb::mdsv2::ReadLinkRequest* request,
                                pb::mdsv2::ReadLinkResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  std::string symlink;
  auto status = file_system->ReadLink(request->inode_id(), symlink);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_symlink(symlink);
}

void MDSServiceImpl::ReadLink(google::protobuf::RpcController* controller, const pb::mdsv2::ReadLinkRequest* request,
                              pb::mdsv2::ReadLinkResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoReadLink(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetAttr(google::protobuf::RpcController* controller, const pb::mdsv2::GetAttrRequest* request,
                               pb::mdsv2::GetAttrResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  InodePtr inode;
  auto status = file_system->GetInode(request->inode_id(), inode);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  ServiceHelper::Inode2PBInode(inode, response->mutable_inode());
}

void MDSServiceImpl::GetAttr(google::protobuf::RpcController* controller, const pb::mdsv2::GetAttrRequest* request,
                             pb::mdsv2::GetAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetAttr(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSetAttr(google::protobuf::RpcController* controller, const pb::mdsv2::SetAttrRequest* request,
                               pb::mdsv2::SetAttrResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  FileSystem::UpdateInodeParam param;
  param.fs_id = request->fs_id();
  param.ino = request->inode_id();
  param.length = request->length();
  param.ctime = request->ctime();
  param.mtime = request->mtime();
  param.atime = request->atime();
  param.uid = request->uid();
  param.gid = request->gid();
  param.mode = request->mode();
  param.update_fields = ServiceHelper::PbRepeatedToVector(request->update_fields());

  InodePtr inode;
  auto status = file_system->UpdateInode(param, inode);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  ServiceHelper::Inode2PBInode(inode, response->mutable_inode());
}

void MDSServiceImpl::SetAttr(google::protobuf::RpcController* controller, const pb::mdsv2::SetAttrRequest* request,
                             pb::mdsv2::SetAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoSetAttr(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::GetXAttrRequest* request,
                                pb::mdsv2::GetXAttrResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  std::string value;
  auto status = file_system->GetXAttr(request->inode_id(), request->name(), value);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_value(value);
}

void MDSServiceImpl::GetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::GetXAttrRequest* request,
                              pb::mdsv2::GetXAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetXAttr(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::SetXAttrRequest* request,
                                pb::mdsv2::SetXAttrResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  Inode::XAttrMap xattrs;
  ServiceHelper::PbMapToMap(request->xattrs(), xattrs);

  std::string value;
  auto status = file_system->SetXAttr(request->inode_id(), xattrs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::SetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::SetXAttrRequest* request,
                              pb::mdsv2::SetXAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoSetXAttr(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoListXAttr(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::ListXAttrRequest* request, pb::mdsv2::ListXAttrResponse* response,
                                 google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  Inode::XAttrMap xattrs;
  auto status = file_system->GetXAttr(request->inode_id(), xattrs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  ServiceHelper::MapToPbMap(xattrs, response->mutable_xattrs());
}

void MDSServiceImpl::ListXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::ListXAttrRequest* request,
                               pb::mdsv2::ListXAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoListXAttr(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoRename(google::protobuf::RpcController* controller, const pb::mdsv2::RenameRequest* request,
                              pb::mdsv2::RenameResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

void MDSServiceImpl::Rename(google::protobuf::RpcController* controller, const pb::mdsv2::RenameRequest* request,
                            pb::mdsv2::RenameResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoRename(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

// quota interface
void MDSServiceImpl::SetFsQuota(google::protobuf::RpcController* controller,
                                const pb::mdsv2::SetFsQuotaRequest* request, pb::mdsv2::SetFsQuotaResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

void MDSServiceImpl::GetFsQuota(google::protobuf::RpcController* controller,
                                const pb::mdsv2::GetFsQuotaRequest* request, pb::mdsv2::GetFsQuotaResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}
void MDSServiceImpl::FlushFsUsage(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::FlushFsUsageRequest* request,
                                  pb::mdsv2::FlushFsUsageResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

void MDSServiceImpl::SetDirQuota(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::SetDirQuotaRequest* request, pb::mdsv2::SetDirQuotaResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

void MDSServiceImpl::GetDirQuota(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::GetDirQuotaRequest* request, pb::mdsv2::GetDirQuotaResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}
void MDSServiceImpl::DeleteDirQuota(google::protobuf::RpcController* controller,
                                    const pb::mdsv2::DeleteDirQuotaRequest* request,
                                    pb::mdsv2::DeleteDirQuotaResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

void MDSServiceImpl::LoadDirQuotas(google::protobuf::RpcController* controller,
                                   const pb::mdsv2::LoadDirQuotasRequest* request,
                                   pb::mdsv2::LoadDirQuotasResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}
void MDSServiceImpl::FlushDirUsages(google::protobuf::RpcController* controller,
                                    const pb::mdsv2::FlushDirUsagesRequest* request,
                                    pb::mdsv2::FlushDirUsagesResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "not support");
}

}  // namespace mdsv2

}  // namespace dingofs