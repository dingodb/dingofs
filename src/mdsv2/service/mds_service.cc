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

#include "mdsv2/service/mds_service.h"

#include <cstdint>
#include <string>

#include "brpc/controller.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/filesystem.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/server.h"
#include "mdsv2/service/service_helper.h"

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

  auto& mds_meta = Server::GetInstance().GetMDSMeta();

  FileSystemSet::CreateFsParam param;
  param.mds_id = mds_meta.ID();
  param.fs_name = request->fs_name();
  param.block_size = request->block_size();
  param.fs_type = request->fs_type();
  param.fs_extra = request->fs_extra();
  param.enable_sum_in_dir = request->enable_sum_in_dir();
  param.owner = request->owner();
  param.capacity = request->capacity();
  param.recycle_time_hour = request->recycle_time_hour();
  param.partition_type = request->partition_type();

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
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
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

void MDSServiceImpl::DoRefreshFsInfo(google::protobuf::RpcController* controller,
                                     const pb::mdsv2::RefreshFsInfoRequest* request,
                                     pb::mdsv2::RefreshFsInfoResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto status = file_system_set_->RefreshFsInfo(request->fs_name());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::RefreshFsInfo(google::protobuf::RpcController* controller,
                                   const pb::mdsv2::RefreshFsInfoRequest* request,
                                   pb::mdsv2::RefreshFsInfoResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoRefreshFsInfo(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

// dentry interface
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

  EntryOut entry_out;
  auto status = file_system->Lookup(request->parent_ino(), request->name(), entry_out);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.inode);
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

  FileSystem::MkNodParam param;
  param.parent_ino = request->parent_ino();
  param.name = request->name();
  param.mode = request->mode();
  param.uid = request->uid();
  param.gid = request->gid();
  param.rdev = request->rdev();

  EntryOut entry_out;
  auto status = file_system->MkNod(param, entry_out);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.inode);
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

  FileSystem::MkDirParam param;
  param.parent_ino = request->parent_ino();
  param.name = request->name();
  param.mode = request->mode();
  param.uid = request->uid();
  param.gid = request->gid();
  param.rdev = request->rdev();

  EntryOut entry_out;
  auto status = file_system->MkDir(param, entry_out);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.inode);
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

  auto status = file_system->RmDir(request->parent_ino(), request->name());
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

void MDSServiceImpl::DoReadDir(google::protobuf::RpcController* controller, const pb::mdsv2::ReadDirRequest* request,
                               pb::mdsv2::ReadDirResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  std::vector<EntryOut> entry_outs;
  auto status =
      file_system->ReadDir(request->ino(), request->last_name(), request->limit(), request->with_attr(), entry_outs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (auto& entry_out : entry_outs) {
    auto* mut_entry = response->add_entries();
    mut_entry->set_name(entry_out.name);
    mut_entry->set_ino(entry_out.inode.ino());
    if (request->with_attr()) {
      mut_entry->mutable_inode()->Swap(&entry_out.inode);
    }
  }
}

void MDSServiceImpl::ReadDir(google::protobuf::RpcController* controller, const pb::mdsv2::ReadDirRequest* request,
                             pb::mdsv2::ReadDirResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoReadDir(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoOpen(google::protobuf::RpcController* controller, const pb::mdsv2::OpenRequest* request,
                            pb::mdsv2::OpenResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = file_system->Open(request->ino());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::Open(google::protobuf::RpcController* controller, const pb::mdsv2::OpenRequest* request,
                          pb::mdsv2::OpenResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoOpen(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoRelease(google::protobuf::RpcController* controller, const pb::mdsv2::ReleaseRequest* request,
                               pb::mdsv2::ReleaseResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = file_system->Release(request->ino());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::Release(google::protobuf::RpcController* controller, const pb::mdsv2::ReleaseRequest* request,
                             pb::mdsv2::ReleaseResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoRelease(controller, request, response, svr_done); });

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

  EntryOut entry_out;
  auto status = file_system->Link(request->ino(), request->new_parent_ino(), request->new_name(), entry_out);
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.inode);
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

  auto status = file_system->UnLink(request->parent_ino(), request->name());
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

  EntryOut entry_out;
  auto status = file_system->Symlink(request->symlink(), request->new_parent_ino(), request->new_name(), request->uid(),
                                     request->gid(), entry_out);
  ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());

  response->mutable_inode()->Swap(&entry_out.inode);
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
  auto status = file_system->ReadLink(request->ino(), symlink);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
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

  EntryOut entry_out;
  auto status = file_system->GetAttr(request->ino(), entry_out);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.inode);
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

  FileSystem::SetAttrParam param;
  auto& inode = param.inode;
  inode.set_fs_id(request->fs_id());
  inode.set_ino(request->ino());
  inode.set_length(request->length());
  inode.set_ctime(request->ctime());
  inode.set_mtime(request->mtime());
  inode.set_atime(request->atime());
  inode.set_uid(request->uid());
  inode.set_gid(request->gid());
  inode.set_mode(request->mode());
  param.to_set = request->to_set();

  EntryOut entry_out;
  auto status = file_system->SetAttr(request->ino(), param, entry_out);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.inode);
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
  auto status = file_system->GetXAttr(request->ino(), request->name(), value);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
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
  auto status = file_system->SetXAttr(request->ino(), xattrs);
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
  auto status = file_system->GetXAttr(request->ino(), xattrs);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
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

  done_guard.release();
  auto status = file_system->AsyncRename(request->old_parent_ino(), request->old_name(), request->new_parent_ino(),
                                         request->new_name(), [&](Status status) {
                                           brpc::ClosureGuard done_guard(done);
                                           return ServiceHelper::SetError(response->mutable_error(),
                                                                          status.error_code(), status.error_str());
                                         });

  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
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

void MDSServiceImpl::DoAllocSliceId(google::protobuf::RpcController* controller,
                                    const pb::mdsv2::AllocSliceIdRequest* request,
                                    pb::mdsv2::AllocSliceIdResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
}

void MDSServiceImpl::AllocSliceId(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::AllocSliceIdRequest* request,
                                  pb::mdsv2::AllocSliceIdResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoAllocSliceId(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoWriteSlice(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::WriteSliceRequest* request, pb::mdsv2::WriteSliceResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = file_system->WriteSlice(request->ino(), request->chunk_index(), request->slice_list());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::WriteSlice(google::protobuf::RpcController* controller,
                                const pb::mdsv2::WriteSliceRequest* request, pb::mdsv2::WriteSliceResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoWriteSlice(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoReadSlice(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::ReadSliceRequest* request, pb::mdsv2::ReadSliceResponse* response,
                                 google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  pb::mdsv2::SliceList slice_list;
  auto status = file_system->ReadSlice(request->ino(), request->chunk_index(), slice_list);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_slice_list()->Swap(&slice_list);
}

void MDSServiceImpl::ReadSlice(google::protobuf::RpcController* controller, const pb::mdsv2::ReadSliceRequest* request,
                               pb::mdsv2::ReadSliceResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoReadSlice(controller, request, response, svr_done); });

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