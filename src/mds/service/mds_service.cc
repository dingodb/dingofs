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

#include "mds/service/mds_service.h"

#include <cstdint>
#include <map>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "brpc/controller.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "mds/common/context.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/common/status.h"
#include "mds/filesystem/filesystem.h"
#include "mds/filesystem/inode.h"
#include "mds/server.h"
#include "mds/service/service_helper.h"
#include "mds/statistics/fs_stat.h"
#include "utils/string.h"

namespace dingofs {
namespace mds {

const std::string kReadWorkerSetName = "READ_WORKER_SET";
const std::string kWriteWorkerSetName = "WRITE_WORKER_SET";
static const int64_t kMaxGroupNameLength = 255;

DEFINE_string(mds_service_worker_set_type, "execq", "worker set type, execq or simple");
DEFINE_validator(mds_service_worker_set_type,
                 [](const char*, const std::string& value) -> bool { return value == "execq" || value == "simple"; });

DEFINE_uint32(mds_service_read_worker_num, 4, "number of read workers");
DEFINE_uint32(mds_service_read_worker_max_pending_num, 1024, "read service worker max pending num");
DEFINE_bool(mds_service_read_worker_use_pthread, false, "read worker use pthread");

DEFINE_uint32(mds_service_write_worker_num, 4, "number of write workers");
DEFINE_uint32(mds_service_write_worker_max_pending_num, 1024, "write service worker max pending num");
DEFINE_bool(mds_service_write_worker_use_pthread, false, "write worker use pthread");

template <typename T>
static Status ValidateRequest(T* request, uint64_t queue_wait_time_us) {
  if (request->info().timeout_ms() != 0 && (queue_wait_time_us / 1000) > request->info().timeout_ms()) {
    return Status(pb::error::ETIMEOUT, "queue wait timeout");
  }

  return Status::OK();
}

template <typename T>
static Status SimpleValidateRequest(FileSystemSPtr& file_system, T* request, uint64_t queue_wait_time_us) {
  if (request->info().timeout_ms() != 0 && (queue_wait_time_us / 1000) > request->info().timeout_ms()) {
    return Status(pb::error::ETIMEOUT, "queue wait timeout");
  }

  if (file_system == nullptr) {
    return Status(pb::error::ENOT_FOUND, "fs not found");
  }

  return Status::OK();
}

template <typename T>
static Status ValidateRequest(FileSystemSPtr& file_system, T* request, uint64_t queue_wait_time_us) {
  if (request->info().timeout_ms() != 0 && (queue_wait_time_us / 1000) > request->info().timeout_ms()) {
    return Status(pb::error::ETIMEOUT, "queue wait timeout");
  }

  if (file_system == nullptr) {
    return Status(pb::error::ENOT_FOUND, "fs not found");
  }

  if (request->context().epoch() < file_system->Epoch()) {
    return Status(pb::error::EROUTER_EPOCH_CHANGE,
                  fmt::format("epoch change, {}<{}", request->context().epoch(), file_system->Epoch()));

  } else if (request->context().epoch() > file_system->Epoch()) {
    return file_system->RefreshFsInfo("request epoch is larger");
  }

  return Status::OK();
}

MDSServiceImpl::MDSServiceImpl(FileSystemSetSPtr file_system_set, GcProcessorSPtr gc_processor, FsStatsUPtr fs_stat,
                               CacheGroupMemberManagerSPtr cache_group_manager)
    : file_system_set_(file_system_set),
      gc_processor_(gc_processor),
      fs_stat_(std::move(fs_stat)),
      cache_group_manager_(cache_group_manager) {}

bool MDSServiceImpl::Init() {
  read_worker_set_ = FLAGS_mds_service_worker_set_type == "simple"
                         ? SimpleWorkerSet::NewUnique(kReadWorkerSetName, FLAGS_mds_service_read_worker_num,
                                                      FLAGS_mds_service_read_worker_max_pending_num,
                                                      FLAGS_mds_service_read_worker_use_pthread, false)
                         : ExecqWorkerSet::NewUnique(kReadWorkerSetName, FLAGS_mds_service_read_worker_num,
                                                     FLAGS_mds_service_read_worker_max_pending_num);
  if (!read_worker_set_->Init()) {
    DINGO_LOG(ERROR) << "init service read worker set fail.";
    return false;
  }

  write_worker_set_ = FLAGS_mds_service_worker_set_type == "simple"
                          ? SimpleWorkerSet::NewUnique(kWriteWorkerSetName, FLAGS_mds_service_write_worker_num,
                                                       FLAGS_mds_service_write_worker_max_pending_num,
                                                       FLAGS_mds_service_write_worker_use_pthread, false)
                          : ExecqWorkerSet::NewUnique(kWriteWorkerSetName, FLAGS_mds_service_write_worker_num,
                                                      FLAGS_mds_service_write_worker_max_pending_num);
  if (!write_worker_set_->Init()) {
    DINGO_LOG(ERROR) << "init service write worker set fail.";
    return false;
  }

  return true;
}

void MDSServiceImpl::Destroy() {
  read_worker_set_->Destroy();
  write_worker_set_->Destroy();
}

FileSystemSPtr MDSServiceImpl::GetFileSystem(uint32_t fs_id) { return file_system_set_->GetFileSystem(fs_id); }

void MDSServiceImpl::Echo(google::protobuf::RpcController*, const pb::mds::EchoRequest* request,
                          pb::mds::EchoResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  response->set_message(request->message());
}

void MDSServiceImpl::DoHeartbeat(google::protobuf::RpcController*, const pb::mds::HeartbeatRequest* request,
                                 pb::mds::HeartbeatResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto status = ValidateRequest(request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  auto heartbeat = Server::GetInstance().GetHeartbeat();

  Context ctx;
  if (request->role() == pb::mds::ROLE_MDS) {
    auto mds = request->mds();
    status = heartbeat->SendHeartbeat(ctx, mds);

  } else if (request->role() == pb::mds::ROLE_CLIENT) {
    auto client = request->client();
    status = heartbeat->SendHeartbeat(ctx, client);

  } else if (request->role() == pb::mds::ROLE_CACHE_MEMBER) {
    auto cache_member = request->cache_group_member();
    status = heartbeat->SendHeartbeat(ctx, cache_member);

  } else {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETER, "role is illegal");
  }

  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::Heartbeat(google::protobuf::RpcController* controller, const pb::mds::HeartbeatRequest* request,
                               pb::mds::HeartbeatResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [controller, request, response, svr_done]() { DoHeartbeat(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetMDSList(google::protobuf::RpcController*, const pb::mds::GetMDSListRequest* request,
                                  pb::mds::GetMDSListResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto status = ValidateRequest(request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  auto heartbeat = Server::GetInstance().GetHeartbeat();

  Context ctx;
  std::vector<MdsEntry> mdses;
  status = heartbeat->GetMDSList(ctx, mdses);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  Helper::VectorToPbRepeated(mdses, response->mutable_mdses());
}

void MDSServiceImpl::GetMDSList(google::protobuf::RpcController* controller, const pb::mds::GetMDSListRequest* request,
                                pb::mds::GetMDSListResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [controller, request, response, svr_done]() { DoGetMDSList(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoCreateFs(google::protobuf::RpcController*, const pb::mds::CreateFsRequest* request,
                                pb::mds::CreateFsResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto status = ValidateRequest(request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  auto& mds_meta = Server::GetInstance().GetMDSMeta();

  FileSystemSet::CreateFsParam param;
  param.mds_id = mds_meta.ID();
  param.fs_id = request->fs_id();
  param.fs_name = request->fs_name();
  param.chunk_size = request->chunk_size();
  param.block_size = request->block_size();
  param.fs_type = request->fs_type();
  param.fs_extra = request->fs_extra();
  param.enable_sum_in_dir = request->enable_sum_in_dir();
  param.owner = request->owner();
  param.capacity = request->capacity();
  param.recycle_time_hour = request->recycle_time_hour();
  param.partition_type = request->partition_type();
  param.expect_mds_num = request->expect_mds_num();
  param.candidate_mds_ids = Helper::PbRepeatedToVector(request->candidate_mds_ids());

  pb::mds::FsInfo fs_info;
  status = file_system_set_->CreateFs(param, fs_info);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  *response->mutable_fs_info() = fs_info;
}

// fs interface
void MDSServiceImpl::CreateFs(google::protobuf::RpcController* controller, const pb::mds::CreateFsRequest* request,
                              pb::mds::CreateFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs name is empty");
    }

    if (request->chunk_size() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "chunk size is zero");
    }

    if (request->block_size() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "block size is zero");
    }

    return Status::OK();
  };

  auto status = validate_fn();
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

void MDSServiceImpl::DoMountFs(google::protobuf::RpcController*, const pb::mds::MountFsRequest* request,
                               pb::mds::MountFsResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto status = ValidateRequest(request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  Context ctx;
  status = file_system_set_->MountFs(ctx, request->fs_name(), request->mount_point());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::MountFs(google::protobuf::RpcController* controller, const pb::mds::MountFsRequest* request,
                             pb::mds::MountFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs name is empty");
    }
    const auto& mount_point = request->mount_point();
    if (mount_point.client_id().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "client_id is empty");
    }
    if (mount_point.hostname().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "hostname is empty");
    }
    if (mount_point.path().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "mount point path is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

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

void MDSServiceImpl::DoUmountFs(google::protobuf::RpcController*, const pb::mds::UmountFsRequest* request,
                                pb::mds::UmountFsResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto status = ValidateRequest(request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  Context ctx;
  status = file_system_set_->UmountFs(ctx, request->fs_name(), request->client_id());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::UmountFs(google::protobuf::RpcController* controller, const pb::mds::UmountFsRequest* request,
                              pb::mds::UmountFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs name is empty");
    }

    if (request->client_id().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "client_id is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

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

void MDSServiceImpl::DoDeleteFs(google::protobuf::RpcController*, const pb::mds::DeleteFsRequest* request,
                                pb::mds::DeleteFsResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto status = ValidateRequest(request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  Context ctx;
  status = file_system_set_->DeleteFs(ctx, request->fs_name(), request->is_force());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::DeleteFs(google::protobuf::RpcController* controller, const pb::mds::DeleteFsRequest* request,
                              pb::mds::DeleteFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs name is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

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

void MDSServiceImpl::DoGetFsInfo(google::protobuf::RpcController*, const pb::mds::GetFsInfoRequest* request,
                                 pb::mds::GetFsInfoResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  std::string fs_name = request->fs_name();
  if (request->fs_id() > 0) {
    auto file_system = GetFileSystem(request->fs_id());
    auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
    if (BAIDU_UNLIKELY(!status.ok())) {
      return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    }

    fs_name = file_system->FsName();
  }

  Context ctx;
  pb::mds::FsInfo fs_info;
  auto status = file_system_set_->GetFsInfo(ctx, fs_name, fs_info);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_fs_info()->Swap(&fs_info);
}

void MDSServiceImpl::GetFsInfo(google::protobuf::RpcController* controller, const pb::mds::GetFsInfoRequest* request,
                               pb::mds::GetFsInfoResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty() && request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_name or fs_id is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

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

void MDSServiceImpl::DoListFsInfo(google::protobuf::RpcController*, const pb::mds::ListFsInfoRequest* request,
                                  pb::mds::ListFsInfoResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto status = ValidateRequest(request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  Context ctx;
  std::vector<pb::mds::FsInfo> fs_infoes;
  status = file_system_set_->GetAllFsInfo(ctx, true, fs_infoes);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  Helper::VectorToPbRepeated(fs_infoes, response->mutable_fs_infos());
}

void MDSServiceImpl::ListFsInfo(google::protobuf::RpcController* controller, const pb::mds::ListFsInfoRequest* request,
                                pb::mds::ListFsInfoResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoListFsInfo(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoUpdateFsInfo(google::protobuf::RpcController*, const pb::mds::UpdateFsInfoRequest* request,
                                    pb::mds::UpdateFsInfoResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto status = ValidateRequest(request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  Context ctx;
  status = file_system_set_->UpdateFsInfo(ctx, request->fs_name(), request->fs_info());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::UpdateFsInfo(google::protobuf::RpcController* controller,
                                  const pb::mds::UpdateFsInfoRequest* request, pb::mds::UpdateFsInfoResponse* response,
                                  google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_name is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoUpdateFsInfo(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetDentry(google::protobuf::RpcController*, const pb::mds::GetDentryRequest* request,
                                 pb::mds::GetDentryResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  Dentry dentry;
  status = file_system->GetDentry(ctx, request->parent(), request->name(), dentry);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_dentry()->CopyFrom(dentry.Copy());
}

// dentry interface
void MDSServiceImpl::GetDentry(google::protobuf::RpcController* controller, const pb::mds::GetDentryRequest* request,
                               pb::mds::GetDentryResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetDentry(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoListDentry(google::protobuf::RpcController*, const pb::mds::ListDentryRequest* request,
                                  pb::mds::ListDentryResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::vector<Dentry> dentries;
  uint32_t limit = request->limit() > 0 ? request->limit() : UINT32_MAX;
  status = file_system->ListDentry(ctx, request->parent(), request->last(), limit, request->is_only_dir(), dentries);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (auto& dentry : dentries) {
    *response->add_dentries() = dentry.Copy();
  }
}

void MDSServiceImpl::ListDentry(google::protobuf::RpcController* controller, const pb::mds::ListDentryRequest* request,
                                pb::mds::ListDentryResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }
    if (request->parent() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "parent is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoListDentry(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetInode(google::protobuf::RpcController*, const pb::mds::GetInodeRequest* request,
                                pb::mds::GetInodeResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  EntryOut entry_out;
  status = file_system->GetInode(ctx, request->ino(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
}

// inode interface
void MDSServiceImpl::GetInode(google::protobuf::RpcController* controller, const pb::mds::GetInodeRequest* request,
                              pb::mds::GetInodeResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetInode(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoBatchGetInode(google::protobuf::RpcController*, const pb::mds::BatchGetInodeRequest* request,
                                     pb::mds::BatchGetInodeResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::vector<EntryOut> entries;
  status = file_system->BatchGetInode(ctx, Helper::PbRepeatedToVector(request->inoes()), entries);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (auto& entry : entries) {
    response->add_inodes()->Swap(&entry.attr);
  }
}

void MDSServiceImpl::BatchGetInode(google::protobuf::RpcController* controller,
                                   const pb::mds::BatchGetInodeRequest* request,
                                   pb::mds::BatchGetInodeResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoBatchGetInode(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoBatchGetXAttr(google::protobuf::RpcController*, const pb::mds::BatchGetXAttrRequest* request,
                                     pb::mds::BatchGetXAttrResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::vector<pb::mds::XAttr> xattrs;
  status = file_system->BatchGetXAttr(ctx, Helper::PbRepeatedToVector(request->inoes()), xattrs);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (auto& xattr : xattrs) {
    response->add_xattrs()->CopyFrom(xattr);
  }
}

void MDSServiceImpl::BatchGetXAttr(google::protobuf::RpcController* controller,
                                   const pb::mds::BatchGetXAttrRequest* request,
                                   pb::mds::BatchGetXAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoBatchGetXAttr(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

// high level interface
void MDSServiceImpl::DoLookup(google::protobuf::RpcController* controller, const pb::mds::LookupRequest* request,
                              pb::mds::LookupResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  EntryOut entry_out;
  status = file_system->Lookup(ctx, request->parent(), request->name(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
}

void MDSServiceImpl::Lookup(google::protobuf::RpcController* controller, const pb::mds::LookupRequest* request,
                            pb::mds::LookupResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoLookup(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoBatchCreate(google::protobuf::RpcController*, const pb::mds::BatchCreateRequest* request,
                                   pb::mds::BatchCreateResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  std::vector<FileSystem::MkNodParam> params;
  params.reserve(request->params_size());
  for (const auto& req_param : request->params()) {
    FileSystem::MkNodParam param;
    param.parent = request->parent();
    param.name = req_param.name();
    param.mode = req_param.mode();
    param.uid = req_param.uid();
    param.gid = req_param.gid();
    param.rdev = req_param.rdev();

    params.push_back(param);
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  EntryOut entry_out;
  std::vector<std::string> session_ids;
  status = file_system->BatchCreate(ctx, request->parent(), params, entry_out, session_ids);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (auto& attr : entry_out.attrs) response->add_inodes()->Swap(&attr);
  Helper::VectorToPbRepeated(session_ids, response->mutable_session_ids());
  response->set_parent_version(entry_out.parent_version);
}

void MDSServiceImpl::BatchCreate(google::protobuf::RpcController* controller,
                                 const pb::mds::BatchCreateRequest* request, pb::mds::BatchCreateResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is empty");
    }
    if (request->parent() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "parent is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoBatchCreate(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoMkNod(google::protobuf::RpcController*, const pb::mds::MkNodRequest* request,
                             pb::mds::MkNodResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  FileSystem::MkNodParam param;
  param.parent = request->parent();
  param.name = request->name();
  param.mode = request->mode();
  param.uid = request->uid();
  param.gid = request->gid();
  param.rdev = request->rdev();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  EntryOut entry_out;
  status = file_system->MkNod(ctx, param, entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
  response->set_parent_version(entry_out.parent_version);
}

void MDSServiceImpl::MkNod(google::protobuf::RpcController* controller, const pb::mds::MkNodRequest* request,
                           pb::mds::MkNodResponse* response, google::protobuf::Closure* done) {
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

void MDSServiceImpl::DoMkDir(google::protobuf::RpcController*, const pb::mds::MkDirRequest* request,
                             pb::mds::MkDirResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  FileSystem::MkDirParam param;
  param.parent = request->parent();
  param.name = request->name();
  param.mode = request->mode();
  param.uid = request->uid();
  param.gid = request->gid();
  param.rdev = request->rdev();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  EntryOut entry_out;
  status = file_system->MkDir(ctx, param, entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
  response->set_parent_version(entry_out.parent_version);
}

void MDSServiceImpl::MkDir(google::protobuf::RpcController* controller, const pb::mds::MkDirRequest* request,
                           pb::mds::MkDirResponse* response, google::protobuf::Closure* done) {
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

void MDSServiceImpl::DoRmDir(google::protobuf::RpcController*, const pb::mds::RmDirRequest* request,
                             pb::mds::RmDirResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  status = file_system->RmDir(ctx, request->parent(), request->name());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::RmDir(google::protobuf::RpcController* controller, const pb::mds::RmDirRequest* request,
                           pb::mds::RmDirResponse* response, google::protobuf::Closure* done) {
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

void MDSServiceImpl::DoReadDir(google::protobuf::RpcController*, const pb::mds::ReadDirRequest* request,
                               pb::mds::ReadDirResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::vector<EntryOut> entry_outs;
  status = file_system->ReadDir(ctx, request->ino(), request->last_name(), request->limit(), request->with_attr(),
                                entry_outs);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (auto& entry_out : entry_outs) {
    auto* mut_entry = response->add_entries();
    mut_entry->set_name(entry_out.name);
    mut_entry->set_ino(entry_out.attr.ino());
    if (request->with_attr()) {
      mut_entry->mutable_inode()->Swap(&entry_out.attr);
    }
  }
}

void MDSServiceImpl::ReadDir(google::protobuf::RpcController* controller, const pb::mds::ReadDirRequest* request,
                             pb::mds::ReadDirResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoReadDir(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoOpen(google::protobuf::RpcController*, const pb::mds::OpenRequest* request,
                            pb::mds::OpenResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version(), req_ctx.client_id());

  std::string session_id;
  EntryOut entry_out;
  std::vector<ChunkEntry> chunks;
  status = file_system->Open(ctx, request->ino(), request->flags(), request->prefetch_chunk(), session_id, entry_out,
                             chunks);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_session_id(session_id);
  response->mutable_inode()->Swap(&entry_out.attr);
  Helper::VectorToPbRepeated(chunks, response->mutable_chunks());
}

void MDSServiceImpl::Open(google::protobuf::RpcController* controller, const pb::mds::OpenRequest* request,
                          pb::mds::OpenResponse* response, google::protobuf::Closure* done) {
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

void MDSServiceImpl::DoRelease(google::protobuf::RpcController*, const pb::mds::ReleaseRequest* request,
                               pb::mds::ReleaseResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version(), req_ctx.client_id());

  status = file_system->Release(ctx, request->ino(), request->session_id());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::Release(google::protobuf::RpcController* controller, const pb::mds::ReleaseRequest* request,
                             pb::mds::ReleaseResponse* response, google::protobuf::Closure* done) {
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

void MDSServiceImpl::DoLink(google::protobuf::RpcController*, const pb::mds::LinkRequest* request,
                            pb::mds::LinkResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  EntryOut entry_out;
  status = file_system->Link(ctx, request->ino(), request->new_parent(), request->new_name(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
  response->set_parent_version(entry_out.parent_version);
}

void MDSServiceImpl::Link(google::protobuf::RpcController* controller, const pb::mds::LinkRequest* request,
                          pb::mds::LinkResponse* response, google::protobuf::Closure* done) {
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

void MDSServiceImpl::DoUnLink(google::protobuf::RpcController*, const pb::mds::UnLinkRequest* request,
                              pb::mds::UnLinkResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  EntryOut entry_out;
  status = file_system->UnLink(ctx, request->parent(), request->name(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
  response->set_parent_version(entry_out.parent_version);
}

void MDSServiceImpl::UnLink(google::protobuf::RpcController* controller, const pb::mds::UnLinkRequest* request,
                            pb::mds::UnLinkResponse* response, google::protobuf::Closure* done) {
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

void MDSServiceImpl::DoSymlink(google::protobuf::RpcController*, const pb::mds::SymlinkRequest* request,
                               pb::mds::SymlinkResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  EntryOut entry_out;
  status = file_system->Symlink(ctx, request->symlink(), request->new_parent(), request->new_name(), request->uid(),
                                request->gid(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (!status.ok()) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
  response->set_parent_version(entry_out.parent_version);
}

void MDSServiceImpl::Symlink(google::protobuf::RpcController* controller, const pb::mds::SymlinkRequest* request,
                             pb::mds::SymlinkResponse* response, google::protobuf::Closure* done) {
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

void MDSServiceImpl::DoReadLink(google::protobuf::RpcController*, const pb::mds::ReadLinkRequest* request,
                                pb::mds::ReadLinkResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::string symlink;
  status = file_system->ReadLink(ctx, request->ino(), symlink);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_symlink(symlink);
}

void MDSServiceImpl::ReadLink(google::protobuf::RpcController* controller, const pb::mds::ReadLinkRequest* request,
                              pb::mds::ReadLinkResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoReadLink(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetAttr(google::protobuf::RpcController*, const pb::mds::GetAttrRequest* request,
                               pb::mds::GetAttrResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  EntryOut entry_out;
  status = file_system->GetAttr(ctx, request->ino(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
}

void MDSServiceImpl::GetAttr(google::protobuf::RpcController* controller, const pb::mds::GetAttrRequest* request,
                             pb::mds::GetAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is empty");
    }
    if (request->ino() == 0) {  // ino is required
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetAttr(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSetAttr(google::protobuf::RpcController*, const pb::mds::SetAttrRequest* request,
                               pb::mds::SetAttrResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  FileSystem::SetAttrParam param;
  auto& attr = param.attr;
  attr.set_fs_id(request->fs_id());
  attr.set_ino(request->ino());
  attr.set_length(request->length());
  attr.set_ctime(request->ctime());
  attr.set_mtime(request->mtime());
  attr.set_atime(request->atime());
  attr.set_uid(request->uid());
  attr.set_gid(request->gid());
  attr.set_mode(request->mode());
  attr.set_flags(request->flags());
  param.to_set = request->to_set();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  EntryOut entry_out;
  status = file_system->SetAttr(ctx, request->ino(), param, entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
}

void MDSServiceImpl::SetAttr(google::protobuf::RpcController* controller, const pb::mds::SetAttrRequest* request,
                             pb::mds::SetAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is empty");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

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

void MDSServiceImpl::DoGetXAttr(google::protobuf::RpcController*, const pb::mds::GetXAttrRequest* request,
                                pb::mds::GetXAttrResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::string value;
  status = file_system->GetXAttr(ctx, request->ino(), request->name(), value);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_value(value);
}

void MDSServiceImpl::GetXAttr(google::protobuf::RpcController* controller, const pb::mds::GetXAttrRequest* request,
                              pb::mds::GetXAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is empty");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is empty");
    }
    if (request->name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "name is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetXAttr(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSetXAttr(google::protobuf::RpcController*, const pb::mds::SetXAttrRequest* request,
                                pb::mds::SetXAttrResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  EntryOut entry_out;
  status = file_system->SetXAttr(ctx, request->ino(), request->xattrs(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
}

void MDSServiceImpl::SetXAttr(google::protobuf::RpcController* controller, const pb::mds::SetXAttrRequest* request,
                              pb::mds::SetXAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is empty");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is empty");
    }
    if (request->xattrs().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "xattrs is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

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

void MDSServiceImpl::DoRemoveXAttr(google::protobuf::RpcController*, const pb::mds::RemoveXAttrRequest* request,
                                   pb::mds::RemoveXAttrResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  EntryOut entry_out;
  status = file_system->RemoveXAttr(ctx, request->ino(), request->name(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
}

void MDSServiceImpl::RemoveXAttr(google::protobuf::RpcController* controller,
                                 const pb::mds::RemoveXAttrRequest* request, pb::mds::RemoveXAttrResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is empty");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is empty");
    }
    if (request->name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "name is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoRemoveXAttr(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoListXAttr(google::protobuf::RpcController*, const pb::mds::ListXAttrRequest* request,
                                 pb::mds::ListXAttrResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  Inode::XAttrMap xattrs;
  status = file_system->GetXAttr(ctx, request->ino(), xattrs);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_xattrs()->swap(xattrs);
}

void MDSServiceImpl::ListXAttr(google::protobuf::RpcController* controller, const pb::mds::ListXAttrRequest* request,
                               pb::mds::ListXAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoListXAttr(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoRename(google::protobuf::RpcController*, const pb::mds::RenameRequest* request,
                              pb::mds::RenameResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  FileSystem::RenameParam param;
  param.old_parent = request->old_parent();
  param.old_name = request->old_name();
  param.new_parent = request->new_parent();
  param.new_name = request->new_name();
  param.old_ancestors = Helper::PbRepeatedToVector(request->old_ancestors());
  param.new_ancestors = Helper::PbRepeatedToVector(request->new_ancestors());

  uint64_t old_parent_version, new_parent_version;
  status = file_system->CommitRename(ctx, param, old_parent_version, new_parent_version);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_old_parent_version(old_parent_version);
  response->set_new_parent_version(new_parent_version);
}

void MDSServiceImpl::Rename(google::protobuf::RpcController* controller, const pb::mds::RenameRequest* request,
                            pb::mds::RenameResponse* response, google::protobuf::Closure* done) {
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

void MDSServiceImpl::DoAllocSliceId(google::protobuf::RpcController*, const pb::mds::AllocSliceIdRequest* request,
                                    pb::mds::AllocSliceIdResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  if (request->alloc_num() == 0) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETER,
                                   "param alloc_num is error");
  }

  uint64_t slice_id;
  auto status = file_system_set_->AllocSliceId(request->alloc_num(), request->min_slice_id(), slice_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_slice_id(slice_id);
}

void MDSServiceImpl::AllocSliceId(google::protobuf::RpcController* controller,
                                  const pb::mds::AllocSliceIdRequest* request, pb::mds::AllocSliceIdResponse* response,
                                  google::protobuf::Closure* done) {
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

void MDSServiceImpl::DoWriteSlice(google::protobuf::RpcController*, const pb::mds::WriteSliceRequest* request,
                                  pb::mds::WriteSliceResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  status = file_system->WriteSlice(ctx, request->parent(), request->ino(),
                                   Helper::PbRepeatedToVector(request->delta_slices()));
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
}

void MDSServiceImpl::WriteSlice(google::protobuf::RpcController* controller, const pb::mds::WriteSliceRequest* request,
                                pb::mds::WriteSliceResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }
    if (request->parent() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "parent is 0");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is 0");
    }
    if (request->delta_slices().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "delta_slices is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoWriteSlice(controller, request, response, svr_done); });

  bool ret = write_worker_set_->ExecuteHash(request->ino(), task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoReadSlice(google::protobuf::RpcController*, const pb::mds::ReadSliceRequest* request,
                                 pb::mds::ReadSliceResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::vector<ChunkEntry> chunks;
  status = file_system->ReadSlice(ctx, request->ino(), Helper::PbRepeatedToVector(request->chunk_indexes()), chunks);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  Helper::VectorToPbRepeated(chunks, response->mutable_chunks());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
}

void MDSServiceImpl::ReadSlice(google::protobuf::RpcController* controller, const pb::mds::ReadSliceRequest* request,
                               pb::mds::ReadSliceResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is 0");
    }

    if (request->chunk_indexes().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "chunk_indexes is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoReadSlice(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoFallocate(google::protobuf::RpcController*, const pb::mds::FallocateRequest* request,
                                 pb::mds::FallocateResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = ValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  EntryOut entry_out;
  status = file_system->Fallocate(ctx, request->ino(), request->mode(), request->offset(), request->len(), entry_out);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
}

void MDSServiceImpl::Fallocate(google::protobuf::RpcController* controller, const pb::mds::FallocateRequest* request,
                               pb::mds::FallocateResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is 0");
    }
    if (request->len() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "len is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoFallocate(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoCompactChunk(google::protobuf::RpcController*, const pb::mds::CompactChunkRequest* request,
                                    pb::mds::CompactChunkResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::vector<pb::mds::TrashSlice> trash_slices;
  status = file_system->CompactChunk(ctx, request->ino(), request->chunk_index(), trash_slices);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());

  for (auto& slice : trash_slices) {
    response->add_trash_slices()->Swap(&slice);
  }
}

void MDSServiceImpl::CompactChunk(google::protobuf::RpcController* controller,
                                  const pb::mds::CompactChunkRequest* request, pb::mds::CompactChunkResponse* response,
                                  google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoCompactChunk(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoCleanTrashSlice(google::protobuf::RpcController*, const pb::mds::CleanTrashSliceRequest* request,
                                       pb::mds::CleanTrashSliceResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  auto& trace = ctx.GetTrace();

  auto status = gc_processor_->ManualCleanDelSlice(trace, request->fs_id(), request->ino(), request->chunk_index());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::CleanTrashSlice(google::protobuf::RpcController* controller,
                                     const pb::mds::CleanTrashSliceRequest* request,
                                     pb::mds::CleanTrashSliceResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }

    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCleanTrashSlice(controller, request, response, svr_done);
  });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoCleanDelFile(google::protobuf::RpcController*, const pb::mds::CleanDelFileRequest* request,
                                    pb::mds::CleanDelFileResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  auto& trace = ctx.GetTrace();

  auto status = gc_processor_->ManualCleanDelFile(trace, request->fs_id(), request->ino());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::CleanDelFile(google::protobuf::RpcController* controller,
                                  const pb::mds::CleanDelFileRequest* request, pb::mds::CleanDelFileResponse* response,
                                  google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }

    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoCleanDelFile(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSetFsQuota(google::protobuf::RpcController*, const pb::mds::SetFsQuotaRequest* request,
                                  pb::mds::SetFsQuotaResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  auto& quota_manager = file_system->GetQuotaManager();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  status = quota_manager.SetFsQuota(ctx.GetTrace(), request->quota());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

// quota interface
void MDSServiceImpl::SetFsQuota(google::protobuf::RpcController* controller, const pb::mds::SetFsQuotaRequest* request,
                                pb::mds::SetFsQuotaResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoSetFsQuota(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetFsQuota(google::protobuf::RpcController*, const pb::mds::GetFsQuotaRequest* request,
                                  pb::mds::GetFsQuotaResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  auto& quota_manager = file_system->GetQuotaManager();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  pb::mds::Quota quota;
  status = quota_manager.GetFsQuota(ctx.GetTrace(), req_ctx.is_bypass_cache(), quota);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_quota()->Swap(&quota);
}

void MDSServiceImpl::GetFsQuota(google::protobuf::RpcController* controller, const pb::mds::GetFsQuotaRequest* request,
                                pb::mds::GetFsQuotaResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetFsQuota(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSetDirQuota(google::protobuf::RpcController*, const pb::mds::SetDirQuotaRequest* request,
                                   pb::mds::SetDirQuotaResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  auto& quota_manager = file_system->GetQuotaManager();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  status = quota_manager.SetDirQuota(ctx.GetTrace(), request->ino(), request->quota(), true);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::SetDirQuota(google::protobuf::RpcController* controller,
                                 const pb::mds::SetDirQuotaRequest* request, pb::mds::SetDirQuotaResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoSetDirQuota(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetDirQuota(google::protobuf::RpcController*, const pb::mds::GetDirQuotaRequest* request,
                                   pb::mds::GetDirQuotaResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  auto& quota_manager = file_system->GetQuotaManager();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  pb::mds::Quota quota;
  status = quota_manager.GetDirQuota(ctx.GetTrace(), request->ino(), quota);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_quota()->Swap(&quota);
}

void MDSServiceImpl::GetDirQuota(google::protobuf::RpcController* controller,
                                 const pb::mds::GetDirQuotaRequest* request, pb::mds::GetDirQuotaResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetDirQuota(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoDeleteDirQuota(google::protobuf::RpcController*, const pb::mds::DeleteDirQuotaRequest* request,
                                      pb::mds::DeleteDirQuotaResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  auto& quota_manager = file_system->GetQuotaManager();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  status = quota_manager.DeleteDirQuota(ctx.GetTrace(), request->ino());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::DeleteDirQuota(google::protobuf::RpcController* controller,
                                    const pb::mds::DeleteDirQuotaRequest* request,
                                    pb::mds::DeleteDirQuotaResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoDeleteDirQuota(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoLoadDirQuotas(google::protobuf::RpcController*, const pb::mds::LoadDirQuotasRequest* request,
                                     pb::mds::LoadDirQuotasResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  auto status = SimpleValidateRequest(file_system, request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  auto& quota_manager = file_system->GetQuotaManager();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::map<uint64_t, pb::mds::Quota> quotas;
  status = quota_manager.LoadDirQuotas(ctx.GetTrace(), quotas);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (const auto& [ino, quota] : quotas) {
    response->mutable_quotas()->insert(std::make_pair(ino, quota));
  }
}

void MDSServiceImpl::LoadDirQuotas(google::protobuf::RpcController* controller,
                                   const pb::mds::LoadDirQuotasRequest* request,
                                   pb::mds::LoadDirQuotasResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoLoadDirQuotas(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSetFsStats(google::protobuf::RpcController*, const pb::mds::SetFsStatsRequest* request,
                                  pb::mds::SetFsStatsResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto status = ValidateRequest(request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  uint32_t fs_id = file_system_set_->GetFsId(request->fs_name());
  if (fs_id == 0) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  Context ctx;
  status = fs_stat_->UploadFsStat(ctx, fs_id, request->stats());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::SetFsStats(google::protobuf::RpcController* controller, const pb::mds::SetFsStatsRequest* request,
                                pb::mds::SetFsStatsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_name is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoSetFsStats(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetFsStats(google::protobuf::RpcController*, const pb::mds::GetFsStatsRequest* request,
                                  pb::mds::GetFsStatsResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto status = ValidateRequest(request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
  uint32_t fs_id = file_system_set_->GetFsId(request->fs_name());
  if (fs_id == 0) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  Context ctx;
  pb::mds::FsStatsData stats;
  status = fs_stat_->GetFsStat(ctx, fs_id, stats);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_stats()->Swap(&stats);
}

void MDSServiceImpl::GetFsStats(google::protobuf::RpcController* controller, const pb::mds::GetFsStatsRequest* request,
                                pb::mds::GetFsStatsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_name is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetFsStats(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetFsPerSecondStats(google::protobuf::RpcController*,
                                           const pb::mds::GetFsPerSecondStatsRequest* request,
                                           pb::mds::GetFsPerSecondStatsResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto status = ValidateRequest(request, done->GetQueueWaitTimeUs());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  uint32_t fs_id = file_system_set_->GetFsId(request->fs_name());
  if (fs_id == 0) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  Context ctx;
  std::map<uint64_t, pb::mds::FsStatsData> stats;
  status = fs_stat_->GetFsStatsPerSecond(ctx, fs_id, stats);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (auto& [time, stat] : stats) {
    response->mutable_stats()->insert(std::make_pair(time, std::move(stat)));
  }
}

void MDSServiceImpl::GetFsPerSecondStats(google::protobuf::RpcController* controller,
                                         const pb::mds::GetFsPerSecondStatsRequest* request,
                                         pb::mds::GetFsPerSecondStatsResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_name is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetFsPerSecondStats(controller, request, response, svr_done);
  });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::CheckAlive(google::protobuf::RpcController* controller, const pb::mds::CheckAliveRequest* request,
                                pb::mds::CheckAliveResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  brpc::ClosureGuard done_guard(svr_done);
}

void MDSServiceImpl::DoNotifyBuddy(google::protobuf::RpcController*, const pb::mds::NotifyBuddyRequest* request,
                                   pb::mds::NotifyBuddyResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  for (const auto& message : request->messages()) {
    switch (message.type()) {
      case pb::mds::NotifyBuddyRequest::TYPE_REFRESH_FS_INFO: {
        auto status = file_system_set_->RefreshFsInfo(message.refresh_fs_info().fs_name(), "notify buddy");
        if (!status.ok()) {
          DINGO_LOG(ERROR) << fmt::format("refresh fs info fail, status({})", status.error_str());
        }

      } break;

      case pb::mds::NotifyBuddyRequest::TYPE_REFRESH_INODE: {
        auto file_system = GetFileSystem(message.fs_id());
        if (file_system == nullptr) {
          return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
        }

        auto& mut_message = const_cast<pb::mds::NotifyBuddyRequest::Message&>(message);
        file_system->RefreshInode(*mut_message.mutable_refresh_inode()->mutable_inode());
      } break;

      case pb::mds::NotifyBuddyRequest::TYPE_CLEAN_PARTITION_CACHE: {
        auto file_system = GetFileSystem(message.fs_id());
        if (file_system == nullptr) {
          return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
        }

        file_system->GetPartitionCache().DeleteIf(message.clean_partition_cache().ino(),
                                                  message.clean_partition_cache().version());

      } break;

      case pb::mds::NotifyBuddyRequest::TYPE_SET_DIR_QUOTA: {
        auto file_system = GetFileSystem(message.fs_id());
        auto& quota_manager = file_system->GetQuotaManager();
        Trace trace;
        const auto& set_dir_quota = message.set_dir_quota();
        quota_manager.SetDirQuota(trace, set_dir_quota.ino(), set_dir_quota.quota(), false);
      } break;

      case pb::mds::NotifyBuddyRequest::TYPE_DELETE_DIR_QUOTA: {
        auto file_system = GetFileSystem(message.fs_id());
        auto& quota_manager = file_system->GetQuotaManager();
        const auto& delete_dir_quota = message.delete_dir_quota();
        quota_manager.DeleteDirQuotaByNotified(delete_dir_quota.ino(), delete_dir_quota.uuid());
      } break;

      default:
        DINGO_LOG(FATAL) << "unknown message type: " << message.type();
    }
  }
}

void MDSServiceImpl::NotifyBuddy(google::protobuf::RpcController* controller,
                                 const pb::mds::NotifyBuddyRequest* request, pb::mds::NotifyBuddyResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoNotifyBuddy(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::JoinFs(google::protobuf::RpcController* controller, const pb::mds::JoinFsRequest* request,
                            pb::mds::JoinFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  brpc::ClosureGuard done_guard(svr_done);

  if (request->fs_name().empty() && request->fs_id() == 0) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETER,
                                   "fs_name or fs_id is empty");
  }

  if (request->mds_ids().empty()) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETER, "mds_ids is empty");
  }

  Context ctx;
  std::vector<uint64_t> mds_ids = Helper::PbRepeatedToVector(request->mds_ids());

  std::string reason = "manual join fs";
  Status status = !request->fs_name().empty() ? file_system_set_->JoinFs(ctx, request->fs_name(), mds_ids, reason)
                                              : file_system_set_->JoinFs(ctx, request->fs_id(), mds_ids, reason);

  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::QuitFs(google::protobuf::RpcController* controller, const pb::mds::QuitFsRequest* request,
                            pb::mds::QuitFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  brpc::ClosureGuard done_guard(svr_done);

  if (request->fs_name().empty() && request->fs_id() == 0) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETER,
                                   "fs_name or fs_id is empty");
  }

  if (request->mds_ids().empty()) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETER, "mds_ids is empty");
  }

  Context ctx;
  std::vector<uint64_t> mds_ids = Helper::PbRepeatedToVector(request->mds_ids());

  std::string reason = "manual quit fs";
  Status status = !request->fs_name().empty() ? file_system_set_->QuitFs(ctx, request->fs_name(), mds_ids, reason)
                                              : file_system_set_->QuitFs(ctx, request->fs_id(), mds_ids, reason);

  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::StopMds(google::protobuf::RpcController*, const pb::mds::StopMdsRequest* request,
                             pb::mds::StopMdsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  brpc::ClosureGuard done_guard(svr_done);
}

// cache group member
static bool IsUuidRegex(const std::string member_id) {
  static const std::regex uuid_pattern(
      "^[0-9a-fA-F]{8}-"
      "[0-9a-fA-F]{4}-"
      "[0-9a-fA-F]{4}-"
      "[0-9a-fA-F]{4}-"
      "[0-9a-fA-F]{12}$");
  return std::regex_match(member_id, uuid_pattern);
}

static Status CheckGroupName(const std::string& group_name) {
  if (group_name.empty()) {
    return Status(pb::error::Errno::EILLEGAL_PARAMTETER, "group name is empty");
  } else if (group_name.size() > kMaxGroupNameLength) {
    return Status(pb::error::Errno::EILLEGAL_PARAMTETER, "group name is too long");
  }
  return Status::OK();
}

void MDSServiceImpl::JoinCacheGroup(google::protobuf::RpcController* controller,
                                    const pb::mds::JoinCacheGroupRequest* request,
                                    pb::mds::JoinCacheGroupResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  auto validate_fn = [&]() -> Status {
    if (!IsUuidRegex(request->member_id())) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "member_id is not uuid");
    }
    return CheckGroupName(request->group_name());
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoJoinCacheGroup(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::LeaveCacheGroup(google::protobuf::RpcController* controller,
                                     const pb::mds::LeaveCacheGroupRequest* request,
                                     pb::mds::LeaveCacheGroupResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  auto validate_fn = [&]() -> Status {
    if (!IsUuidRegex(request->member_id())) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "member_id is not uuid");
    }
    return CheckGroupName(request->group_name());
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoLeaveCacheGroup(controller, request, response, svr_done);
  });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::ListGroups(google::protobuf::RpcController* controller, const pb::mds::ListGroupsRequest* request,
                                pb::mds::ListGroupsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoListGroups(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::ReweightMember(google::protobuf::RpcController* controller,
                                    const pb::mds::ReweightMemberRequest* request,
                                    pb::mds::ReweightMemberResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  auto validate_fn = [&]() -> Status {
    if (!IsUuidRegex(request->member_id())) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "member_id is not uuid");
    }
    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoReweightMember(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::ListMembers(google::protobuf::RpcController* controller,
                                 const pb::mds::ListMembersRequest* request, pb::mds::ListMembersResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoListMembers(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::UnlockMember(google::protobuf::RpcController* controller,
                                  const pb::mds::UnLockMemberRequest* request, pb::mds::UnLockMemberResponse* response,
                                  google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  auto validate_fn = [&]() -> Status {
    if (!IsUuidRegex(request->member_id())) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "member_id is not uuid");
    }
    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoUnlockMember(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DeleteMember(google::protobuf::RpcController* controller,
                                  const pb::mds::DeleteMemberRequest* request, pb::mds::DeleteMemberResponse* response,
                                  google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  auto validate_fn = [&]() -> Status {
    if (!IsUuidRegex(request->member_id())) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "member_id is not uuid");
    }
    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoDeleteMember(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoJoinCacheGroup(google::protobuf::RpcController* controller,
                                      const pb::mds::JoinCacheGroupRequest* request,
                                      pb::mds::JoinCacheGroupResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  Context ctx;
  auto status = cache_group_manager_->JoinCacheGroup(ctx, request->group_name(), request->ip(), request->port(),
                                                     request->weight(), request->member_id());

  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::DoLeaveCacheGroup(google::protobuf::RpcController* controller,
                                       const pb::mds::LeaveCacheGroupRequest* request,
                                       pb::mds::LeaveCacheGroupResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  Context ctx;
  auto status = cache_group_manager_->LeaveCacheGroup(ctx, request->group_name(), request->member_id(), request->ip(),
                                                      request->port());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::DoListGroups(google::protobuf::RpcController* controller,
                                  const pb::mds::ListGroupsRequest* request, pb::mds::ListGroupsResponse* response,
                                  TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  Context ctx;
  std::unordered_set<std::string> groups;
  auto status = cache_group_manager_->ListGroups(ctx, groups);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
  *response->mutable_group_names() = {groups.begin(), groups.end()};
}

void MDSServiceImpl::DoReweightMember(google::protobuf::RpcController* controller,
                                      const pb::mds::ReweightMemberRequest* request,
                                      pb::mds::ReweightMemberResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  Context ctx;
  auto status = cache_group_manager_->ReweightMember(ctx, request->member_id(), request->ip(), request->port(),
                                                     request->weight());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::DoListMembers(google::protobuf::RpcController* controller,
                                   const pb::mds::ListMembersRequest* request, pb::mds::ListMembersResponse* response,
                                   TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  Context ctx;
  std::vector<CacheMemberEntry> members;
  auto status = cache_group_manager_->ListMembers(ctx, request->group_name(), members);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
  *response->mutable_members() = {members.begin(), members.end()};
}

void MDSServiceImpl::DoUnlockMember(google::protobuf::RpcController* controller,
                                    const pb::mds::UnLockMemberRequest* request,
                                    pb::mds::UnLockMemberResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  Context ctx;
  auto status = cache_group_manager_->UnlockMember(ctx, request->member_id(), request->ip(), request->port());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::DoDeleteMember(google::protobuf::RpcController* controller,
                                    const pb::mds::DeleteMemberRequest* request,
                                    pb::mds::DeleteMemberResponse* response, TraceClosure* done) {
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  Context ctx;
  auto status = cache_group_manager_->DeleteMember(ctx, request->member_id());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::DescribeByJson(Json::Value& value) {
  Json::Value read_worker_set_value;
  read_worker_set_->DescribeByJson(read_worker_set_value);
  value["read_worker_set"] = read_worker_set_value;

  Json::Value write_worker_set_value;
  write_worker_set_->DescribeByJson(write_worker_set_value);
  value["write_worker_set"] = write_worker_set_value;
}

}  // namespace mds
}  // namespace dingofs