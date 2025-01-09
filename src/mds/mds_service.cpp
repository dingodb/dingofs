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
 * Project: dingo
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include "mds/mds_service.h"

#include <vector>

#include "mds/metric/metric.h"

namespace dingofs {
namespace mds {

using pb::mds::Mountpoint;

void MdsServiceImpl::CreateFs(::google::protobuf::RpcController* controller,
                              const pb::mds::CreateFsRequest* request,
                              pb::mds::CreateFsResponse* response,
                              ::google::protobuf::Closure* done) {
  (void)controller;
  brpc::ClosureGuard doneGuard(done);
  const std::string& fsName = request->fsname();
  uint64_t blockSize = request->blocksize();
  pb::common::FSType type = request->fstype();
  bool enableSumInDir = request->enablesumindir();

  // set response statuscode default value is ok
  response->set_statuscode(FSStatusCode::OK);

  LOG(INFO) << "CreateFs request: " << request->ShortDebugString();

  // create s3 fs
  auto createS3Fs = [&]() {
    if (!request->fsdetail().has_s3info()) {
      response->set_statuscode(FSStatusCode::PARAM_ERROR);
      LOG(ERROR) << "CreateFs request, type is s3, but has no s3info"
                 << ", fsName = " << fsName;
      return;
    }
    const auto& s3Info = request->fsdetail().s3info();
    FSStatusCode status =
        fsManager_->CreateFs(request, response->mutable_fsinfo());

    if (status != FSStatusCode::OK) {
      response->clear_fsinfo();
      response->set_statuscode(status);
      LOG(ERROR) << "CreateFs fail, fsName = " << fsName
                 << ", blockSize = " << blockSize
                 << ", s3Info.bucketname = " << s3Info.bucketname()
                 << ", enableSumInDir = " << enableSumInDir
                 << ", owner = " << request->owner()
                 << ", capacity = " << request->capacity()
                 << ", errCode = " << FSStatusCode_Name(status);
      return;
    }
  };

  switch (type) {
    case pb::common::FSType::TYPE_VOLUME:
      CHECK(false) << "CreateFs TYPE_VOLUME is not supported";
      break;
    case pb::common::FSType::TYPE_S3:
      createS3Fs();
      break;
    case pb::common::FSType::TYPE_HYBRID:
      CHECK(false) << "CreateFs TYPE_HYBRID is not supported";
      break;
    default:
      response->set_statuscode(FSStatusCode::PARAM_ERROR);
      LOG(ERROR) << "CreateFs fail, fs type is invalid"
                 << ", fsName = " << fsName << ", blockSize = " << blockSize
                 << ", fsType = " << type << ", errCode = "
                 << FSStatusCode_Name(FSStatusCode::PARAM_ERROR);
      break;
  }

  if (response->statuscode() != FSStatusCode::OK) {
    return;
  }
  LOG(INFO) << "CreateFs success, fsName = " << fsName
            << ", blockSize = " << blockSize << ", owner = " << request->owner()
            << ", capacity = " << request->capacity();
}

void MdsServiceImpl::MountFs(::google::protobuf::RpcController* controller,
                             const pb::mds::MountFsRequest* request,
                             pb::mds::MountFsResponse* response,
                             ::google::protobuf::Closure* done) {
  (void)controller;
  brpc::ClosureGuard doneGuard(done);
  const std::string& fsName = request->fsname();
  const Mountpoint& mount = request->mountpoint();
  LOG(INFO) << "MountFs request, fsName = " << fsName
            << ", mountPoint = " << mount.ShortDebugString();
  FSStatusCode status =
      fsManager_->MountFs(fsName, mount, response->mutable_fsinfo());
  if (status != FSStatusCode::OK) {
    response->clear_fsinfo();
    response->set_statuscode(status);
    LOG(ERROR) << "MountFs fail, fsName = " << fsName
               << ", mountPoint = " << mount.ShortDebugString()
               << ", errCode = " << FSStatusCode_Name(status);
    return;
  }

  response->set_statuscode(FSStatusCode::OK);
  LOG(INFO) << "MountFs success, fsName = " << fsName
            << ", mountPoint = " << mount.ShortDebugString()
            << ", mps: " << response->mutable_fsinfo()->mountpoints_size();
}

void MdsServiceImpl::UmountFs(::google::protobuf::RpcController* controller,
                              const pb::mds::UmountFsRequest* request,
                              pb::mds::UmountFsResponse* response,
                              ::google::protobuf::Closure* done) {
  (void)controller;
  brpc::ClosureGuard doneGuard(done);
  const std::string& fsName = request->fsname();
  const Mountpoint& mount = request->mountpoint();
  LOG(INFO) << "UmountFs request, " << request->ShortDebugString();
  FSStatusCode status = fsManager_->UmountFs(fsName, mount);
  if (status != FSStatusCode::OK) {
    response->set_statuscode(status);
    LOG(ERROR) << "UmountFs fail, fsName = " << fsName
               << ", mountPoint = " << mount.ShortDebugString()
               << ", errCode = " << FSStatusCode_Name(status);
    return;
  }

  response->set_statuscode(FSStatusCode::OK);
  LOG(INFO) << "UmountFs success, fsName = " << fsName
            << ", mountPoint = " << mount.ShortDebugString();
}

void MdsServiceImpl::GetFsInfo(::google::protobuf::RpcController* controller,
                               const pb::mds::GetFsInfoRequest* request,
                               pb::mds::GetFsInfoResponse* response,
                               ::google::protobuf::Closure* done) {
  (void)controller;
  brpc::ClosureGuard doneGuard(done);

  LOG(INFO) << "GetFsInfo request: " << request->ShortDebugString();

  pb::mds::FsInfo* fsInfo = response->mutable_fsinfo();
  FSStatusCode status = FSStatusCode::OK;
  if (request->has_fsid() && request->has_fsname()) {
    status = fsManager_->GetFsInfo(request->fsname(), request->fsid(), fsInfo);
  } else if (!request->has_fsid() && request->has_fsname()) {
    status = fsManager_->GetFsInfo(request->fsname(), fsInfo);
  } else if (request->has_fsid() && !request->has_fsname()) {
    status = fsManager_->GetFsInfo(request->fsid(), fsInfo);
  } else {
    status = FSStatusCode::PARAM_ERROR;
  }

  if (status != FSStatusCode::OK) {
    response->clear_fsinfo();
    response->set_statuscode(status);
    LOG(ERROR) << "GetFsInfo fail, request: " << request->ShortDebugString()
               << ", errCode = " << FSStatusCode_Name(status);
    return;
  }

  response->set_statuscode(FSStatusCode::OK);
  LOG(INFO) << "GetFsInfo success, response: " << response->ShortDebugString();
}

void MdsServiceImpl::DeleteFs(::google::protobuf::RpcController* controller,
                              const pb::mds::DeleteFsRequest* request,
                              pb::mds::DeleteFsResponse* response,
                              ::google::protobuf::Closure* done) {
  (void)controller;
  brpc::ClosureGuard doneGuard(done);
  const std::string& fsName = request->fsname();
  LOG(INFO) << "DeleteFs request, fsName = " << fsName;
  FSStatusCode status = fsManager_->DeleteFs(fsName);
  response->set_statuscode(status);
  if (status != FSStatusCode::OK && status != FSStatusCode::UNDER_DELETING) {
    LOG(ERROR) << "DeleteFs fail, fsName = " << fsName
               << ", errCode = " << FSStatusCode_Name(status);
    return;
  }

  LOG(INFO) << "DeleteFs success, fsName = " << fsName;
}

void MdsServiceImpl::AllocateS3Chunk(
    ::google::protobuf::RpcController* controller,
    const pb::mds::AllocateS3ChunkRequest* request,
    pb::mds::AllocateS3ChunkResponse* response,
    ::google::protobuf::Closure* done) {
  (void)controller;

  brpc::ClosureGuard guard(done);
  VLOG(9) << "start to allocate chunkId.";

  uint64_t chunkId = 0;
  uint64_t chunkIdNum = 1;
  if (request->has_chunkidnum()) {
    chunkIdNum = request->chunkidnum();
  }
  int stat = chunkIdAllocator_->GenChunkId(chunkIdNum, &chunkId);
  FSStatusCode resStat;
  if (stat >= 0) {
    resStat = pb::mds::OK;
  } else {
    resStat = pb::mds::ALLOCATE_CHUNKID_ERROR;
  }

  response->set_statuscode(resStat);

  if (resStat != pb::mds::OK) {
    LOG(ERROR) << "AllocateS3Chunk failure, request: "
               << request->ShortDebugString()
               << ", error: " << FSStatusCode_Name(resStat);
  } else {
    response->set_beginchunkid(chunkId);
    VLOG(9) << "AllocateS3Chunk success, request: "
            << request->ShortDebugString()
            << ", response: " << response->ShortDebugString();
  }
}

void MdsServiceImpl::ListClusterFsInfo(
    ::google::protobuf::RpcController* controller,
    const pb::mds::ListClusterFsInfoRequest* request,
    pb::mds::ListClusterFsInfoResponse* response,
    ::google::protobuf::Closure* done) {
  (void)controller;
  (void)request;

  brpc::ClosureGuard guard(done);
  LOG(INFO) << "start to check cluster fs info.";
  fsManager_->GetAllFsInfo(response->mutable_fsinfo());
  LOG(INFO) << "ListClusterFsInfo success, response: "
            << response->ShortDebugString();
}

void MdsServiceImpl::RefreshSession(
    ::google::protobuf::RpcController* controller,
    const pb::mds::RefreshSessionRequest* request,
    pb::mds::RefreshSessionResponse* response,
    ::google::protobuf::Closure* done) {
  (void)controller;
  brpc::ClosureGuard guard(done);
  fsManager_->RefreshSession(request, response);
  response->set_statuscode(FSStatusCode::OK);
}

void MdsServiceImpl::GetLatestTxId(
    ::google::protobuf::RpcController* controller,
    const pb::mds::GetLatestTxIdRequest* request,
    pb::mds::GetLatestTxIdResponse* response,
    ::google::protobuf::Closure* done) {
  (void)controller;
  brpc::ClosureGuard guard(done);
  VLOG(3) << "GetLatestTxId [request]: " << request->DebugString();
  fsManager_->GetLatestTxId(request, response);
  VLOG(3) << "GetLatestTxId [response]: " << response->DebugString();
}

void MdsServiceImpl::CommitTx(::google::protobuf::RpcController* controller,
                              const pb::mds::CommitTxRequest* request,
                              pb::mds::CommitTxResponse* response,
                              ::google::protobuf::Closure* done) {
  (void)controller;
  brpc::ClosureGuard guard(done);
  VLOG(3) << "CommitTx [request]: " << request->DebugString();
  fsManager_->CommitTx(request, response);
  VLOG(3) << "CommitTx [response]: " << response->DebugString();
}

void MdsServiceImpl::SetFsStats(::google::protobuf::RpcController* controller,
                                const pb::mds::SetFsStatsRequest* request,
                                pb::mds::SetFsStatsResponse* response,
                                ::google::protobuf::Closure* done) {
  (void)controller;
  brpc::ClosureGuard guard(done);
  VLOG(9) << "SetFsStats [request]: " << request->DebugString();
  fsManager_->SetFsStats(request, response);
  VLOG(9) << "SetFsStats [response]: " << response->DebugString();
}

void MdsServiceImpl::GetFsStats(::google::protobuf::RpcController* controller,
                                const pb::mds::GetFsStatsRequest* request,
                                pb::mds::GetFsStatsResponse* response,
                                ::google::protobuf::Closure* done) {
  (void)controller;
  brpc::ClosureGuard guard(done);
  VLOG(9) << "GetFsStats [request]: " << request->DebugString();
  fsManager_->GetFsStats(request, response);
  VLOG(9) << "GetFsStats [response]: " << response->DebugString();
}

void MdsServiceImpl::GetFsPerSecondStats(
    ::google::protobuf::RpcController* controller,
    const pb::mds::GetFsPerSecondStatsRequest* request,
    pb::mds::GetFsPerSecondStatsResponse* response,
    ::google::protobuf::Closure* done) {
  (void)controller;
  brpc::ClosureGuard guard(done);
  VLOG(9) << "GetFsStats [request]: " << request->DebugString();
  fsManager_->GetFsPerSecondStats(request, response);
  VLOG(9) << "GetFsStats [response]: " << response->DebugString();
}

}  // namespace mds
}  // namespace dingofs
