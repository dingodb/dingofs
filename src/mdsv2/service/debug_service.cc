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

#include "mdsv2/service/debug_service.h"

#include <fmt/chrono.h>

#include "dingofs/debug.pb.h"
#include "dingofs/error.pb.h"
#include "mdsv2/common/context.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/filesystem/partition.h"
#include "mdsv2/server.h"
#include "mdsv2/service/service_helper.h"

#ifdef USE_TCMALLOC
#include "gperftools/malloc_extension.h"
#endif

namespace dingofs {
namespace mdsv2 {

FileSystemSPtr DebugServiceImpl::GetFileSystem(uint32_t fs_id) { return file_system_set_->GetFileSystem(fs_id); }

void DebugServiceImpl::GetLogLevel(google::protobuf::RpcController*, const pb::debug::GetLogLevelRequest* request,
                                   pb::debug::GetLogLevelResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::ClosureGuard done_guard(svr_done);

  DINGO_LOG(INFO) << "GetLogLevel request:" << request->ShortDebugString();

  auto* log_detail = response->mutable_log_detail();
  log_detail->set_log_buf_secs(DingoLogger::GetLogBuffSecs());
  log_detail->set_max_log_size(DingoLogger::GetMaxLogSize());
  log_detail->set_stop_logging_if_full_disk(DingoLogger::GetStoppingWhenDiskFull());

  int const min_log_level = DingoLogger::GetMinLogLevel();
  int const min_verbose_level = DingoLogger::GetMinVerboseLevel();

  if (min_log_level > pb::debug::FATAL) {
    DINGO_LOG(ERROR) << "Invalid Log Level:" << min_log_level;
    ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETER,
                            fmt::format("param min_log_level({}) illegal", min_log_level));
    return;
  }

  if (min_log_level == 0 && min_verbose_level > 1) {
    response->set_log_level(static_cast<pb::debug::LogLevel>(0));
  } else {
    response->set_log_level(static_cast<pb::debug::LogLevel>(min_log_level + 1));
  }
}

static LogLevel LogLevelPB2LogLevel(const pb::debug::LogLevel& level) {
  switch (level) {
    case pb::debug::LogLevel::DEBUG:
      return LogLevel::kDEBUG;
    case pb::debug::LogLevel::INFO:
      return LogLevel::kINFO;
    case pb::debug::LogLevel::WARNING:
      return LogLevel::kWARNING;
    case pb::debug::LogLevel::ERROR:
      return LogLevel::kERROR;
    case pb::debug::LogLevel::FATAL:
      return LogLevel::kFATAL;
    default:
      CHECK(false) << "invalid log level: " << level;
  }
}

void DebugServiceImpl::ChangeLogLevel(google::protobuf::RpcController*, const pb::debug::ChangeLogLevelRequest* request,
                                      pb::debug::ChangeLogLevelResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::ClosureGuard done_guard(svr_done);

  DINGO_LOG(INFO) << "ChangeLogLevel request:" << request->ShortDebugString();

  const pb::debug::LogLevel log_level = request->log_level();
  const auto& log_detail = request->log_detail();

  DingoLogger::ChangeGlogLevelUsingDingoLevel(LogLevelPB2LogLevel(log_level), log_detail.verbose());
  DingoLogger::SetLogBuffSecs(log_detail.log_buf_secs());
  DingoLogger::SetMaxLogSize(log_detail.max_log_size());
  DingoLogger::SetStoppingWhenDiskFull(log_detail.stop_logging_if_full_disk());
}

void DebugServiceImpl::GetFs(google::protobuf::RpcController*, const pb::debug::GetFsRequest* request,
                             pb::debug::GetFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::ClosureGuard done_guard(svr_done);

  if (request->fs_id() == 0) {
    auto fs_list = file_system_set_->GetAllFileSystem();
    for (auto& fs : fs_list) {
      *response->add_fses() = fs->GetFsInfo();
    }

  } else {
    auto fs = file_system_set_->GetFileSystem(request->fs_id());
    if (fs != nullptr) {
      *response->add_fses() = fs->GetFsInfo();
    }
  }
}

static void FillPartition(PartitionPtr partition, bool with_inode,
                          pb::debug::GetPartitionResponse::Partition* pb_partition) {
  *pb_partition->mutable_parent_inode() = partition->ParentInode()->Copy();

  auto child_dentries = partition->GetAllChildren();
  for (auto& child_dentry : child_dentries) {
    auto* pb_dentry = pb_partition->add_entries();
    *pb_dentry->mutable_dentry() = child_dentry.Copy();
    auto inode = child_dentry.Inode();
    if (with_inode && inode != nullptr) {
      *pb_dentry->mutable_inode() = inode->Copy();
    }
  }
}

void DebugServiceImpl::GetPartition(google::protobuf::RpcController* controller,
                                    const pb::debug::GetPartitionRequest* request,
                                    pb::debug::GetPartitionResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::ClosureGuard done_guard(svr_done);

  auto fs = GetFileSystem(request->fs_id());
  if (!fs) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  // get all partition
  if (request->parent() == 0) {
    auto partition_map = fs->GetAllPartitionsFromCache();
    for (auto& [_, partition] : partition_map) {
      auto* pb_partition = response->add_partitions();
      FillPartition(partition, request->with_inode(), pb_partition);
    }

    return;
  }

  Context ctx(false, 0);
  PartitionPtr partition;
  auto status = fs->GetPartition(ctx, request->parent(), partition);
  if (!status.ok()) {
    return ServiceHelper::SetError(response->mutable_error(), status);
  }

  auto* pb_partition = response->add_partitions();

  // get all children of one partition
  if (request->name().empty()) {
    FillPartition(partition, request->with_inode(), pb_partition);
  } else {
    Dentry dentry;
    partition->GetChild(request->name(), dentry);
    *pb_partition->mutable_parent_inode() = partition->ParentInode()->Copy();
    auto* pb_dentry = pb_partition->add_entries();
    auto inode = dentry.Inode();
    if (request->with_inode() && inode != nullptr) {
      *pb_dentry->mutable_inode() = inode->Copy();
    }
  }
}

void DebugServiceImpl::GetInode(google::protobuf::RpcController*, const pb::debug::GetInodeRequest* request,
                                pb::debug::GetInodeResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::ClosureGuard done_guard(svr_done);

  auto fs = GetFileSystem(request->fs_id());
  if (!fs) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  Context ctx(!request->use_cache(), 0);

  if (request->inoes().empty() && request->use_cache()) {
    auto inode_map = fs->GetAllInodesFromCache();
    for (auto& [_, inode] : inode_map) {
      *response->add_inodes() = inode->Copy();
    }
  }

  for (const auto& ino : request->inoes()) {
    InodeSPtr inode;
    auto status = fs->GetInode(ctx, ino, inode);
    if (status.ok()) {
      *response->add_inodes() = inode->Copy();
    }
  }
}

void DebugServiceImpl::GetOpenFile(google::protobuf::RpcController*, const pb::debug::GetOpenFileRequest* request,
                                   pb::debug::GetOpenFileResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::ClosureGuard done_guard(svr_done);

  auto fs = GetFileSystem(request->fs_id());
  if (!fs) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  std::vector<FileSessionEntry> file_sessions;
  auto status = fs->GetFileSessionManager().GetAll(file_sessions);
  if (!status.ok()) {
    return ServiceHelper::SetError(response->mutable_error(), status);
  }

  Helper::VectorToPbRepeated(file_sessions, response->mutable_file_sessions());
}

void DebugServiceImpl::TraceWorkerSet(google::protobuf::RpcController*, const pb::debug::TraceWorkerSetRequest* request,
                                      pb::debug::TraceWorkerSetResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::ClosureGuard done_guard(svr_done);

  auto& mds_service = Server::GetInstance().GetMDSService();

  if (request->name() == "read") {
    auto& worker_set = mds_service->GetReadWorkerSet();
    response->set_trace(worker_set->Trace());

  } else if (request->name() == "write") {
    auto& worker_set = mds_service->GetWriteWorkerSet();
    response->set_trace(worker_set->Trace());

  } else {
    ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETER,
                            fmt::format("invalid worker set name: {}", request->name()));
  }
}

void DebugServiceImpl::CleanCache(google::protobuf::RpcController* controller,
                                  const pb::debug::CleanCacheRequest* request, pb::debug::CleanCacheResponse* response,
                                  google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::ClosureGuard done_guard(svr_done);

  auto fs = GetFileSystem(request->fs_id());
  if (fs == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  if (request->cache_type() == pb::debug::CACHE_TYPE_PARTITION) {
    fs->ClearPartitionCache();
  } else if (request->cache_type() == pb::debug::CACHE_TYPE_INODE) {
    fs->ClearInodeCache();
  } else if (request->cache_type() == pb::debug::CACHE_TYPE_CHUNK) {
    fs->ClearChunkCache();
  }
}

void DebugServiceImpl::GetMemoryStats(google::protobuf::RpcController*, const pb::debug::GetMemoryStatsRequest* request,
                                      pb::debug::GetMemoryStatsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::ClosureGuard done_guard(svr_done);

#ifdef USE_TCMALLOC
  auto* tcmalloc = MallocExtension::instance();
  if (tcmalloc == nullptr) {
    response->mutable_error()->set_errcode(pb::error::EINTERNAL);
    response->mutable_error()->set_errmsg("No use tcmalloc");
    return;
  }

  std::string stat_buf(4096, '\0');
  tcmalloc->GetStats(stat_buf.data(), stat_buf.size());
  response->set_memory_stats(stat_buf.c_str());

#else
  response->mutable_error()->set_errcode(pb::error::EINTERNAL);
  response->mutable_error()->set_errmsg("No use tcmalloc");
#endif
}

void DebugServiceImpl::ReleaseFreeMemory(google::protobuf::RpcController*,
                                         const pb::debug::ReleaseFreeMemoryRequest* request,
                                         pb::debug::ReleaseFreeMemoryResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::ClosureGuard done_guard(svr_done);

#ifdef USE_TCMALLOC
  auto* tcmalloc = MallocExtension::instance();
  if (tcmalloc == nullptr) {
    response->mutable_error()->set_errcode(pb::error::EINTERNAL);
    response->mutable_error()->set_errmsg("No use tcmalloc");
    return;
  }

  if (request->is_force()) {
    tcmalloc->ReleaseFreeMemory();
  } else {
    tcmalloc->SetMemoryReleaseRate(request->rate());
  }
#else
  response->mutable_error()->set_errcode(pb::error::EINTERNAL);
  response->mutable_error()->set_errmsg("No use tcmalloc");
#endif
}

}  // namespace mdsv2
}  // namespace dingofs
