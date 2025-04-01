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

#include "dingofs/debug.pb.h"
#include "mdsv2/common/context.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/filesystem/partition.h"
#include "mdsv2/service/service_helper.h"

namespace dingofs {
namespace mdsv2 {

FileSystemPtr DebugServiceImpl::GetFileSystem(uint32_t fs_id) { return file_system_set_->GetFileSystem(fs_id); }

void DebugServiceImpl::GetFs(google::protobuf::RpcController*, const pb::debug::GetFsRequest* request,
                             pb::debug::GetFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::ClosureGuard done_guard(svr_done);

  if (request->fs_id() == 0) {
    auto fs_list = file_system_set_->GetAllFileSystem();
    for (auto& fs : fs_list) {
      *response->add_fses() = fs->FsInfo();
    }

  } else {
    auto fs = file_system_set_->GetFileSystem(request->fs_id());
    if (fs != nullptr) {
      *response->add_fses() = fs->FsInfo();
    }
  }
}

static void FillPartition(PartitionPtr partition, bool with_inode,
                          pb::debug::GetPartitionResponse::Partition* pb_partition) {
  *pb_partition->mutable_parent_inode() = partition->ParentInode()->CopyTo();

  auto child_dentries = partition->GetAllChildren();
  for (auto& child_dentry : child_dentries) {
    auto* pb_dentry = pb_partition->add_entries();
    *pb_dentry->mutable_dentry() = child_dentry.CopyTo();
    auto inode = child_dentry.Inode();
    if (with_inode && inode != nullptr) {
      *pb_dentry->mutable_inode() = inode->CopyTo();
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
  if (request->parent_ino() == 0) {
    auto partition_map = fs->GetAllPartitionsFromCache();
    for (auto& [_, partition] : partition_map) {
      auto* pb_partition = response->add_partitions();
      FillPartition(partition, request->with_inode(), pb_partition);
    }

    return;
  }

  Context ctx(false, 0);
  PartitionPtr partition;
  auto status = fs->GetPartition(ctx, request->parent_ino(), partition);
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
    *pb_partition->mutable_parent_inode() = partition->ParentInode()->CopyTo();
    auto* pb_dentry = pb_partition->add_entries();
    auto inode = dentry.Inode();
    if (request->with_inode() && inode != nullptr) {
      *pb_dentry->mutable_inode() = inode->CopyTo();
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
      *response->add_inodes() = inode->CopyTo();
    }
  }

  for (const auto& ino : request->inoes()) {
    InodePtr inode;
    auto status = fs->GetInode(ctx, ino, inode);
    if (status.ok()) {
      *response->add_inodes() = inode->CopyTo();
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

  auto states = fs->GetOpenFiles().GetAllState();
  for (const auto& state : states) {
    auto* open_file = response->add_open_files();
    open_file->set_ino(state.inode->Ino());
    open_file->set_ref_count(state.ref_count);
  }
}

}  // namespace mdsv2
}  // namespace dingofs
