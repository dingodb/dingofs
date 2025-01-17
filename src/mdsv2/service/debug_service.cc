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

#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/inode.h"
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
    if (fs) {
      *response->add_fses() = fs->FsInfo();
    }
  }
}

void DebugServiceImpl::GetDentry(google::protobuf::RpcController*, const pb::debug::GetDentryRequest* request,
                                 pb::debug::GetDentryResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::ClosureGuard done_guard(svr_done);

  auto fs = GetFileSystem(request->fs_id());
  if (!fs) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  DentrySetPtr dentry_set;
  auto status = fs->GetDentrySet(request->parent_ino(), dentry_set);
  if (!status.ok()) {
    return ServiceHelper::SetError(response->mutable_error(), status);
  }

  *response->mutable_parent_inode() = dentry_set->ParentInode()->CopyTo();

  auto child_dentries = dentry_set->GetAllChildren();
  for (auto& dentry : child_dentries) {
    auto* entry = response->add_child_entries();
    *entry->mutable_dentry() = dentry.CopyTo();

    auto inode = dentry.Inode();
    if (request->with_inode() && inode != nullptr) {
      *entry->mutable_inode() = inode->CopyTo();
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

  for (const auto& ino : request->inoes()) {
    InodePtr inode;
    if (request->use_cache()) {
      inode = fs->GetInodeFromCache(ino);
    } else {
      fs->GetInodeFromStore(ino, inode);
    }

    if (inode != nullptr) {
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
