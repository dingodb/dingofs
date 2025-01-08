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

#include "dingofs/src/client/filesystemv2/filesystem.h"

#include <cstddef>

namespace dingofs {
namespace client {
namespace filesystem {

const uint32_t kMaxXAttrNameLength = 255;
const uint32_t kMaxXAttrValueLength = 64 * 1024;

bool MDSV2FileSystem::Init() { return true; }

void MDSV2FileSystem::UnInit() {}

Status MDSV2FileSystem::Lookup(uint64_t parent_ino, const std::string& name,
                               EntryOut* entry_out) {
  auto status =
      mds_client_->Lookup(fs_info_.fs_id(), parent_ino, name, entry_out);

  return Status::OK();
}

Status MDSV2FileSystem::MkNod(uint64_t parent_ino, const std::string& name,
                              uint32_t uid, uint32_t gid, mode_t mode,
                              dev_t rdev, EntryOut* entry_out) {
  auto status = mds_client_->MkNod(fs_info_.fs_id(), parent_ino, name, uid, gid,
                                   mode, rdev, entry_out);
  return Status::OK();
}

Status MDSV2FileSystem::MkDir(uint64_t parent_ino, const std::string& name,
                              uint32_t uid, uint32_t gid, mode_t mode,
                              dev_t rdev, EntryOut* entry_out) {
  auto status = mds_client_->MkDir(fs_info_.fs_id(), parent_ino, name, uid, gid,
                                   mode, rdev, entry_out);
  return Status::OK();
}

Status MDSV2FileSystem::RmDir(uint64_t parent_ino, const std::string& name) {
  auto status = mds_client_->RmDir(fs_info_.fs_id(), parent_ino, name);
  return Status::OK();
}

Status MDSV2FileSystem::Link(uint64_t parent_ino, uint64_t ino,
                             const std::string& name) {
  auto status = mds_client_->Link(fs_info_.fs_id(), parent_ino, ino, name);
  return Status::OK();
}

Status MDSV2FileSystem::UnLink(uint64_t parent_ino, const std::string& name) {
  auto status = mds_client_->UnLink(fs_info_.fs_id(), parent_ino, name);
  return Status::OK();
}

Status MDSV2FileSystem::Symlink(uint64_t parent_ino, const std::string& name,
                                uint32_t uid, uint32_t gid,
                                const std::string& symlink,
                                EntryOut* entry_out) {
  auto status = mds_client_->Symlink(fs_info_.fs_id(), parent_ino, name, uid,
                                     gid, symlink, entry_out);
  return Status::OK();
}

Status MDSV2FileSystem::ReadLink(uint64_t ino, std::string& symlink) {
  auto status = mds_client_->ReadLink(fs_info_.fs_id(), ino, symlink);
  return Status::OK();
}

Status MDSV2FileSystem::GetAttr(uint64_t ino, EntryOut* entry_out) {
  auto status = mds_client_->GetAttr(fs_info_.fs_id(), ino, entry_out);
  return Status::OK();
}

Status MDSV2FileSystem::SetAttr(uint64_t ino, struct stat* attr, int to_set,
                                AttrOut* attr_out) {
  auto status =
      mds_client_->SetAttr(fs_info_.fs_id(), ino, attr, to_set, attr_out);
  return Status::OK();
}

DINGOFS_ERROR MDSV2FileSystem::GetXAttr(uint64_t ino, const std::string& name,
                                        size_t size, std::string& value) {
  auto status = mds_client_->GetXAttr(fs_info_.fs_id(), ino, name, value);

  if (value.empty()) {
    return DINGOFS_ERROR::NODATA;
  }

  if (value.size() > kMaxXAttrValueLength || value.size() > size) {
    return DINGOFS_ERROR::OUT_OF_RANGE;
  }

  return DINGOFS_ERROR::OK;
}

Status MDSV2FileSystem::SetXAttr(uint64_t ino, const std::string& name,
                                 const std::string& value) {
  auto status = mds_client_->SetXAttr(fs_info_.fs_id(), ino, name, value);
  return Status::OK();
}

DINGOFS_ERROR MDSV2FileSystem::ListXAttr(uint64_t ino, size_t size,
                                         std::string& out_names) {
  if (size == 0) {
    return DINGOFS_ERROR::OK;
  }

  std::map<std::string, std::string> xattrs;
  auto status = mds_client_->ListXAttr(fs_info_.fs_id(), ino, xattrs);

  out_names.reserve(4096);
  for (const auto& [name, value] : xattrs) {
    out_names.append(name);
    out_names.push_back('\0');
  }

  return out_names.size() <= size ? DINGOFS_ERROR::OK
                                  : DINGOFS_ERROR::OUT_OF_RANGE;
}

Status MDSV2FileSystem::Rename() {
  auto status = mds_client_->Rename();
  return Status::OK();
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs