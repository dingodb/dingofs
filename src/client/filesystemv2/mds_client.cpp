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

#include "client/filesystemv2/mds_client.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstdint>
#include <string>
#include <utility>

#include "client/filesystem/meta.h"
#include "dingofs/mdsv2.pb.h"
#include "dingofs/metaserver.pb.h"
#include "mdsv2/common/constant.h"

namespace dingofs {
namespace client {
namespace filesystem {

static pb::metaserver::FsFileType ToFsFileType(pb::mdsv2::FileType type) {
  switch (type) {
    case pb::mdsv2::FileType::FILE:
      return pb::metaserver::FsFileType::TYPE_FILE;
    case pb::mdsv2::FileType::DIRECTORY:
      return pb::metaserver::FsFileType::TYPE_DIRECTORY;
    case pb::mdsv2::FileType::SYM_LINK:
      return pb::metaserver::FsFileType::TYPE_SYM_LINK;
    default:
      return pb::metaserver::FsFileType::TYPE_FILE;
  }
}

static void FillInodeAttr(const pb::mdsv2::Inode& inode, EntryOut& entry_out) {
  auto& attr = entry_out.attr;

  attr.set_inodeid(inode.ino());
  attr.set_fsid(inode.fs_id());
  attr.set_length(inode.length());

  attr.set_ctime(inode.ctime() / 1000000000);
  attr.set_ctime_ns(inode.ctime() % 1000000000);
  attr.set_mtime(inode.mtime() / 1000000000);
  attr.set_mtime_ns(inode.mtime() % 1000000000);
  attr.set_atime(inode.atime() / 1000000000);
  attr.set_atime_ns(inode.atime() % 1000000000);

  attr.set_mode(attr.mode());
  attr.set_uid(inode.uid());
  attr.set_gid(inode.gid());
  attr.set_nlink(inode.nlink());

  attr.set_rdev(inode.rdev());
  attr.set_dtime(inode.dtime());
  attr.set_type(ToFsFileType(inode.type()));

  attr.set_symlink(inode.symlink());

  *(attr.mutable_xattr()) = inode.xattr();
}

static void FillInodeAttr(const pb::mdsv2::Inode& inode, AttrOut& entry_out) {
  auto& attr = entry_out.attr;

  attr.set_inodeid(inode.ino());
  attr.set_fsid(inode.fs_id());
  attr.set_length(inode.length());

  attr.set_ctime(inode.ctime() / 1000000000);
  attr.set_ctime_ns(inode.ctime() % 1000000000);
  attr.set_mtime(inode.mtime() / 1000000000);
  attr.set_mtime_ns(inode.mtime() % 1000000000);
  attr.set_atime(inode.atime() / 1000000000);
  attr.set_atime_ns(inode.atime() % 1000000000);

  attr.set_mode(attr.mode());
  attr.set_uid(inode.uid());
  attr.set_gid(inode.gid());
  attr.set_nlink(inode.nlink());

  attr.set_rdev(inode.rdev());
  attr.set_dtime(inode.dtime());
  attr.set_type(ToFsFileType(inode.type()));

  attr.set_symlink(inode.symlink());

  *(attr.mutable_xattr()) = inode.xattr();
}

bool MDSClient::Init() { return true; }

void MDSClient::Destory() {}

bool MDSClient::SetEndpoint(const std::string& ip, int port, bool is_default) {
  return rpc_->AddEndpoint(ip, port, is_default);
}

Status MDSClient::GetFsInfo(const std::string& name,
                            pb::mdsv2::FsInfo& fs_info) {
  pb::mdsv2::GetFsInfoRequest request;
  pb::mdsv2::GetFsInfoResponse response;

  request.set_fs_name(name);

  auto status = rpc_->SendRequest("MDSService", "GetFsInfo", request, response);
  if (!status.ok()) {
    return status;
  }

  fs_info = response.fs_info();

  return Status::OK();
}

Status MDSClient::MountFs(const std::string& name,
                          const pb::mdsv2::MountPoint& mount_point) {
  pb::mdsv2::MountFsRequest request;
  pb::mdsv2::MountFsResponse response;

  request.set_fs_name(name);
  request.mutable_mount_point()->CopyFrom(mount_point);

  auto status = rpc_->SendRequest("MDSService", "MountFs", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::UmountFs(const std::string& name,
                           const pb::mdsv2::MountPoint& mount_point) {
  pb::mdsv2::UmountFsRequest request;
  pb::mdsv2::UmountFsResponse response;

  request.set_fs_name(name);
  request.mutable_mount_point()->CopyFrom(mount_point);

  auto status = rpc_->SendRequest("MDSService", "UmountFs", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::Lookup(uint64_t parent_ino, const std::string& name,
                         EntryOut& entry_out) {
  pb::mdsv2::LookupRequest request;
  pb::mdsv2::LookupResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent_ino(parent_ino);
  request.set_name(name);

  auto status = rpc_->SendRequest("MDSService", "Lookup", request, response);
  if (!status.ok()) {
    return status;
  }

  entry_out.inode = response.inode();

  return Status::OK();
}

Status MDSClient::MkNod(uint64_t parent_ino, const std::string& name,
                        uint32_t uid, uint32_t gid, mode_t mode, dev_t rdev,
                        EntryOut& entry_out) {
  pb::mdsv2::MkNodRequest request;
  pb::mdsv2::MkNodResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent_ino(parent_ino);
  request.set_name(name);
  request.set_mode(mode);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_rdev(rdev);

  request.set_length(0);

  auto status = rpc_->SendRequest("MDSService", "MkNod", request, response);
  if (!status.ok()) {
    return status;
  }

  entry_out.inode = response.inode();

  return Status::OK();
}

Status MDSClient::MkDir(uint64_t parent_ino, const std::string& name,
                        uint32_t uid, uint32_t gid, mode_t mode, dev_t rdev,
                        EntryOut& entry_out) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  pb::mdsv2::MkDirRequest request;
  pb::mdsv2::MkDirResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent_ino(parent_ino);
  request.set_name(name);
  request.set_mode(mode);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_rdev(rdev);

  request.set_length(0);

  auto status = rpc_->SendRequest("MDSService", "MkDir", request, response);
  if (!status.ok()) {
    return status;
  }

  entry_out.inode = response.inode();

  return Status::OK();
}

Status MDSClient::RmDir(uint64_t parent_ino, const std::string& name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  pb::mdsv2::RmDirRequest request;
  pb::mdsv2::RmDirResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent_ino(parent_ino);
  request.set_name(name);

  auto status = rpc_->SendRequest("MDSService", "RmDir", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::ReadDir(
    uint64_t ino, std::string& last_name, uint32_t limit, bool with_attr,
    std::vector<pb::mdsv2::ReadDirResponse::Entry>& entries) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  pb::mdsv2::ReadDirRequest request;
  pb::mdsv2::ReadDirResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_last_name(last_name);
  request.set_limit(limit);
  request.set_with_attr(with_attr);

  auto status = rpc_->SendRequest("MDSService", "ReadDir", request, response);
  if (!status.ok()) {
    return status;
  }

  entries.resize(response.entries_size());
  for (int i = 0; i < response.entries_size(); ++i) {
    entries[i].Swap(response.mutable_entries(i));
  }

  return Status::OK();
}

Status MDSClient::Open(uint64_t ino) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  pb::mdsv2::OpenRequest request;
  pb::mdsv2::OpenResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status = rpc_->SendRequest("MDSService", "Open", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::Release(uint64_t ino) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  pb::mdsv2::ReleaseRequest request;
  pb::mdsv2::ReleaseResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status = rpc_->SendRequest("MDSService", "Release", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::Link(uint64_t parent_ino, uint64_t ino,
                       const std::string& name, EntryOut& entry_out) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  pb::mdsv2::LinkRequest request;
  pb::mdsv2::LinkResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_new_parent_ino(parent_ino);
  request.set_new_name(name);

  auto status = rpc_->SendRequest("MDSService", "Link", request, response);
  if (!status.ok()) {
    return status;
  }

  entry_out.inode = response.inode();

  return Status::OK();
}

Status MDSClient::UnLink(uint64_t parent_ino, const std::string& name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  pb::mdsv2::UnLinkRequest request;
  pb::mdsv2::UnLinkResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent_ino(parent_ino);
  request.set_name(name);

  auto status = rpc_->SendRequest("MDSService", "UnLink", request, response);
  if (!status.ok()) {
    return status;
  }
  return Status::OK();
}

Status MDSClient::Symlink(uint64_t parent_ino, const std::string& name,
                          uint32_t uid, uint32_t gid,
                          const std::string& symlink, EntryOut& entry_out) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  pb::mdsv2::SymlinkRequest request;
  pb::mdsv2::SymlinkResponse response;

  request.set_fs_id(fs_id_);
  request.set_symlink(symlink);

  request.set_new_parent_ino(parent_ino);
  request.set_new_name(name);
  request.set_uid(uid);
  request.set_gid(gid);

  auto status = rpc_->SendRequest("MDSService", "Symlink", request, response);
  if (!status.ok()) {
    return status;
  }

  entry_out.inode = response.inode();

  return Status::OK();
}

Status MDSClient::ReadLink(uint64_t ino, std::string& symlink) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  pb::mdsv2::ReadLinkRequest request;
  pb::mdsv2::ReadLinkResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status = rpc_->SendRequest("MDSService", "ReadLink", request, response);
  if (!status.ok()) {
    return status;
  }

  symlink = response.symlink();

  return Status::OK();
}

Status MDSClient::GetAttr(uint64_t ino, AttrOut& entry_out) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  pb::mdsv2::GetAttrRequest request;
  pb::mdsv2::GetAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status = rpc_->SendRequest("MDSService", "GetAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  entry_out.inode = response.inode();

  return Status::OK();
}

static uint64_t ToTimestamp(const struct timespec& ts) {
  return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

Status MDSClient::SetAttr(uint64_t ino, struct stat* attr, int to_set,
                          AttrOut& attr_out) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  pb::mdsv2::SetAttrRequest request;
  pb::mdsv2::SetAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  uint32_t temp_to_set = 0;
  if (to_set & FUSE_SET_ATTR_MODE) {
    request.set_mode(attr->st_mode);
    temp_to_set |= mdsv2::kSetAttrMode;
  }
  if (to_set & FUSE_SET_ATTR_UID) {
    request.set_uid(attr->st_uid);
    temp_to_set |= mdsv2::kSetAttrUid;
  }
  if (to_set & FUSE_SET_ATTR_GID) {
    request.set_gid(attr->st_gid);
    temp_to_set |= mdsv2::kSetAttrGid;
  }

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);

  if (to_set & FUSE_SET_ATTR_ATIME) {
    request.set_atime(ToTimestamp(attr->st_atim));
    temp_to_set |= mdsv2::kSetAttrAtime;

  } else if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
    request.set_atime(ToTimestamp(now));
    temp_to_set |= mdsv2::kSetAttrAtime;
  }

  if (to_set & FUSE_SET_ATTR_MTIME) {
    request.set_mtime(ToTimestamp(attr->st_mtim));
    temp_to_set |= mdsv2::kSetAttrMtime;

  } else if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
    request.set_mtime(ToTimestamp(now));
    temp_to_set |= mdsv2::kSetAttrMtime;
  }

  if (to_set & FUSE_SET_ATTR_CTIME) {
    request.set_ctime(ToTimestamp(attr->st_ctim));
    temp_to_set |= mdsv2::kSetAttrCtime;
  } else {
    request.set_ctime(ToTimestamp(now));
    temp_to_set |= mdsv2::kSetAttrCtime;
  }

  if (to_set & FUSE_SET_ATTR_SIZE) {
    // todo: Truncate data
    request.set_length(attr->st_size);
  }

  request.set_to_set(temp_to_set);

  auto status = rpc_->SendRequest("MDSService", "SetAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  attr_out.inode = response.inode();

  return Status::OK();
}

Status MDSClient::GetXAttr(uint64_t ino, const std::string& name,
                           std::string& value) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  pb::mdsv2::GetXAttrRequest request;
  pb::mdsv2::GetXAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_name(name);

  auto status = rpc_->SendRequest("MDSService", "GetXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  value = response.value();

  return Status::OK();
}

Status MDSClient::SetXAttr(uint64_t ino, const std::string& name,
                           const std::string& value) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  pb::mdsv2::SetXAttrRequest request;
  pb::mdsv2::SetXAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.mutable_xattrs()->insert({name, value});

  auto status = rpc_->SendRequest("MDSService", "SetXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::ListXAttr(uint64_t ino,
                            std::map<std::string, std::string>& xattrs) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";
  pb::mdsv2::ListXAttrRequest request;
  pb::mdsv2::ListXAttrResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status = rpc_->SendRequest("MDSService", "ListXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  for (const auto& [name, value] : response.xattrs()) {
    xattrs[name] = value;
  }

  return Status::OK();
}

Status MDSClient::Rename(uint64_t old_parent_ino, const std::string& old_name,
                         uint64_t new_parent_ino, const std::string& new_name) {
  CHECK(fs_id_ != 0) << "fs_id is invalid.";

  pb::mdsv2::RenameRequest request;
  pb::mdsv2::RenameResponse response;

  request.set_fs_id(fs_id_);
  request.set_old_parent_ino(old_parent_ino);
  request.set_old_name(old_name);
  request.set_new_parent_ino(new_parent_ino);
  request.set_new_name(new_name);

  auto status = rpc_->SendRequest("MDSService", "Rename", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs