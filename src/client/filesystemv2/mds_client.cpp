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

#include <cstdint>
#include <string>

#include "client/filesystem/meta.h"
#include "dingofs/mdsv2.pb.h"
#include "dingofs/metaserver.pb.h"

namespace dingofs {
namespace client {
namespace filesystem {

bool MDSClient::Init() { return true; }

void MDSClient::Destory() {}

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

static void FillInodeAttr(const pb::mdsv2::Inode& inode, EntryOut* entry_out) {
  auto& attr = entry_out->attr;

  attr.set_inodeid(inode.inode_id());
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

static void FillInodeAttr(const pb::mdsv2::Inode& inode, AttrOut* entry_out) {
  auto& attr = entry_out->attr;

  attr.set_inodeid(inode.inode_id());
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

Status MDSClient::Lookup(uint32_t fs_id, uint64_t parent_ino,
                         const std::string& name, EntryOut* entry_out) {
  pb::mdsv2::LookupRequest request;
  pb::mdsv2::LookupResponse response;

  request.set_fs_id(fs_id);
  request.set_parent_inode_id(parent_ino);
  request.set_name(name);

  auto status = rpc_->SendRequest("MDSService", "Lookup", request, response);
  if (!status.ok()) {
    return status;
  }

  FillInodeAttr(response.inode(), entry_out);

  return Status::OK();
}

Status MDSClient::MkNod(uint32_t fs_id, uint64_t parent_ino,
                        const std::string& name, uint32_t uid, uint32_t gid,
                        mode_t mode, dev_t rdev, EntryOut* entry_out) {
  pb::mdsv2::MkNodRequest request;
  pb::mdsv2::MkNodResponse response;

  request.set_fs_id(fs_id);
  request.set_parent_inode_id(parent_ino);
  request.set_name(name);
  request.set_mode(mode);
  request.set_type(pb::mdsv2::FileType::FILE);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_rdev(rdev);

  request.set_length(0);

  auto status = rpc_->SendRequest("MDSService", "MkNod", request, response);
  if (!status.ok()) {
    return status;
  }

  FillInodeAttr(response.inode(), entry_out);

  return Status::OK();
}

Status MDSClient::MkDir(uint32_t fs_id, uint64_t parent_ino,
                        const std::string& name, uint32_t uid, uint32_t gid,
                        mode_t mode, dev_t rdev, EntryOut* entry_out) {
  pb::mdsv2::MkDirRequest request;
  pb::mdsv2::MkDirResponse response;

  request.set_fs_id(fs_id);
  request.set_parent_inode_id(parent_ino);
  request.set_name(name);
  request.set_mode(mode);
  request.set_type(pb::mdsv2::FileType::DIRECTORY);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_rdev(rdev);

  request.set_length(0);

  auto status = rpc_->SendRequest("MDSService", "MkDir", request, response);
  if (!status.ok()) {
    return status;
  }

  FillInodeAttr(response.inode(), entry_out);

  return Status::OK();
}

Status MDSClient::RmDir(uint32_t fs_id, uint64_t parent_ino,
                        const std::string& name) {
  pb::mdsv2::RmDirRequest request;
  pb::mdsv2::RmDirResponse response;

  request.set_fs_id(fs_id);
  request.set_parent_inode_id(parent_ino);
  request.set_name(name);

  auto status = rpc_->SendRequest("MDSService", "RmDir", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::Link(uint32_t fs_id, uint64_t parent_ino, uint64_t ino,
                       const std::string& name) {
  pb::mdsv2::LinkRequest request;
  pb::mdsv2::LinkResponse response;

  request.set_fs_id(fs_id);
  request.set_parent_inode_id(parent_ino);
  request.set_inode_id(ino);
  request.set_name(name);

  auto status = rpc_->SendRequest("MDSService", "Link", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::UnLink(uint32_t fs_id, uint64_t parent_ino,
                         const std::string& name) {
  pb::mdsv2::UnLinkRequest request;
  pb::mdsv2::UnLinkResponse response;

  request.set_fs_id(fs_id);
  request.set_parent_inode_id(parent_ino);
  request.set_name(name);

  auto status = rpc_->SendRequest("MDSService", "UnLink", request, response);
  if (!status.ok()) {
    return status;
  }
  return Status::OK();
}

Status MDSClient::Symlink(uint32_t fs_id, uint64_t parent_ino,
                          const std::string& name, uint32_t uid, uint32_t gid,
                          const std::string& symlink, EntryOut* entry_out) {
  pb::mdsv2::SymlinkRequest request;
  pb::mdsv2::SymlinkResponse response;

  request.set_fs_id(fs_id);
  request.set_parent_inode_id(parent_ino);
  request.set_name(name);
  request.set_mode(S_IFLNK | 0777);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_symlink(symlink);
  request.set_rdev(0);
  request.set_length(symlink.size());

  auto status = rpc_->SendRequest("MDSService", "Symlink", request, response);
  if (!status.ok()) {
    return status;
  }

  FillInodeAttr(response.inode(), entry_out);

  return Status::OK();
}

Status MDSClient::ReadLink(uint32_t fs_id, uint64_t ino, std::string& symlink) {
  pb::mdsv2::ReadLinkRequest request;
  pb::mdsv2::ReadLinkResponse response;

  request.set_fs_id(fs_id);
  request.set_inode_id(ino);

  auto status = rpc_->SendRequest("MDSService", "ReadLink", request, response);
  if (!status.ok()) {
    return status;
  }

  symlink = response.symlink();

  return Status::OK();
}

Status MDSClient::GetAttr(uint32_t fs_id, uint64_t ino, EntryOut* entry_out) {
  pb::mdsv2::GetAttrRequest request;
  pb::mdsv2::GetAttrResponse response;

  request.set_fs_id(fs_id);
  request.set_inode_id(ino);

  auto status = rpc_->SendRequest("MDSService", "GetAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  FillInodeAttr(response.inode(), entry_out);

  return Status::OK();
}

static uint64_t ToTimestamp(const struct timespec& ts) {
  return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

Status MDSClient::SetAttr(uint32_t fs_id, uint64_t ino, struct stat* attr,
                          int to_set, AttrOut* attr_out) {
  pb::mdsv2::SetAttrRequest request;
  pb::mdsv2::SetAttrResponse response;

  request.set_fs_id(fs_id);
  request.set_inode_id(ino);

  if (to_set & FUSE_SET_ATTR_MODE) {
    request.set_mode(attr->st_mode);
    request.add_update_fields("mode");
  }
  if (to_set & FUSE_SET_ATTR_UID) {
    request.set_uid(attr->st_uid);
    request.add_update_fields("uid");
  }
  if (to_set & FUSE_SET_ATTR_GID) {
    request.set_gid(attr->st_gid);
    request.add_update_fields("gid");
  }

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);

  if (to_set & FUSE_SET_ATTR_ATIME) {
    request.set_atime(ToTimestamp(attr->st_atim));
    request.add_update_fields("atime");

  } else if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
    request.set_atime(ToTimestamp(now));
    request.add_update_fields("atime");
  }

  if (to_set & FUSE_SET_ATTR_MTIME) {
    request.set_mtime(ToTimestamp(attr->st_mtim));
    request.add_update_fields("mtime");

  } else if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
    request.set_mtime(ToTimestamp(now));
    request.add_update_fields("mtime");
  }

  if (to_set & FUSE_SET_ATTR_CTIME) {
    request.set_ctime(ToTimestamp(attr->st_ctim));
    request.add_update_fields("ctime");
  } else {
    request.set_ctime(ToTimestamp(now));
    request.add_update_fields("ctime");
  }

  if (to_set & FUSE_SET_ATTR_SIZE) {
    // todo: Truncate data
    request.set_length(attr->st_size);
  }

  auto status = rpc_->SendRequest("MDSService", "SetAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  FillInodeAttr(response.inode(), attr_out);

  return Status::OK();
}

Status MDSClient::GetXAttr(uint32_t fs_id, uint64_t ino,
                           const std::string& name, std::string& value) {
  pb::mdsv2::GetXAttrRequest request;
  pb::mdsv2::GetXAttrResponse response;

  request.set_fs_id(fs_id);
  request.set_inode_id(ino);
  request.set_name(name);

  auto status = rpc_->SendRequest("MDSService", "GetXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  value = response.value();

  return Status::OK();
}

Status MDSClient::SetXAttr(uint32_t fs_id, uint64_t ino,
                           const std::string& name, const std::string& value) {
  pb::mdsv2::SetXAttrRequest request;
  pb::mdsv2::SetXAttrResponse response;

  request.set_fs_id(fs_id);
  request.set_inode_id(ino);
  request.mutable_xattrs()->insert({name, value});

  auto status = rpc_->SendRequest("MDSService", "SetXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSClient::ListXAttr(uint32_t fs_id, uint64_t ino,
                            std::map<std::string, std::string>& xattrs) {
  pb::mdsv2::ListXAttrRequest request;
  pb::mdsv2::ListXAttrResponse response;

  request.set_fs_id(fs_id);
  request.set_inode_id(ino);

  auto status = rpc_->SendRequest("MDSService", "ListXAttr", request, response);
  if (!status.ok()) {
    return status;
  }

  for (const auto& [name, value] : response.xattrs()) {
    xattrs[name] = value;
  }

  return Status::OK();
}

Status MDSClient::Rename() { return Status::OK(); }

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs