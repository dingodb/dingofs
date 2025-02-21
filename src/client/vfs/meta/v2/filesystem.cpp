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

#include "client/vfs/meta/v2/filesystem.h"

#include <cstdint>
#include <string>
#include <vector>

#include "client/vfs/meta/v2/dir_reader.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

const uint32_t kMaxHostNameLength = 255;

const uint32_t kMaxXAttrNameLength = 255;
const uint32_t kMaxXAttrValueLength = 64 * 1024;

const uint32_t kDirReaderInitFhID = 10000;

const std::set<std::string> kXAttrBlackList = {
    "system.posix_acl_access", "system.posix_acl_default", "system.nfs4_acl"};

DEFINE_uint32(read_dir_batch_size, 1024, "Read dir batch size.");

std::string GetHostName() {
  char hostname[kMaxHostNameLength];
  int ret = gethostname(hostname, kMaxHostNameLength);
  if (ret < 0) {
    LOG(ERROR) << "GetHostName fail, ret=" << ret;
    return "";
  }

  return std::string(hostname);
}

MDSV2FileSystem::MDSV2FileSystem(pb::mdsv2::FsInfo fs_info,
                                 const std::string& mount_path,
                                 MDSDiscoveryPtr mds_discovery,
                                 MDSClientPtr mds_client)
    : name_(fs_info.fs_name()),
      mount_path_(mount_path),
      fs_info_(fs_info),
      mds_discovery_(mds_discovery),
      mds_client_(mds_client),
      dir_reader_(kDirReaderInitFhID) {}

MDSV2FileSystem::~MDSV2FileSystem() {}  // NOLINT

Status MDSV2FileSystem::Init() {
  LOG(INFO) << fmt::format("fs_info: {}.", fs_info_.ShortDebugString());
  // mount fs
  if (!MountFs()) {
    LOG(ERROR) << fmt::format("mount fs({}) fail.", name_);
    return Status::MountFailed("mount fs fail");
  }

  return Status::OK();
}

void MDSV2FileSystem::UnInit() {
  // unmount fs
  UnmountFs();
}

bool MDSV2FileSystem::MountFs() {
  std::string hostname = GetHostName();
  if (hostname.empty()) {
    LOG(ERROR) << "get hostname fail.";
    return false;
  }

  pb::mdsv2::MountPoint mount_point;
  mount_point.set_hostname(hostname);
  mount_point.set_port(9999);
  mount_point.set_path(mount_path_);
  mount_point.set_cto(false);

  LOG(INFO) << fmt::format("mount point: {}.", mount_point.ShortDebugString());

  auto status = mds_client_->MountFs(name_, mount_point);
  if (!status.ok() && status.Errno() != pb::error::EEXISTED) {
    LOG(ERROR) << fmt::format("mount fs({}) info fail, mountpoint({}), {}.",
                              name_, mount_path_, status.ToString());
    return false;
  }

  return true;
}

bool MDSV2FileSystem::UnmountFs() {
  std::string hostname = GetHostName();
  if (hostname.empty()) {
    return false;
  }

  pb::mdsv2::MountPoint mount_point;
  mount_point.set_hostname(hostname);
  mount_point.set_port(9999);
  mount_point.set_path(mount_path_);

  auto status = mds_client_->UmountFs(name_, mount_point);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("mount fs({}) info fail, mountpoint({}).", name_,
                              mount_path_);
    return false;
  }

  return true;
}

Status MDSV2FileSystem::Lookup(Ino parent_ino, const std::string& name,
                               Attr* out_attr) {
  auto status = mds_client_->Lookup(parent_ino, name, *out_attr);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Create(Ino parent, const std::string& name,
                               uint32_t uid, uint32_t gid, uint32_t mode,
                               int flags, Attr* attr) {
  return Status::NotSupport("to be implemented");
}

Status MDSV2FileSystem::MkNod(Ino parent_ino, const std::string& name,
                              uint32_t uid, uint32_t gid, uint32_t mode,
                              uint64_t rdev, Attr* out_attr) {
  auto status =
      mds_client_->MkNod(parent_ino, name, uid, gid, mode, rdev, *out_attr);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Open(Ino ino, int flags) {
  LOG(INFO) << fmt::format("Open ino({}).", ino);

  auto status = mds_client_->Open(ino);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Open ino({}) fail, error: {}.", ino,
                              status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Close(Ino ino) {
  LOG(INFO) << fmt::format("Release ino({}).", ino);
  auto status = mds_client_->Release(ino);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Release ino({}) fail, error: {}.", ino,
                              status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::ReadSlice(Ino ino, uint64_t index,
                                  std::vector<Slice>* slices) {
  auto status = mds_client_->ReadSlice(ino, index, slices);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("ReeadSlice ino({}) fail, error: {}.", ino,
                              status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::NewSliceId(uint64_t* id) {
  auto status = mds_client_->NewSliceId(id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("NewSliceId fail, error: {}.", status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::WriteSlice(Ino ino, uint64_t index,
                                   const std::vector<Slice>& slices) {
  auto status = mds_client_->WriteSlice(ino, index, slices);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("WriteSlice ino({}) fail, error: {}.", ino,
                              status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::MkDir(Ino parent, const std::string& name, uint32_t uid,
                              uint32_t gid, uint32_t mode, Attr* out_attr) {
  // auto status =
  //     mds_client_->MkDir(parent, name, uid, gid, mode, rdev, *out_attr);
  // if (!status.ok()) {
  //   return status;
  // }

  return Status::OK();
}

Status MDSV2FileSystem::RmDir(Ino parent, const std::string& name) {
  auto status = mds_client_->RmDir(parent, name);
  if (!status.ok()) {
    return status;
  }
  return Status::OK();
}

// TODO: implement
Status MDSV2FileSystem::OpenDir(Ino ino) {
  LOG(INFO) << fmt::format("OpenDir ino({})", ino);

  // fh = dir_reader_.NewState(ino);

  return Status::NotSupport("to be implemented");
}

Status MDSV2FileSystem::NewDirHandler(Ino ino, bool with_attr,
                                      DirHandler** handler) {
  return Status::NotSupport("to be implemented");
}

Status MDSV2FileSystem::Link(Ino ino, Ino new_parent,
                             const std::string& new_name, Attr* attr) {
  auto status = mds_client_->Link(ino, new_parent, new_name, *attr);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Link({}/{}) to ino({}) fail, error: {}.",
                              new_parent, new_name, ino, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Unlink(Ino parent, const std::string& name) {
  auto status = mds_client_->UnLink(parent, name);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("UnLink({}/{}) fail, error: {}.", parent, name,
                              status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Symlink(Ino parent, const std::string& name,
                                uint32_t uid, uint32_t gid,
                                const std::string& link, Attr* out_attr) {
  auto status = mds_client_->Symlink(parent, name, uid, gid, link, *out_attr);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Symlink({}/{}) fail, symlink({}) error: {}.",
                              parent, name, symlink, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::ReadLink(Ino ino, std::string* link) {
  auto status = mds_client_->ReadLink(ino, *link);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("ReadLink {} fail, error: {}.", ino,
                              status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::GetAttr(Ino ino, Attr* out_attr) {
  auto status = mds_client_->GetAttr(ino, *out_attr);
  if (!status.ok()) {
    return Status::Internal(
        fmt::format("get attr fail, error: {}", ino, status.ToString()));
  }

  return Status::OK();
}

Status MDSV2FileSystem::SetAttr(Ino ino, int set, const Attr& attr,
                                Attr* out_attr) {
  auto status = mds_client_->SetAttr(ino, attr, set, *out_attr);
  if (!status.ok()) {
    return Status::Internal(fmt::format("set attr fail, ino({}) error: {}", ino,
                                        status.ToString()));
  }

  return Status::OK();
}

Status MDSV2FileSystem::GetXattr(Ino ino, const std::string& name,
                                 std::string* value) {
  if (kXAttrBlackList.find(name) != kXAttrBlackList.end()) {
    // LOG(WARNING) << fmt::format("xattr({}) is in black list.", name);
    return Status::OK();
  }

  auto status = mds_client_->GetXAttr(ino, name, *value);

  // if (value.empty()) {
  //   return Status(pb::error::ENO_DATA, "no data");
  // }

  // if (value.size() > kMaxXAttrValueLength) {
  //   return Status(pb::error::EOUT_OF_RANGE, "out of range");
  // }

  return Status::OK();
}

Status MDSV2FileSystem::SetXattr(Ino ino, const std::string& name,
                                 const std::string& value, int flags) {
  auto status = mds_client_->SetXAttr(ino, name, value);
  if (!status.ok()) {
    return Status::Internal(
        fmt::format("set xattr({}/{}) fail, ino({}) error: {}", name, value,
                    ino, status.ToString()));
  }

  return Status::OK();
}

Status MDSV2FileSystem::ListXattr(Ino ino, std::vector<std::string>* xattrs) {
  CHECK(xattrs != nullptr) << "xattrs is null.";

  // auto status = mds_client_->ListXAttr(ino, *xattrs);
  // if (!status.ok()) {
  //   return Status::Internal(fmt::format("list xattr fail, ino({}) error: {}",
  //                                       ino, status.ToString()));
  // }

  return Status::OK();
}

Status MDSV2FileSystem::Rename(Ino old_parent, const std::string& old_name,
                               Ino new_parent, const std::string& new_name) {
  auto status = mds_client_->Rename(old_parent, old_name, new_parent, new_name);
  if (!status.ok()) {
    return Status::Internal(
        fmt::format("rename fail, {}/{} -> {}/{}, error: {}", old_parent,
                    old_name, new_parent, new_name, status.ToString()));
  }

  return Status::OK();
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs