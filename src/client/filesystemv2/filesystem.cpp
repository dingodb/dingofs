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

#include "client/filesystemv2/filesystem.h"

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "client/filesystemv2/dir_reader.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"

namespace dingofs {
namespace client {
namespace filesystem {

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

bool MDSV2FileSystem::Init() {
  LOG(INFO) << fmt::format("fs_info: {}.", fs_info_.ShortDebugString());
  // mount fs
  if (!MountFs()) {
    LOG(ERROR) << fmt::format("mount fs({}) fail.", name_);
    return false;
  }

  return true;
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
  if (!status.ok() && status.error_code() != pb::error::EEXISTED) {
    LOG(ERROR) << fmt::format("mount fs({}) info fail, mountpoint({}), {}.",
                              name_, mount_path_, status.error_cstr());
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

Status MDSV2FileSystem::Lookup(uint64_t parent_ino, const std::string& name,
                               EntryOut& entry_out) {
  auto status = mds_client_->Lookup(parent_ino, name, entry_out);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::MkNod(uint64_t parent_ino, const std::string& name,
                              uint32_t uid, uint32_t gid, mode_t mode,
                              dev_t rdev, EntryOut& entry_out) {
  auto status =
      mds_client_->MkNod(parent_ino, name, uid, gid, mode, rdev, entry_out);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Open(uint64_t ino) {
  LOG(INFO) << fmt::format("Open ino({}).", ino);

  auto status = mds_client_->Open(ino);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Open ino({}) fail, error: {}.", ino,
                              status.error_str());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Release(uint64_t ino) {
  LOG(INFO) << fmt::format("Release ino({}).", ino);
  auto status = mds_client_->Release(ino);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Release ino({}) fail, error: {}.", ino,
                              status.error_str());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Read(uint64_t ino, off_t off, size_t size, char* buf,
                             size_t& rsize) {
  LOG(INFO) << fmt::format("Read ino({}) off({}) size({}).", ino, off, size);

  std::string data(size, 0);

  memcpy(buf, data.data(), data.size());
  rsize = data.size();

  return Status::OK();
}

Status MDSV2FileSystem::Write(uint64_t ino, off_t off, const char* buf,
                              size_t size, size_t& wsize) {
  LOG(INFO) << fmt::format("Write ino({}) off({}) size({}).", ino, off, size);

  std::string data(size, 0);

  memcpy(const_cast<char*>(data.data()), buf, data.size());

  wsize = data.size();

  return Status::OK();
}

Status MDSV2FileSystem::Flush(uint64_t ino) {
  LOG(INFO) << fmt::format("Flush ino({}).", ino);
  return Status::OK();
}

Status MDSV2FileSystem::Fsync(uint64_t ino, int data_sync) {
  LOG(INFO) << fmt::format("Fsync ino({}) data_sync({}).", ino, data_sync);
  return Status::OK();
}

Status MDSV2FileSystem::MkDir(uint64_t parent_ino, const std::string& name,
                              uint32_t uid, uint32_t gid, mode_t mode,
                              dev_t rdev, EntryOut& entry_out) {
  auto status =
      mds_client_->MkDir(parent_ino, name, uid, gid, mode, rdev, entry_out);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::RmDir(uint64_t parent_ino, const std::string& name) {
  auto status = mds_client_->RmDir(parent_ino, name);
  if (!status.ok()) {
    return status;
  }
  return Status::OK();
}

Status MDSV2FileSystem::OpenDir(uint64_t ino, uint64_t& fh) {
  LOG(INFO) << fmt::format("OpenDir ino({}) fh({}).", ino, fh);

  fh = dir_reader_.NewState(ino);

  return Status::OK();
}

Status MDSV2FileSystem::ReadDir(uint64_t fh, uint64_t ino,
                                ReadDirHandler handler) {
  LOG(INFO) << fmt::format("ReadDir ino({}) fh({}).", ino, fh);

  auto state = dir_reader_.GetState(fh);
  if (state == nullptr) {
    return Status(pb::error::ENO_DATA, "no data");
  }

  for (;;) {
    for (; state->offset < state->entries.size(); ++state->offset) {
      const auto& entry = state->entries[state->offset];
      if (!handler(entry.name(), entry.ino())) {
        return Status::OK();
      }
    }

    if (state->is_end) {
      LOG(INFO) << fmt::format("ReadDir({}/{}) end.", ino, fh);
      return Status::OK();
    }

    std::vector<DirReader::Entry> entries;
    auto status = mds_client_->ReadDir(
        ino, state->last_name, FLAGS_read_dir_batch_size, false, entries);
    if (!status.ok()) {
      LOG(ERROR) << fmt::format("ReadDir({}/{}) fail, error: {}.", ino, fh,
                                status.error_str());
      return status;
    }

    if (entries.size() == FLAGS_read_dir_batch_size) {
      state->last_name = entries.back().name();
    } else {
      state->is_end = true;
    }

    state->entries.swap(entries);
    state->offset = 0;
  }

  return Status::OK();
}

Status MDSV2FileSystem::ReadDirPlus(uint64_t fh, uint64_t ino,
                                    ReadDirPlusHandler handler) {
  LOG(INFO) << fmt::format("ReadDirPlus ino({}) fh({}).", ino, fh);

  auto state = dir_reader_.GetState(fh);
  if (state == nullptr) {
    return Status(pb::error::ENO_DATA, "no data");
  }

  for (;;) {
    for (; state->offset < state->entries.size(); ++state->offset) {
      const auto& entry = state->entries[state->offset];
      if (!handler(entry.name(), entry.inode())) {
        return Status::OK();
      }
    }

    if (state->is_end) {
      LOG(INFO) << fmt::format("ReadDir({}/{}) end.", ino, fh);
      return Status::OK();
    }

    std::vector<DirReader::Entry> entries;
    auto status = mds_client_->ReadDir(
        ino, state->last_name, FLAGS_read_dir_batch_size, true, entries);
    if (!status.ok()) {
      LOG(ERROR) << fmt::format("ReadDir({}/{}) fail, error: {}.", ino, fh,
                                status.error_str());
      return status;
    }

    if (entries.size() == FLAGS_read_dir_batch_size) {
      state->last_name = entries.back().name();
    } else {
      state->is_end = true;
    }

    state->entries.swap(entries);
    state->offset = 0;
  }

  return Status::OK();
}

Status MDSV2FileSystem::ReleaseDir(uint64_t ino, uint64_t fh) {
  LOG(INFO) << fmt::format("ReleaseDir ino({}) fh({}).", ino, fh);

  dir_reader_.DeleteState(fh);

  return Status::OK();
}

Status MDSV2FileSystem::Link(uint64_t ino, uint64_t new_parent_ino,
                             const std::string& new_name, EntryOut& entry_out) {
  auto status = mds_client_->Link(ino, new_parent_ino, new_name, entry_out);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Link({}/{}) to ino({}) fail, error: {}.",
                              new_parent_ino, new_name, ino,
                              status.error_str());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::UnLink(uint64_t parent_ino, const std::string& name) {
  auto status = mds_client_->UnLink(parent_ino, name);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("UnLink({}/{}) fail, error: {}.", parent_ino,
                              name, status.error_str());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Symlink(uint64_t parent_ino, const std::string& name,
                                uint32_t uid, uint32_t gid,
                                const std::string& symlink,
                                EntryOut& entry_out) {
  auto status =
      mds_client_->Symlink(parent_ino, name, uid, gid, symlink, entry_out);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Symlink({}/{}) fail, symlink({}) error: {}.",
                              parent_ino, name, symlink, status.error_str());
    return status;
  }
  return Status::OK();
}

Status MDSV2FileSystem::ReadLink(uint64_t ino, std::string& symlink) {
  auto status = mds_client_->ReadLink(ino, symlink);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("ReadLink {} fail, error: {}.", ino,
                              status.error_str());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::GetAttr(uint64_t ino, AttrOut& entry_out) {
  auto status = mds_client_->GetAttr(ino, entry_out);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, fmt::format("get attr fail, error: {}",
                                                    ino, status.error_str()));
  }

  return Status::OK();
}

Status MDSV2FileSystem::SetAttr(uint64_t ino, struct stat* attr, int to_set,
                                AttrOut& attr_out) {
  auto status = mds_client_->SetAttr(ino, attr, to_set, attr_out);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL,
                  fmt::format("set attr fail, ino({}) error: {}", ino,
                              status.error_str()));
  }

  return Status::OK();
}

Status MDSV2FileSystem::GetXAttr(uint64_t ino, const std::string& name,
                                 std::string& value) {
  if (kXAttrBlackList.find(name) != kXAttrBlackList.end()) {
    // LOG(WARNING) << fmt::format("xattr({}) is in black list.", name);
    return Status::OK();
  }

  auto status = mds_client_->GetXAttr(ino, name, value);

  if (value.empty()) {
    return Status(pb::error::ENO_DATA, "no data");
  }

  if (value.size() > kMaxXAttrValueLength) {
    return Status(pb::error::EOUT_OF_RANGE, "out of range");
  }

  return Status::OK();
}

Status MDSV2FileSystem::SetXAttr(uint64_t ino, const std::string& name,
                                 const std::string& value) {
  auto status = mds_client_->SetXAttr(ino, name, value);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL,
                  fmt::format("set xattr({}/{}) fail, ino({}) error: {}", name,
                              value, ino, status.error_str()));
  }

  return Status::OK();
}

Status MDSV2FileSystem::ListXAttr(uint64_t ino, size_t size,
                                  std::string& out_names) {
  if (size == 0) {
    return Status::OK();
  }

  std::map<std::string, std::string> xattrs;
  auto status = mds_client_->ListXAttr(ino, xattrs);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL,
                  fmt::format("list xattr fail, ino({}) error: {}", ino,
                              status.error_str()));
  }

  out_names.reserve(4096);
  for (const auto& [name, value] : xattrs) {
    out_names.append(name);
    out_names.push_back('\0');
  }

  return out_names.size() <= size
             ? Status::OK()
             : Status(pb::error::EOUT_OF_RANGE, "out of range");
}

Status MDSV2FileSystem::Rename(uint64_t old_parent_ino,
                               const std::string& old_name,
                               uint64_t new_parent_ino,
                               const std::string& new_name) {
  auto status =
      mds_client_->Rename(old_parent_ino, old_name, new_parent_ino, new_name);
  if (!status.ok()) {
    return Status(
        pb::error::EINTERNAL,
        fmt::format("rename fail, {}/{} -> {}/{}, error: {}", old_parent_ino,
                    old_name, new_parent_ino, new_name, status.error_str()));
  }

  return Status::OK();
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs