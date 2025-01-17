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

#include "client/filesystemv2/dummy_filesystem.h"

#include <fcntl.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "dingofs/error.pb.h"
#include "mdsv2/common/status.h"

static const uint32_t kFsID = 10000;
static const uint64_t kRootIno = 1;

namespace dingofs {
namespace client {
namespace filesystem {

ReadDirStateMemo::ReadDirStateMemo() {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

ReadDirStateMemo::~ReadDirStateMemo() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destroy mutex fail.";
}

uint64_t ReadDirStateMemo::NewState() {
  BAIDU_SCOPED_LOCK(mutex_);

  uint64_t id = GenID();
  state_map_[id] = State{};

  return id;
}

bool ReadDirStateMemo::GetState(uint64_t fh, ReadDirStateMemo::State& state) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = state_map_.find(fh);
  if (it == state_map_.end()) {
    return false;
  }

  state = it->second;

  return true;
}

void ReadDirStateMemo::UpdateState(uint64_t fh,
                                   const ReadDirStateMemo::State& state) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = state_map_.find(fh);
  if (it != state_map_.end()) {
    it->second = state;
  }
}

void ReadDirStateMemo::DeleteState(uint64_t fh) {
  BAIDU_SCOPED_LOCK(mutex_);

  state_map_.erase(fh);
}

OpenFileMemo::OpenFileMemo() {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}
OpenFileMemo::~OpenFileMemo() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destroy mutex fail.";
}

bool OpenFileMemo::IsOpened(uint64_t ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto iter = file_map_.find(ino);
  return iter != file_map_.end();
}

void OpenFileMemo::Open(uint64_t ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto iter = file_map_.find(ino);
  if (iter != file_map_.end()) {
    iter->second.ref_count++;
    return;
  }

  State state;
  state.ref_count = 1;
  file_map_[ino] = state;
}

void OpenFileMemo::Close(uint64_t ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto iter = file_map_.find(ino);
  if (iter == file_map_.end()) {
    return;
  }

  CHECK_GT(iter->second.ref_count, 0);
  iter->second.ref_count--;

  if (iter->second.ref_count == 0) {
    file_map_.erase(iter);
  }
}

DataStorage::DataStorage() {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

DataStorage::~DataStorage() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destroy mutex fail.";
}

DataStorage::DataBufferPtr DataStorage::GetDataBuffer(uint64_t ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  DataBufferPtr buffer;
  auto it = data_map_.find(ino);
  if (it == data_map_.end()) {
    buffer = std::make_shared<DataBuffer>();
    data_map_[ino] = buffer;
  } else {
    buffer = it->second;
  }

  CHECK(buffer != nullptr) << "data buffer is nullptr.";

  return buffer;
}

Status DataStorage::Read(uint64_t ino, off_t off, size_t size, char* buf,
                         size_t& rsize) {
  DataBufferPtr buffer = GetDataBuffer(ino);

  if (off >= buffer->data.size()) {
    return Status(pb::error::EOUT_OF_RANGE, "offset is out of range");
  }

  rsize = std::min(size, buffer->data.size() - off);
  memcpy(buf, buffer->data.data() + off, rsize);

  return Status::OK();
}

Status DataStorage::Write(uint64_t ino, off_t off, const char* buf,
                          size_t size) {
  DataBufferPtr buffer = GetDataBuffer(ino);
  std::string& data = buffer->data;

  if (off + size > data.size()) {
    data.resize(off + size);
  }

  memcpy(data.data() + off, buf, size);

  return Status::OK();
}
bool DataStorage::GetLength(uint64_t ino, size_t& length) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = data_map_.find(ino);
  if (it == data_map_.end()) {
    return false;
  }

  length = it->second->data.size();

  return true;
}

DummyFileSystem::DummyFileSystem() {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

DummyFileSystem::~DummyFileSystem() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destroy mutex fail.";
}

static pb::mdsv2::FsInfo GenFsInfo() {
  pb::mdsv2::FsInfo fs_info;
  fs_info.set_fs_id(kFsID);
  fs_info.set_fs_name("dummy_fs");
  fs_info.set_block_size(4096);
  fs_info.set_fs_type(pb::mdsv2::FsType::S3);
  fs_info.set_owner("dengzihui");
  fs_info.set_capacity(1024 * 1024 * 1024);
  fs_info.set_recycle_time_hour(24);

  return fs_info;
}

static uint64_t ToTimestamp(const struct timespec& ts) {
  return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

// root inode mode: S_IFDIR | 01777
static DummyFileSystem::PBInode GenInode(uint32_t fs_id, uint64_t ino,
                                         pb::mdsv2::FileType type) {
  DummyFileSystem::PBInode inode;
  inode.set_ino(ino);
  inode.set_fs_id(fs_id);
  inode.set_length(0);
  inode.set_mode(S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP |
                 S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  inode.set_uid(1008);
  inode.set_gid(1008);
  inode.set_rdev(0);
  inode.set_type(type);

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);

  inode.set_atime(ToTimestamp(now));
  inode.set_mtime(ToTimestamp(now));
  inode.set_ctime(ToTimestamp(now));

  if (type == pb::mdsv2::FileType::DIRECTORY) {
    inode.set_nlink(2);
  } else {
    inode.set_nlink(1);
  }

  return inode;
}

static DummyFileSystem::PBDentry GenDentry(uint32_t fs_id, uint64_t parent_ino,
                                           uint64_t ino,
                                           const std::string& name,
                                           pb::mdsv2::FileType type) {
  DummyFileSystem::PBDentry dentry;
  dentry.set_ino(ino);
  dentry.set_name(name);
  dentry.set_parent_ino(parent_ino);
  dentry.set_fs_id(fs_id);
  dentry.set_type(type);

  return dentry;
}

bool DummyFileSystem::Init() {
  // create fs
  fs_info_ = GenFsInfo();

  // create root inode
  auto inode =
      GenInode(fs_info_.fs_id(), kRootIno, pb::mdsv2::FileType::DIRECTORY);

  // create root dentry
  auto pb_dentry = GenDentry(fs_info_.fs_id(), 0, inode.ino(), "/",
                             pb::mdsv2::FileType::DIRECTORY);

  Dentry dentry;
  dentry.dentry = pb_dentry;

  LOG(INFO) << fmt::format("root inode: {}", inode.ShortDebugString());

  AddInode(inode);
  AddDentry(dentry);

  return true;
}

void DummyFileSystem::UnInit() {}

Status DummyFileSystem::Lookup(uint64_t parent_ino, const std::string& name,
                               EntryOut& entry_out) {
  PBDentry dentry;
  if (!GetChildDentry(parent_ino, name, dentry)) {
    return Status(pb::error::ENOT_FOUND, "not found dentry");
  }

  PBInode inode;
  if (!GetInode(dentry.ino(), inode)) {
    return Status(pb::error::ENOT_FOUND, "not found inode");
  }

  entry_out.inode = inode;

  return Status::OK();
}

Status DummyFileSystem::MkNod(uint64_t parent_ino, const std::string& name,
                              uint32_t uid, uint32_t gid, mode_t mode,
                              dev_t rdev, EntryOut& entry_out) {
  uint32_t fs_id = fs_info_.fs_id();

  Dentry dentry;
  if (!GetDentry(parent_ino, dentry)) {
    return Status(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  uint64_t ino = GenIno();
  auto inode = GenInode(fs_id, ino, pb::mdsv2::FileType::FILE);
  inode.set_mode(S_IFREG | mode);
  inode.set_uid(uid);
  inode.set_gid(gid);
  inode.set_rdev(rdev);

  auto pb_dentry =
      GenDentry(fs_id, parent_ino, ino, name, pb::mdsv2::FileType::FILE);

  AddChildDentry(parent_ino, pb_dentry);
  AddInode(inode);

  entry_out.inode = inode;

  return Status::OK();
}

Status DummyFileSystem::Open(uint64_t ino) {
  if (open_file_memo_.IsOpened(ino)) {
    open_file_memo_.Open(ino);
    return Status::OK();
  }

  open_file_memo_.Open(ino);

  return Status::OK();
}

Status DummyFileSystem::Release(uint64_t ino) {
  if (!open_file_memo_.IsOpened(ino)) {
    return Status::OK();
  }

  open_file_memo_.Close(ino);

  return Status::OK();
}

Status DummyFileSystem::Read(uint64_t ino, off_t off, size_t size, char* buf,
                             size_t& rsize) {
  auto status = data_storage_.Read(ino, off, size, buf, rsize);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("read fail, error: {} {}.", status.error_code(),
                              status.error_str());
  }
  return status;
}

Status DummyFileSystem::Write(uint64_t ino, off_t off, const char* buf,
                              size_t size, size_t& wsize) {
  auto status = data_storage_.Write(ino, off, buf, size);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("write fail, error: {} {}.", status.error_code(),
                              status.error_str());
    return status;
  }

  wsize = size;

  size_t length = 0;
  if (data_storage_.GetLength(ino, length)) {
    UpdateInodeLength(ino, length);
  }

  return Status::OK();
}

Status DummyFileSystem::Flush(uint64_t ino) {
  LOG(INFO) << fmt::format("Flush ino({}).", ino);
  return Status::OK();
}

Status DummyFileSystem::Fsync(uint64_t ino, int data_sync) {
  LOG(INFO) << fmt::format("Fsync ino({}) data_sync({}).", ino, data_sync);
  return Status::OK();
}

Status DummyFileSystem::MkDir(uint64_t parent_ino, const std::string& name,
                              uint32_t uid, uint32_t gid, mode_t mode,
                              dev_t rdev, EntryOut& entry_out) {
  uint32_t fs_id = fs_info_.fs_id();

  Dentry parent_dentry;
  if (!GetDentry(parent_ino, parent_dentry)) {
    return Status(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  uint64_t ino = GenIno();
  auto inode = GenInode(fs_id, ino, pb::mdsv2::FileType::DIRECTORY);
  inode.set_mode(S_IFDIR | mode);
  inode.set_uid(uid);
  inode.set_gid(gid);
  inode.set_rdev(rdev);

  auto pb_dentry =
      GenDentry(fs_id, parent_ino, ino, name, pb::mdsv2::FileType::DIRECTORY);

  Dentry dentry;
  dentry.dentry = pb_dentry;

  AddChildDentry(parent_ino, pb_dentry);
  AddDentry(dentry);
  AddInode(inode);

  entry_out.inode = inode;

  return Status::OK();
}

Status DummyFileSystem::RmDir(uint64_t parent_ino, const std::string& name) {
  Dentry parent_dentry;
  if (!GetDentry(parent_ino, parent_dentry)) {
    return Status(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  PBDentry pb_dentry;
  if (!GetChildDentry(parent_ino, name, pb_dentry)) {
    return Status(pb::error::ENOT_FOUND, "not found dentry at parent");
  }

  Dentry dentry;
  if (!GetDentry(pb_dentry.ino(), dentry)) {
    return Status(pb::error::ENOT_FOUND, "not found dentry");
  }

  if (!IsEmptyDentry(dentry)) {
    return Status(pb::error::ENOT_EMPTY, "not empty dentry");
  }

  DeleteDentry(name);
  DeleteChildDentry(parent_ino, name);

  DeleteInode(pb_dentry.ino());

  return Status::OK();
}

Status DummyFileSystem::OpenDir(uint64_t ino, uint64_t& fh) {
  fh = read_dir_state_memo_.NewState();

  IncInodeNlink(ino);
  return Status::OK();
}

Status DummyFileSystem::ReadDir(uint64_t fh, uint64_t ino,
                                ReadDirHandler handler) {
  ReadDirStateMemo::State state;
  if (!read_dir_state_memo_.GetState(fh, state)) {
    return Status(pb::error::ENOT_FOUND, "not found fh");
  }
  if (state.is_end) {
    return Status::OK();
  }

  Dentry dentry;
  if (!GetDentry(ino, dentry)) {
    return Status(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  auto it = state.last_name.empty() ? dentry.children.begin()
                                    : dentry.children.find(state.last_name);
  for (; it != dentry.children.end(); ++it) {
    auto& pb_dentry = it->second;
    if (!handler(pb_dentry.name(), pb_dentry.ino())) {
      state.last_name = pb_dentry.name();
      read_dir_state_memo_.UpdateState(fh, state);
      return Status::OK();
    }
  }

  state.is_end = true;
  read_dir_state_memo_.UpdateState(fh, state);

  return Status::OK();
}

Status DummyFileSystem::ReadDirPlus(uint64_t fh, uint64_t ino,
                                    ReadDirPlusHandler handler) {
  ReadDirStateMemo::State state;
  if (!read_dir_state_memo_.GetState(fh, state)) {
    return Status(pb::error::ENOT_FOUND, "not found fh");
  }
  if (state.is_end) {
    return Status::OK();
  }

  Dentry dentry;
  if (!GetDentry(ino, dentry)) {
    return Status(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  auto it = state.last_name.empty() ? dentry.children.begin()
                                    : dentry.children.find(state.last_name);
  for (; it != dentry.children.end(); ++it) {
    auto& pb_dentry = it->second;

    PBInode inode;
    if (!GetInode(pb_dentry.ino(), inode)) {
      continue;
    }

    if (!handler(pb_dentry.name(), inode)) {
      state.last_name = pb_dentry.name();
      read_dir_state_memo_.UpdateState(fh, state);
      return Status::OK();
    }
  }

  state.is_end = true;
  read_dir_state_memo_.UpdateState(fh, state);

  return Status::OK();
}

Status DummyFileSystem::ReleaseDir(uint64_t ino, uint64_t fh) {
  read_dir_state_memo_.DeleteState(fh);

  DecOrDeleteInodeNlink(ino);

  return Status::OK();
}

Status DummyFileSystem::Link(uint64_t ino, uint64_t new_parent_ino,
                             const std::string& new_name, EntryOut& entry_out) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status(pb::error::ENOT_FOUND, "not found inode");
  }

  if (inode.type() != pb::mdsv2::FileType::FILE) {
    return Status(pb::error::ENOT_FILE, "not file type");
  }

  Dentry dentry;
  if (!GetDentry(new_parent_ino, dentry)) {
    return Status(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  auto pb_dentry = GenDentry(fs_info_.fs_id(), new_parent_ino, ino, new_name,
                             pb::mdsv2::FileType::FILE);

  AddChildDentry(new_parent_ino, pb_dentry);
  IncInodeNlink(inode.ino());

  entry_out.inode = inode;

  return Status::OK();
}

Status DummyFileSystem::UnLink(uint64_t parent_ino, const std::string& name) {
  Dentry dentry;
  if (!GetDentry(parent_ino, dentry)) {
    return Status(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  PBDentry child_dentry;
  if (!GetChildDentry(parent_ino, name, child_dentry)) {
    return Status(pb::error::ENOT_FOUND, "not found child dentry");
  }

  DeleteChildDentry(parent_ino, name);
  DecOrDeleteInodeNlink(child_dentry.ino());

  return Status::OK();
}

Status DummyFileSystem::Symlink(uint64_t parent_ino, const std::string& name,
                                uint32_t uid, uint32_t gid,
                                const std::string& symlink,
                                EntryOut& entry_out) {
  Dentry dentry;
  if (!GetDentry(parent_ino, dentry)) {
    return Status(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  auto pb_dentry = GenDentry(fs_info_.fs_id(), parent_ino, GenIno(), name,
                             pb::mdsv2::FileType::SYM_LINK);
  auto inode = GenInode(fs_info_.fs_id(), pb_dentry.ino(),
                        pb::mdsv2::FileType::SYM_LINK);
  inode.set_mode(S_IFLNK | 0777);
  inode.set_uid(uid);
  inode.set_gid(gid);
  inode.set_symlink(symlink);

  AddInode(inode);
  AddChildDentry(parent_ino, pb_dentry);

  entry_out.inode = inode;

  return Status::OK();
}

Status DummyFileSystem::ReadLink(uint64_t ino, std::string& symlink) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status(pb::error::ENOT_FOUND, "not found inode");
  }

  if (inode.type() != pb::mdsv2::FileType::SYM_LINK) {
    return Status(pb::error::ENOT_SYMLINK, "not symlink type");
  }

  symlink = inode.symlink();

  return Status::OK();
}

Status DummyFileSystem::GetAttr(uint64_t ino, AttrOut& attr_out) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status(pb::error::ENOT_FOUND, "not found inode");
  }

  LOG(INFO) << fmt::format("inode: {}", inode.ShortDebugString());

  attr_out.inode = inode;

  return Status::OK();
}

Status DummyFileSystem::SetAttr(uint64_t ino, struct stat* attr, int to_set,
                                AttrOut& attr_out) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status(pb::error::ENOT_FOUND, "not found inode");
  }

  std::vector<std::string> update_fields;

  if (to_set & FUSE_SET_ATTR_MODE) {
    inode.set_mode(attr->st_mode);
    update_fields.push_back("mode");
  }
  if (to_set & FUSE_SET_ATTR_UID) {
    inode.set_uid(attr->st_uid);
    update_fields.push_back("uid");
  }
  if (to_set & FUSE_SET_ATTR_GID) {
    inode.set_gid(attr->st_gid);
    update_fields.push_back("gid");
  }

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);

  if (to_set & FUSE_SET_ATTR_ATIME) {
    inode.set_atime(ToTimestamp(attr->st_atim));
    update_fields.push_back("atime");

  } else if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
    inode.set_atime(ToTimestamp(now));
    update_fields.push_back("atime");
  }

  if (to_set & FUSE_SET_ATTR_MTIME) {
    inode.set_mtime(ToTimestamp(attr->st_mtim));
    update_fields.push_back("mtime");

  } else if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
    inode.set_mtime(ToTimestamp(now));
    update_fields.push_back("mtime");
  }

  if (to_set & FUSE_SET_ATTR_CTIME) {
    inode.set_ctime(ToTimestamp(attr->st_ctim));
    update_fields.push_back("ctime");
  } else {
    inode.set_ctime(ToTimestamp(now));
    update_fields.push_back("ctime");
  }

  if (to_set & FUSE_SET_ATTR_SIZE) {
    // todo: Truncate data
    inode.set_length(attr->st_size);
  }

  UpdateInode(inode, update_fields);

  attr_out.inode = inode;

  return Status::OK();
}

Status DummyFileSystem::GetXAttr(uint64_t ino, const std::string& name,
                                 std::string& value) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status(pb::error::ENOT_FOUND, "not found inode");
  }

  const auto& xattrs = inode.xattr();
  auto it = xattrs.find(name);
  if (it != xattrs.end()) {
    value = it->second;
  }

  return Status::OK();
}

Status DummyFileSystem::SetXAttr(uint64_t ino, const std::string& name,
                                 const std::string& value) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status(pb::error::ENOT_FOUND, "not found inode");
  }

  UpdateXAttr(ino, name, value);

  return Status::OK();
}

Status DummyFileSystem::ListXAttr(uint64_t ino, size_t size,
                                  std::string& out_names) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status(pb::error::ENOT_FOUND, "not found inode");
  }

  out_names.reserve(4096);
  for (const auto& [name, value] : inode.xattr()) {
    out_names.append(name);
    out_names.push_back('\0');
  }

  return Status::OK();
}

Status DummyFileSystem::Rename(uint64_t parent_ino, const std::string& name,
                               uint64_t new_parent_ino,
                               const std::string& new_name) {
  return Status(pb::error::ENOT_SUPPORT, "not support");
}

void DummyFileSystem::AddDentry(const Dentry& dentry) {
  BAIDU_SCOPED_LOCK(mutex_);

  name_ino_map_[dentry.dentry.name()] = dentry.dentry.ino();
  dentry_map_[dentry.dentry.ino()] = dentry;
}

void DummyFileSystem::AddChildDentry(uint64_t parent_ino,
                                     const PBDentry& pb_dentry) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = dentry_map_.find(parent_ino);
  if (it == dentry_map_.end()) {
    return;
  }

  auto& dentry = it->second;

  dentry.children[pb_dentry.name()] = pb_dentry;
}

void DummyFileSystem::DeleteDentry(uint64_t parent_ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = dentry_map_.find(parent_ino);
  if (it == dentry_map_.end()) {
    return;
  }

  dentry_map_.erase(it);
}

void DummyFileSystem::DeleteDentry(const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = name_ino_map_.find(name);
  if (it == name_ino_map_.end()) {
    return;
  }

  uint64_t ino = it->second;
  dentry_map_.erase(ino);
  name_ino_map_.erase(it);
}

void DummyFileSystem::DeleteChildDentry(uint64_t parent_ino,
                                        const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = dentry_map_.find(parent_ino);
  if (it == dentry_map_.end()) {
    return;
  }

  auto& dentry = it->second;
  dentry.children.erase(name);
}

bool DummyFileSystem::GetDentry(uint64_t parent_ino, Dentry& dentry) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = dentry_map_.find(parent_ino);
  if (it == dentry_map_.end()) {
    return false;
  }

  dentry = it->second;

  return true;
}

bool DummyFileSystem::GetChildDentry(uint64_t parent_ino,
                                     const std::string& name,
                                     PBDentry& dentry) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = dentry_map_.find(parent_ino);
  if (it == dentry_map_.end()) {
    return false;
  }

  auto child_it = it->second.children.find(name);
  if (child_it == it->second.children.end()) {
    return false;
  }

  dentry = child_it->second;

  return true;
}

bool DummyFileSystem::IsEmptyDentry(const Dentry& dentry) {
  BAIDU_SCOPED_LOCK(mutex_);

  return dentry.children.empty();
}

void DummyFileSystem::AddInode(const PBInode& inode) {
  BAIDU_SCOPED_LOCK(mutex_);

  inode_map_[inode.ino()] = inode;
}

void DummyFileSystem::DeleteInode(uint64_t ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  inode_map_.erase(ino);
}

bool DummyFileSystem::GetInode(uint64_t ino, PBInode& inode) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = inode_map_.find(ino);
  if (it == inode_map_.end()) {
    return false;
  }

  inode = it->second;

  return true;
}

void DummyFileSystem::UpdateInode(const PBInode& inode,
                                  const std::vector<std::string>& fields) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = inode_map_.find(inode.ino());
  if (it == inode_map_.end()) {
    return;
  }

  auto& mut_inode = it->second;

  for (const auto& field : fields) {
    if (field == "mode") {
      mut_inode.set_mode(inode.mode());
    } else if (field == "uid") {
      mut_inode.set_uid(inode.uid());
    } else if (field == "gid") {
      mut_inode.set_gid(inode.gid());
    } else if (field == "atime") {
      mut_inode.set_atime(inode.atime());
    } else if (field == "mtime") {
      mut_inode.set_mtime(inode.mtime());
    } else if (field == "ctime") {
      mut_inode.set_ctime(inode.ctime());
    } else if (field == "length") {
      mut_inode.set_length(inode.length());
    } else {
      LOG(ERROR) << "not support update field: " << field;
    }
  }
}

void DummyFileSystem::IncInodeNlink(uint64_t ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = inode_map_.find(ino);
  if (it == inode_map_.end()) {
    return;
  }

  auto& inode = it->second;
  inode.set_nlink(inode.nlink() + 1);
}

void DummyFileSystem::DecOrDeleteInodeNlink(uint64_t ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = inode_map_.find(ino);
  if (it == inode_map_.end()) {
    return;
  }

  auto& inode = it->second;

  if (inode.nlink() == 1) {
    inode_map_.erase(it);
  } else {
    inode.set_nlink(inode.nlink() - 1);
  }
}

void DummyFileSystem::UpdateXAttr(uint64_t ino, const std::string& name,
                                  const std::string& value) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = inode_map_.find(ino);
  if (it == inode_map_.end()) {
    return;
  }

  auto& inode = it->second;
  auto* mut_xattr = inode.mutable_xattr();
  auto xattr_it = mut_xattr->find(name);
  if (xattr_it != mut_xattr->end()) {
    xattr_it->second = value;
  } else {
    inode.mutable_xattr()->insert({name, value});
  }
}

void DummyFileSystem::UpdateInodeLength(uint64_t ino, size_t length) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = inode_map_.find(ino);
  if (it == inode_map_.end()) {
    return;
  }

  auto& inode = it->second;
  if (length != inode.length()) {
    inode.set_length(length);
  }
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs
