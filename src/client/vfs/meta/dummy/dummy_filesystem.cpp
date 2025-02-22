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

#include "client/vfs/meta/dummy/dummy_filesystem.h"

#include <fcntl.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "bthread/mutex.h"
#include "client/common/status.h"
#include "client/vfs/vfs_meta.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"

static const uint32_t kFsID = 10000;
static const uint64_t kRootIno = 1;

namespace dingofs {
namespace client {
namespace vfs {
namespace dummy {

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
    return Status::Internal(pb::error::EOUT_OF_RANGE, "offset is out of range");
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

FileChunkMap::FileChunkMap() {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

FileChunkMap::~FileChunkMap() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destroy mutex fail.";
}

Status FileChunkMap::NewSliceId(uint64_t* id) {
  BAIDU_SCOPED_LOCK(mutex_);

  *id = slice_id_generator_.fetch_add(1);

  return Status::OK();
}

Status FileChunkMap::Read(uint64_t ino, uint64_t index,
                          std::vector<Slice>* slices) {
  auto it = chunk_map_.find(ino);
  if (it == chunk_map_.end()) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found chunk");
  }

  *slices = it->second.slices[index];

  return Status::OK();
}

Status FileChunkMap::Write(uint64_t ino, uint64_t index,
                           const std::vector<Slice>& slices) {
  auto it = chunk_map_.find(ino);
  if (it == chunk_map_.end()) {
    Chunk chunk;
    chunk.slices[index] = slices;
    chunk_map_[ino] = chunk;
  } else {
    auto& mut_slices = it->second.slices[index];
    mut_slices.insert(mut_slices.end(), slices.begin(), slices.end());
  }

  return Status::OK();
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

Status DummyFileSystem::Init() {
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

  return Status::OK();
}

void DummyFileSystem::UnInit() {}

// message Inode {
//   uint32 fs_id = 1;
//   uint64 ino = 2;
//   uint64 length = 3;
//   uint64 ctime = 4;
//   uint64 mtime = 5;
//   uint64 atime = 6;
//   uint32 uid = 10;
//   uint32 gid = 11;
//   uint32 mode = 12;
//   uint32 nlink = 13;
//   FileType type = 14;
//   string symlink = 15;
//   uint64 rdev = 16;
//   uint32 dtime = 17;
//   uint32 openmpcount = 18;
//   repeated uint64 parent_inos = 19;
//   // chunk index -> slice list
//   map<uint64, SliceList> chunks = 20;
//   map<string, bytes> xattrs = 21;
// }

static FileType ToFileType(pb::mdsv2::FileType type) {
  switch (type) {
    case pb::mdsv2::FileType::FILE:
      return FileType::kFile;

    case pb::mdsv2::FileType::DIRECTORY:
      return FileType::kDirectory;

    case pb::mdsv2::FileType::SYM_LINK:
      return FileType::kSymlink;

    default:
      CHECK(false) << "unknown file type: " << type;
  }
}

static Attr ToAttr(const pb::mdsv2::Inode& inode) {
  Attr attr;
  attr.ino = inode.ino();
  attr.length = inode.length();
  attr.atime = inode.atime() / 1000000000;
  attr.atime_ns = inode.atime() % 1000000000;
  attr.mtime = inode.mtime() / 1000000000;
  attr.mtime_ns = inode.mtime() % 1000000000;
  attr.ctime = inode.ctime() / 1000000000;
  attr.ctime_ns = inode.ctime() % 1000000000;
  attr.uid = inode.uid();
  attr.gid = inode.gid();
  attr.mode = inode.mode();
  attr.nlink = inode.nlink();
  attr.type = ToFileType(inode.type());
  attr.rdev = inode.rdev();

  return attr;
}

Status DummyFileSystem::Lookup(Ino parent, const std::string& name,
                               Attr* attr) {
  PBDentry dentry;
  if (!GetChildDentry(parent, name, dentry)) {
    return Status::NotExist("not found dentry");
  }

  PBInode inode;
  if (!GetInode(dentry.ino(), inode)) {
    return Status::NotExist("not found dentry");
  }

  *attr = ToAttr(inode);

  return Status::OK();
}

Status DummyFileSystem::MkNod(Ino parent, const std::string& name, uint32_t uid,
                              uint32_t gid, uint32_t mode, uint64_t rdev,
                              Attr* attr) {
  uint32_t fs_id = fs_info_.fs_id();

  Dentry dentry;
  if (!GetDentry(parent, dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  uint64_t ino = GenIno();
  auto inode = GenInode(fs_id, ino, pb::mdsv2::FileType::FILE);
  inode.set_mode(S_IFREG | mode);
  inode.set_uid(uid);
  inode.set_gid(gid);
  inode.set_rdev(rdev);

  auto pb_dentry =
      GenDentry(fs_id, parent, ino, name, pb::mdsv2::FileType::FILE);

  AddChildDentry(parent, pb_dentry);
  AddInode(inode);

  *attr = ToAttr(inode);

  return Status::OK();
}

Status DummyFileSystem::Open(Ino ino, int flags) {
  if (open_file_memo_.IsOpened(ino)) {
    open_file_memo_.Open(ino);
    return Status::OK();
  }

  open_file_memo_.Open(ino);

  return Status::OK();
}

Status DummyFileSystem::Create(Ino parent, const std::string& name,
                               uint32_t uid, uint32_t gid, uint32_t mode,
                               int flags, Attr* attr) {
  DINGOFS_RETURN_NOT_OK(MkNod(parent, name, uid, gid, mode, 0, attr));
  return Open(attr->ino, flags);
}

Status DummyFileSystem::Close(Ino ino) {
  if (!open_file_memo_.IsOpened(ino)) {
    return Status::OK();
  }

  open_file_memo_.Close(ino);

  return Status::OK();
}

Status DummyFileSystem::ReadSlice(Ino ino, uint64_t index,
                                  std::vector<Slice>* slices) {
  return file_chunk_map_.Read(ino, index, slices);
}

Status DummyFileSystem::NewSliceId(uint64_t* id) {
  return file_chunk_map_.NewSliceId(id);
}

Status DummyFileSystem::WriteSlice(Ino ino, uint64_t index,
                                   const std::vector<Slice>& slices) {
  return file_chunk_map_.Write(ino, index, slices);
}

Status DummyFileSystem::MkDir(Ino parent, const std::string& name, uint32_t uid,
                              uint32_t gid, uint32_t mode, Attr* attr) {
  uint32_t fs_id = fs_info_.fs_id();

  Dentry parent_dentry;
  if (!GetDentry(parent, parent_dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  uint64_t ino = GenIno();
  auto inode = GenInode(fs_id, ino, pb::mdsv2::FileType::DIRECTORY);
  inode.set_mode(S_IFDIR | mode);
  inode.set_uid(uid);
  inode.set_gid(gid);

  auto pb_dentry =
      GenDentry(fs_id, parent, ino, name, pb::mdsv2::FileType::DIRECTORY);

  Dentry dentry;
  dentry.dentry = pb_dentry;

  AddChildDentry(parent, pb_dentry);
  AddDentry(dentry);
  AddInode(inode);

  *attr = ToAttr(inode);

  return Status::OK();
}

Status DummyFileSystem::RmDir(Ino parent, const std::string& name) {
  Dentry parent_dentry;
  if (!GetDentry(parent, parent_dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  PBDentry pb_dentry;
  if (!GetChildDentry(parent, name, pb_dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND,
                            "not found dentry at parent");
  }

  Dentry dentry;
  if (!GetDentry(pb_dentry.ino(), dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found dentry");
  }

  if (!IsEmptyDentry(dentry)) {
    return Status::Internal(pb::error::ENOT_EMPTY, "not empty dentry");
  }

  DeleteDentry(name);
  DeleteChildDentry(parent, name);

  DeleteInode(pb_dentry.ino());

  return Status::OK();
}

Status DummyFileSystem::OpenDir(Ino ino) {
  IncInodeNlink(ino);
  return Status::OK();
}

DirIterator* DummyFileSystem::NewDirIterator(Ino ino) {
  std::vector<DirEntry> entries;
  GetAllChildDentry(ino, entries);

  auto* dir_iterator = new DummyDirIterator(this, ino);
  dir_iterator->SetDirEntries(std::move(entries));

  return dir_iterator;
}

Status DummyFileSystem::Link(Ino ino, Ino new_parent,
                             const std::string& new_name, Attr* attr) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  if (inode.type() != pb::mdsv2::FileType::FILE) {
    return Status::Internal(pb::error::ENOT_FILE, "not file type");
  }

  Dentry dentry;
  if (!GetDentry(new_parent, dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  auto pb_dentry = GenDentry(fs_info_.fs_id(), new_parent, ino, new_name,
                             pb::mdsv2::FileType::FILE);

  AddChildDentry(new_parent, pb_dentry);
  IncInodeNlink(inode.ino());

  *attr = ToAttr(inode);

  return Status::OK();
}

Status DummyFileSystem::Unlink(Ino parent, const std::string& name) {
  Dentry dentry;
  if (!GetDentry(parent, dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  PBDentry child_dentry;
  if (!GetChildDentry(parent, name, child_dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found child dentry");
  }

  DeleteChildDentry(parent, name);
  DecOrDeleteInodeNlink(child_dentry.ino());

  return Status::OK();
}

Status DummyFileSystem::Symlink(Ino parent, const std::string& name,
                                uint32_t uid, uint32_t gid,
                                const std::string& link, Attr* attr) {
  Dentry dentry;
  if (!GetDentry(parent, dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  auto pb_dentry = GenDentry(fs_info_.fs_id(), parent, GenIno(), name,
                             pb::mdsv2::FileType::SYM_LINK);
  auto inode = GenInode(fs_info_.fs_id(), pb_dentry.ino(),
                        pb::mdsv2::FileType::SYM_LINK);
  inode.set_mode(S_IFLNK | 0777);
  inode.set_uid(uid);
  inode.set_gid(gid);
  inode.set_symlink(link);

  AddInode(inode);
  AddChildDentry(parent, pb_dentry);

  *attr = ToAttr(inode);

  return Status::OK();
}

Status DummyFileSystem::ReadLink(Ino ino, std::string* link) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  if (inode.type() != pb::mdsv2::FileType::SYM_LINK) {
    return Status::Internal(pb::error::ENOT_SYMLINK, "not symlink type");
  }

  *link = inode.symlink();

  return Status::OK();
}

Status DummyFileSystem::GetAttr(Ino ino, Attr* attr) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  LOG(INFO) << fmt::format("inode: {}", inode.ShortDebugString());

  *attr = ToAttr(inode);

  return Status::OK();
}

static uint64_t ToTimestamp(uint64_t tv_sec, uint32_t tv_nsec) {
  return tv_sec * 1000000000 + tv_nsec;
}

Status DummyFileSystem::SetAttr(Ino ino, int set, const Attr& attr,
                                Attr* out_attr) {
  out_attr->atime = ToTimestamp(out_attr->atime, out_attr->atime_ns);
  out_attr->atime_ns = 0;

  out_attr->mtime = ToTimestamp(out_attr->mtime, out_attr->mtime_ns);
  out_attr->mtime_ns = 0;

  out_attr->ctime = ToTimestamp(out_attr->ctime, out_attr->ctime_ns);
  out_attr->ctime_ns = 0;

  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  std::vector<std::string> update_fields;

  if (set & kSetAttrMode) {
    inode.set_mode(attr.mode);
    update_fields.push_back("mode");
  }
  if (set & kSetAttrUid) {
    inode.set_uid(attr.uid);
    update_fields.push_back("uid");
  }
  if (set & kSetAttrGid) {
    inode.set_gid(attr.gid);
    update_fields.push_back("gid");
  }

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);

  if (set & kSetAttrAtime) {
    inode.set_atime(attr.atime * 1000000000 + attr.atime_ns);
    update_fields.push_back("atime");

  } else if (set & kSetAttrAtimeNow) {
    inode.set_atime(ToTimestamp(now));
    update_fields.push_back("atime");
  }

  if (set & kSetAttrMtime) {
    inode.set_mtime(attr.mtime * 1000000000 + attr.mtime_ns);
    update_fields.push_back("mtime");

  } else if (set & kSetAttrMtimeNow) {
    inode.set_mtime(ToTimestamp(now));
    update_fields.push_back("mtime");
  }

  if (set & kSetAttrCtime) {
    inode.set_ctime(attr.ctime * 1000000000 + attr.ctime_ns);
    update_fields.push_back("ctime");
  } else {
    inode.set_ctime(ToTimestamp(now));
    update_fields.push_back("ctime");
  }

  if (set & kSetAttrSize) {
    // todo: Truncate data
    inode.set_length(attr.length);
  }

  UpdateInode(inode, update_fields);

  *out_attr = ToAttr(inode);

  return Status::OK();
}

Status DummyFileSystem::GetXattr(Ino ino, const std::string& name,
                                 std::string* value) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  const auto& xattrs = inode.xattrs();
  auto it = xattrs.find(name);
  if (it != xattrs.end()) {
    *value = it->second;
  }

  return Status::OK();
}

Status DummyFileSystem::SetXattr(Ino ino, const std::string& name,
                                 const std::string& value, int flags) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  UpdateXAttr(ino, name, value);

  return Status::OK();
}

Status DummyFileSystem::ListXattr(Ino ino, std::vector<std::string>* xattrs) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  for (const auto& [name, value] : inode.xattrs()) {
    xattrs->push_back(name);
  }

  return Status::OK();
}

Status DummyFileSystem::Rename(Ino old_parent, const std::string& old_name,
                               Ino new_parent, const std::string& new_name) {
  return Status::Internal(pb::error::ENOT_SUPPORT, "not support");
}

Status DummyFileSystem::StatFs(Ino ino, FsStat* fs_stat) {
  fs_stat->max_bytes = 500 * 1000 * 1000 * 1000ul;
  fs_stat->used_bytes = 20 * 1000 * 1000 * 1000ul;
  fs_stat->used_inodes = 100;
  fs_stat->max_inodes = 10000;

  return Status::OK();
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

bool DummyFileSystem::GetAllChildDentry(uint64_t parent_ino,
                                        std::vector<DirEntry>& dir_entries) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = dentry_map_.find(parent_ino);
  if (it == dentry_map_.end()) {
    return false;
  }

  for (auto& [name, pb_dentry] : it->second.children) {
    DirEntry entry;
    entry.name = name;
    entry.ino = pb_dentry.ino();

    dir_entries.push_back(entry);
  }

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
  auto* mut_xattr = inode.mutable_xattrs();
  auto xattr_it = mut_xattr->find(name);
  if (xattr_it != mut_xattr->end()) {
    xattr_it->second = value;
  } else {
    inode.mutable_xattrs()->insert({name, value});
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

DummyDirIterator::~DummyDirIterator() {
  if (dumy_system_ != nullptr) {
    dumy_system_->DecOrDeleteInodeNlink(ino_);
  }
}

bool DummyDirIterator::HasNext() { return offset_ < dir_entries_.size(); }

Status DummyDirIterator::Next(bool with_attr, DirEntry* dir_entry) {
  CHECK(offset_ < dir_entries_.size()) << "offset out of range";

  auto& entry = dir_entries_[offset_];
  dir_entry->name = entry.name;
  dir_entry->ino = entry.ino;
  if (with_attr) {
    DummyFileSystem::PBInode inode;
    if (dumy_system_->GetInode(entry.ino, inode)) {
      dir_entry->attr = ToAttr(inode);
    }
  }

  ++offset_;

  return Status::OK();
}

void DummyDirIterator::SetDirEntries(std::vector<DirEntry>&& dir_entries) {
  dir_entries_ = std::move(dir_entries);
}

}  // namespace dummy
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
