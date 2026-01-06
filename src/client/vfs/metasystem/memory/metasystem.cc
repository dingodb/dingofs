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

#include "client/vfs/metasystem/memory/metasystem.h"

#include <fcntl.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "bthread/mutex.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/metasystem/mds/helper.h"
#include "client/vfs/vfs_meta.h"
#include "common/const.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "json/value.h"
#include "utils/uuid.h"

static const uint32_t kFsID = 10000;
static const uint64_t kRootIno = 1;
static const uint64_t kDefaultBlockSize = 4 * 1024 * 1024;
static const uint64_t kDefaultChunkSize = 64 * 1024 * 1024;
static const std::string kDefaultFsName = "dummy_fs";

namespace dingofs {
namespace client {
namespace vfs {
namespace memory {

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
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = chunk_map_.find(ino);
  if (it == chunk_map_.end()) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found chunk");
  }

  *slices = it->second.slices[index];

  return Status::OK();
}

Status FileChunkMap::Write(uint64_t ino, uint64_t index,
                           const std::vector<Slice>& slices) {
  BAIDU_SCOPED_LOCK(mutex_);
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

MemoryMetaSystem::MemoryMetaSystem() {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

MemoryMetaSystem::~MemoryMetaSystem() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destroy mutex fail.";
}

static pb::mds::FsInfo GenFsInfo() {
  pb::mds::FsInfo fs_info;

  fs_info.set_fs_id(kFsID);
  fs_info.set_fs_name(kDefaultFsName);
  fs_info.set_block_size(kDefaultBlockSize);
  fs_info.set_chunk_size(kDefaultChunkSize);
  fs_info.set_fs_type(pb::mds::FsType::S3);
  fs_info.set_owner("dengzihui");
  fs_info.set_capacity(INT64_MAX);
  fs_info.set_recycle_time_hour(24);
  fs_info.set_uuid(utils::GenerateUUID());

  fs_info.set_status(pb::mds::FsStatus::NORMAL);

  auto* s3_info = fs_info.mutable_extra()->mutable_s3_info();

  std::string ak = "UAJ1WIVF3NM5XRIL0OU2";
  s3_info->set_ak(ak);

  std::string sk = "X9MxdCZslPmXADljX140iiN6r81aGgCnO61wEA3L";
  s3_info->set_sk(sk);

  std::string endpoint = "http://10.220.68.19:80";
  s3_info->set_endpoint(endpoint);

  std::string bucketname = "dummy-fs";
  s3_info->set_bucketname(bucketname);

  LOG(INFO) << fmt::format("gen fs info: {}", fs_info.ShortDebugString());
  return fs_info;
}

// root inode mode: S_IFDIR | 01777
static MemoryMetaSystem::PBInode GenInode(uint32_t fs_id, uint64_t ino,
                                          pb::mds::FileType type) {
  MemoryMetaSystem::PBInode inode;
  inode.set_ino(ino);
  inode.set_fs_id(fs_id);
  inode.set_mode(S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP |
                 S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  inode.set_uid(0);
  inode.set_gid(0);
  inode.set_rdev(0);
  inode.set_type(type);

  uint64_t now_timestamp = CurrentTimestamp();

  inode.set_atime(now_timestamp);
  inode.set_mtime(now_timestamp);
  inode.set_ctime(now_timestamp);

  if (type == pb::mds::FileType::DIRECTORY) {
    inode.set_length(4096);
    inode.set_nlink(2);
  } else {
    inode.set_length(0);
    inode.set_nlink(1);
  }

  return inode;
}

static MemoryMetaSystem::PBDentry GenDentry(uint32_t fs_id, uint64_t parent_ino,
                                            uint64_t ino,
                                            const std::string& name,
                                            pb::mds::FileType type) {
  MemoryMetaSystem::PBDentry dentry;
  dentry.set_ino(ino);
  dentry.set_name(name);
  dentry.set_parent(parent_ino);
  dentry.set_fs_id(fs_id);
  dentry.set_type(type);

  return dentry;
}

Status MemoryMetaSystem::Init(bool upgrade) {
  // create fs
  fs_info_ = GenFsInfo();

  // create root inode
  auto inode =
      GenInode(fs_info_.fs_id(), kRootIno, pb::mds::FileType::DIRECTORY);

  // create root dentry
  auto pb_dentry = GenDentry(fs_info_.fs_id(), 0, inode.ino(), "/",
                             pb::mds::FileType::DIRECTORY);

  Dentry dentry;
  dentry.dentry = pb_dentry;

  LOG(INFO) << fmt::format("root ino: {}", inode.ShortDebugString());

  AddInode(inode);
  AddDentry(dentry);

  return Status::OK();
}

void MemoryMetaSystem::UnInit(bool upgrade) {}

bool MemoryMetaSystem::Dump(ContextSPtr ctx, Json::Value& value) {
  // Implement your dump logic here
  return true;
}

bool MemoryMetaSystem::Dump(const DumpOption& options, Json::Value& value) {
  // Implement your dump logic here
  return true;
}

bool MemoryMetaSystem::Load(ContextSPtr ctx, const Json::Value& value) {
  // Implement your load logic here
  return true;
}

static FileType ToFileType(pb::mds::FileType type) {
  switch (type) {
    case pb::mds::FileType::FILE:
      return FileType::kFile;

    case pb::mds::FileType::DIRECTORY:
      return FileType::kDirectory;

    case pb::mds::FileType::SYM_LINK:
      return FileType::kSymlink;

    default:
      CHECK(false) << "unknown file type: " << type;
  }
}

static Attr ToAttr(const pb::mds::Inode& inode) {
  Attr attr;
  attr.ino = inode.ino();
  attr.length = inode.length();
  attr.atime = inode.atime();
  attr.mtime = inode.mtime();
  attr.ctime = inode.ctime();
  attr.uid = inode.uid();
  attr.gid = inode.gid();
  attr.mode = inode.mode();
  attr.nlink = inode.nlink();
  attr.type = ToFileType(inode.type());
  attr.rdev = inode.rdev();

  return attr;
}

Status MemoryMetaSystem::Lookup(ContextSPtr ctx, Ino parent,
                                const std::string& name, Attr* attr) {
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

Status MemoryMetaSystem::MkNod(ContextSPtr ctx, Ino parent,
                               const std::string& name, uint32_t uid,
                               uint32_t gid, uint32_t mode, uint64_t rdev,
                               Attr* attr) {
  uint32_t fs_id = fs_info_.fs_id();

  Dentry dentry;
  if (!GetDentry(parent, dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  uint64_t ino = GenIno();
  auto inode = GenInode(fs_id, ino, pb::mds::FileType::FILE);
  inode.set_mode(S_IFREG | mode);
  inode.set_uid(uid);
  inode.set_gid(gid);
  inode.set_rdev(rdev);

  auto pb_dentry = GenDentry(fs_id, parent, ino, name, pb::mds::FileType::FILE);

  AddChildDentry(parent, pb_dentry);
  AddInode(inode);

  *attr = ToAttr(inode);

  return Status::OK();
}

Status MemoryMetaSystem::Open(ContextSPtr ctx, Ino ino, int flags,
                              uint64_t fh) {
  if (open_file_memo_.IsOpened(ino)) {
    open_file_memo_.Open(ino);
    return Status::OK();
  }

  open_file_memo_.Open(ino);

  return Status::OK();
}

Status MemoryMetaSystem::Create(ContextSPtr ctx, Ino parent,
                                const std::string& name, uint32_t uid,
                                uint32_t gid, uint32_t mode, int flags,
                                Attr* attr, uint64_t fh) {
  DINGOFS_RETURN_NOT_OK(MkNod(ctx, parent, name, uid, gid, mode, 0, attr));
  return Open(ctx, attr->ino, flags, fh);
}

Status MemoryMetaSystem::Flush(ContextSPtr ctx, Ino ino, uint64_t fh) {
  return Status::OK();
}

Status MemoryMetaSystem::Close(ContextSPtr ctx, Ino ino, uint64_t fh) {
  if (!open_file_memo_.IsOpened(ino)) {
    return Status::OK();
  }

  open_file_memo_.Close(ino);

  return Status::OK();
}

Status MemoryMetaSystem::ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                                   uint64_t fh, std::vector<Slice>* slices,
                                   uint64_t& version) {
  return file_chunk_map_.Read(ino, index, slices);
}

Status MemoryMetaSystem::NewSliceId(ContextSPtr ctx, Ino ino, uint64_t* id) {
  return file_chunk_map_.NewSliceId(id);
}

Status MemoryMetaSystem::WriteSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                                    uint64_t fh,
                                    const std::vector<Slice>& slices) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  DINGOFS_RETURN_NOT_OK(file_chunk_map_.Write(ino, index, slices));

  std::vector<std::string> update_fields;

  uint64_t new_max_length = 0;
  for (const auto& slice : slices) {
    new_max_length = std::max(new_max_length, slice.End());
  }

  if (new_max_length > inode.length()) {
    inode.set_length(new_max_length);
    update_fields.push_back("length");
  }

  uint64_t now_timestamp = CurrentTimestamp();

  inode.set_mtime(now_timestamp);
  inode.set_ctime(now_timestamp);

  UpdateInode(inode, update_fields);

  return Status::OK();
}

Status MemoryMetaSystem::AsyncWriteSlice(ContextSPtr ctx, Ino ino,
                                         uint64_t index, uint64_t fh,
                                         const std::vector<Slice>& slices,
                                         DoneClosure done) {
  return Status::Internal(pb::error::ENOT_SUPPORT, "not support");
}

Status MemoryMetaSystem::Write(ContextSPtr ctx, Ino ino, uint64_t offset,
                               uint64_t size, uint64_t fh) {
  return Status::OK();
}

Status MemoryMetaSystem::MkDir(ContextSPtr ctx, Ino parent,
                               const std::string& name, uint32_t uid,
                               uint32_t gid, uint32_t mode, Attr* attr) {
  uint32_t fs_id = fs_info_.fs_id();

  Dentry parent_dentry;
  if (!GetDentry(parent, parent_dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  uint64_t ino = GenIno();
  auto inode = GenInode(fs_id, ino, pb::mds::FileType::DIRECTORY);
  inode.set_mode(S_IFDIR | mode);
  inode.set_uid(uid);
  inode.set_gid(gid);

  auto pb_dentry =
      GenDentry(fs_id, parent, ino, name, pb::mds::FileType::DIRECTORY);

  Dentry dentry;
  dentry.dentry = pb_dentry;

  AddChildDentry(parent, pb_dentry);
  AddDentry(dentry);
  AddInode(inode);

  *attr = ToAttr(inode);

  return Status::OK();
}

Status MemoryMetaSystem::RmDir(ContextSPtr ctx, Ino parent,
                               const std::string& name) {
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

Status MemoryMetaSystem::OpenDir(ContextSPtr ctx, Ino ino, uint64_t fh) {
  auto dir_iterator = std::make_shared<DirIterator>(this, ino);
  auto status = dir_iterator->Seek();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.dummy] OpenDir ino({}) fail, error: {}.",
                              ino, status.ToString());
    return status;
  }

  dir_iterator_manager_.Put(fh, dir_iterator);

  return Status::OK();
}

Status MemoryMetaSystem::ReadDir(ContextSPtr ctx, Ino, uint64_t fh,
                                 uint64_t offset, bool with_attr,
                                 ReadDirHandler handler) {
  auto dir_iterator = dir_iterator_manager_.Get(fh);
  CHECK(dir_iterator != nullptr) << "dir_iterator is null";

  while (dir_iterator->Valid()) {
    DirEntry entry = dir_iterator->GetValue(with_attr);

    if (!handler(entry, offset)) {
      break;
    }

    dir_iterator->Next();
  }

  return Status::OK();
}

Status MemoryMetaSystem::ReleaseDir(ContextSPtr ctx, Ino, uint64_t fh) {
  dir_iterator_manager_.Delete(fh);
  return Status::OK();
}

Status MemoryMetaSystem::Link(ContextSPtr ctx, Ino ino, Ino new_parent,
                              const std::string& new_name, Attr* attr) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  if (inode.type() != pb::mds::FileType::FILE) {
    return Status::Internal(pb::error::ENOT_FILE, "not file type");
  }

  Dentry dentry;
  if (!GetDentry(new_parent, dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  auto pb_dentry = GenDentry(fs_info_.fs_id(), new_parent, ino, new_name,
                             pb::mds::FileType::FILE);

  AddChildDentry(new_parent, pb_dentry);
  IncInodeNlink(inode.ino());

  *attr = ToAttr(inode);

  return Status::OK();
}

Status MemoryMetaSystem::Unlink(ContextSPtr ctx, Ino parent,
                                const std::string& name) {
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

Status MemoryMetaSystem::Symlink(ContextSPtr ctx, Ino parent,
                                 const std::string& name, uint32_t uid,
                                 uint32_t gid, const std::string& link,
                                 Attr* attr) {
  Dentry dentry;
  if (!GetDentry(parent, dentry)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found parent dentry");
  }

  auto pb_dentry = GenDentry(fs_info_.fs_id(), parent, GenIno(), name,
                             pb::mds::FileType::SYM_LINK);
  auto inode =
      GenInode(fs_info_.fs_id(), pb_dentry.ino(), pb::mds::FileType::SYM_LINK);
  inode.set_mode(S_IFLNK | 0777);
  inode.set_uid(uid);
  inode.set_gid(gid);
  inode.set_symlink(link);

  AddInode(inode);
  AddChildDentry(parent, pb_dentry);

  *attr = ToAttr(inode);

  return Status::OK();
}

Status MemoryMetaSystem::ReadLink(ContextSPtr ctx, Ino ino, std::string* link) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  if (inode.type() != pb::mds::FileType::SYM_LINK) {
    return Status::Internal(pb::error::ENOT_SYMLINK, "not symlink type");
  }

  *link = inode.symlink();

  return Status::OK();
}

Status MemoryMetaSystem::GetAttr(ContextSPtr ctx, Ino ino, Attr* attr) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  LOG(INFO) << fmt::format("ino: {}", inode.ShortDebugString());

  *attr = ToAttr(inode);

  return Status::OK();
}

Status MemoryMetaSystem::SetAttr(ContextSPtr ctx, Ino ino, int set,
                                 const Attr& attr, Attr* out_attr) {
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

  uint64_t now_timestamp = CurrentTimestamp();

  if (set & kSetAttrAtime) {
    inode.set_atime(attr.atime);
    update_fields.push_back("atime");

  } else if (set & kSetAttrAtimeNow) {
    inode.set_atime(now_timestamp);
    update_fields.push_back("atime");
  }

  if (set & kSetAttrMtime) {
    inode.set_mtime(attr.mtime);
    update_fields.push_back("mtime");

  } else if (set & kSetAttrMtimeNow) {
    inode.set_mtime(now_timestamp);
    update_fields.push_back("mtime");
  }

  if (set & kSetAttrCtime) {
    inode.set_ctime(attr.ctime);
    update_fields.push_back("ctime");
  } else {
    inode.set_ctime(now_timestamp);
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

Status MemoryMetaSystem::GetXattr(ContextSPtr ctx, Ino ino,
                                  const std::string& name, std::string* value) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::NoData(pb::error::ENOT_FOUND, "not found inode");
  }

  const auto& xattrs = inode.xattrs();
  auto it = xattrs.find(name);
  if (it != xattrs.end()) {
    *value = it->second;
  }

  return Status::OK();
}

Status MemoryMetaSystem::SetXattr(ContextSPtr ctx, Ino ino,
                                  const std::string& name,
                                  const std::string& value, int flags) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  UpdateXAttr(ino, name, value);

  return Status::OK();
}

Status MemoryMetaSystem::RemoveXattr(ContextSPtr ctx, Ino ino,
                                     const std::string& name) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  RemoveXattr(ctx, ino, name);

  return Status::OK();
}

Status MemoryMetaSystem::ListXattr(ContextSPtr ctx, Ino ino,
                                   std::vector<std::string>* xattrs) {
  PBInode inode;
  if (!GetInode(ino, inode)) {
    return Status::Internal(pb::error::ENOT_FOUND, "not found inode");
  }

  for (const auto& [name, value] : inode.xattrs()) {
    xattrs->push_back(name);
  }

  return Status::OK();
}

Status MemoryMetaSystem::Rename(ContextSPtr ctx, Ino old_parent,
                                const std::string& old_name, Ino new_parent,
                                const std::string& new_name) {
  return Status::Internal(pb::error::ENOT_SUPPORT, "not support");
}

Status MemoryMetaSystem::StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) {
  fs_stat->max_bytes = 500 * 1000 * 1000 * 1000ul;
  fs_stat->used_bytes = 20 * 1000 * 1000 * 1000ul;
  fs_stat->used_inodes = 100;
  fs_stat->max_inodes = 10000;

  return Status::OK();
}

static StoreType ToStoreType(pb::mds::FsType fs_type) {
  switch (fs_type) {
    case pb::mds::FsType::S3:
      return StoreType::kS3;

    case pb::mds::FsType::RADOS:
      return StoreType::kRados;

    case pb::mds::FsType::LOCALFILE:
      return StoreType::kLocalFile;

    default:
      CHECK(false) << "unknown fs type: " << pb::mds::FsType_Name(fs_type);
  }
}

static S3Info ToS3Info(const pb::mds::S3Info& s3_info) {
  S3Info ret;
  ret.ak = s3_info.ak();
  ret.sk = s3_info.sk();
  ret.endpoint = s3_info.endpoint();
  ret.bucket_name = s3_info.bucketname();
  return ret;
}

static RadosInfo ToRadosInfo(const pb::mds::RadosInfo& rados_info) {
  RadosInfo result;
  result.user_name = rados_info.user_name();
  result.key = rados_info.key();
  result.mon_host = rados_info.mon_host();
  result.pool_name = rados_info.pool_name();
  result.cluster_name = rados_info.cluster_name();
  return result;
}

Status MemoryMetaSystem::GetFsInfo(ContextSPtr, FsInfo* fs_info) {
  fs_info->name = kDefaultFsName;
  fs_info->id = fs_info_.fs_id();
  fs_info->chunk_size = fs_info_.chunk_size();
  fs_info->block_size = fs_info_.block_size();
  fs_info->uuid = fs_info_.uuid();
  fs_info->status = meta::Helper::ToFsStatus(fs_info_.status());

  fs_info->storage_info.store_type = ToStoreType(fs_info_.fs_type());
  if (fs_info->storage_info.store_type == StoreType::kS3) {
    CHECK(fs_info_.extra().has_s3_info())
        << "fs type is S3, but s3 info is not set";
    fs_info->storage_info.s3_info = ToS3Info(fs_info_.extra().s3_info());

  } else if (fs_info->storage_info.store_type == StoreType::kRados) {
    CHECK(fs_info_.extra().has_rados_info())
        << "fs type is Rados, but rados info is not set";

    fs_info->storage_info.rados_info =
        ToRadosInfo(fs_info_.extra().rados_info());

  } else if (fs_info->storage_info.store_type == StoreType::kLocalFile) {
    CHECK(fs_info_.extra().has_file_info())
        << "fs type is LocalFile, but file info is not set";

    fs_info->storage_info.file_info.path = fs_info_.extra().file_info().path();

  }

  else {
    LOG(ERROR) << fmt::format("unknown fs type: {}.",
                              pb::mds::FsType_Name(fs_info_.fs_type()));

    return Status::InvalidParam("unknown fs type");
  }

  return Status::OK();
}

void MemoryMetaSystem::AddDentry(const Dentry& dentry) {
  BAIDU_SCOPED_LOCK(mutex_);

  name_ino_map_[dentry.dentry.name()] = dentry.dentry.ino();
  dentry_map_[dentry.dentry.ino()] = dentry;
}

void MemoryMetaSystem::AddChildDentry(uint64_t parent_ino,
                                      const PBDentry& pb_dentry) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = dentry_map_.find(parent_ino);
  if (it == dentry_map_.end()) {
    return;
  }

  auto& dentry = it->second;

  dentry.children[pb_dentry.name()] = pb_dentry;
}

void MemoryMetaSystem::DeleteDentry(uint64_t parent_ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = dentry_map_.find(parent_ino);
  if (it == dentry_map_.end()) {
    return;
  }

  dentry_map_.erase(it);
}

void MemoryMetaSystem::DeleteDentry(const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = name_ino_map_.find(name);
  if (it == name_ino_map_.end()) {
    return;
  }

  uint64_t ino = it->second;
  dentry_map_.erase(ino);
  name_ino_map_.erase(it);
}

void MemoryMetaSystem::DeleteChildDentry(uint64_t parent_ino,
                                         const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = dentry_map_.find(parent_ino);
  if (it == dentry_map_.end()) {
    return;
  }

  auto& dentry = it->second;
  dentry.children.erase(name);
}

bool MemoryMetaSystem::GetDentry(uint64_t parent_ino, Dentry& dentry) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = dentry_map_.find(parent_ino);
  if (it == dentry_map_.end()) {
    return false;
  }

  dentry = it->second;

  return true;
}

bool MemoryMetaSystem::GetChildDentry(uint64_t parent_ino,
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

bool MemoryMetaSystem::GetAllChildDentry(uint64_t parent_ino,
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

bool MemoryMetaSystem::IsEmptyDentry(const Dentry& dentry) {
  BAIDU_SCOPED_LOCK(mutex_);

  return dentry.children.empty();
}

void MemoryMetaSystem::AddInode(const PBInode& inode) {
  BAIDU_SCOPED_LOCK(mutex_);

  inode_map_[inode.ino()] = inode;
}

void MemoryMetaSystem::DeleteInode(uint64_t ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  inode_map_.erase(ino);
}

bool MemoryMetaSystem::GetInode(uint64_t ino, PBInode& inode) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = inode_map_.find(ino);
  if (it == inode_map_.end()) {
    return false;
  }

  inode = it->second;

  return true;
}

void MemoryMetaSystem::UpdateInode(const PBInode& inode,
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

void MemoryMetaSystem::IncInodeNlink(uint64_t ino) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = inode_map_.find(ino);
  if (it == inode_map_.end()) {
    return;
  }

  auto& inode = it->second;
  inode.set_nlink(inode.nlink() + 1);
}

void MemoryMetaSystem::DecOrDeleteInodeNlink(uint64_t ino) {
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

void MemoryMetaSystem::UpdateXAttr(uint64_t ino, const std::string& name,
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

void MemoryMetaSystem::RemoveXAttr(uint64_t ino, const std::string& name) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = inode_map_.find(ino);
  if (it == inode_map_.end()) {
    return;
  }

  auto& inode = it->second;
  auto* mut_xattr = inode.mutable_xattrs();
  auto xattr_it = mut_xattr->find(name);
  if (xattr_it != mut_xattr->end()) {
    mut_xattr->erase(xattr_it);
  }
}

void MemoryMetaSystem::UpdateInodeLength(uint64_t ino, size_t length) {
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

DirIterator::~DirIterator() {
  if (dumy_system_ != nullptr) {
    dumy_system_->DecOrDeleteInodeNlink(ino_);
  }
}

Status DirIterator::Seek() { return Status::OK(); }

bool DirIterator::Valid() { return offset_ < dir_entries_.size(); }

DirEntry DirIterator::GetValue(bool with_attr) {
  CHECK(offset_ < dir_entries_.size()) << "offset out of range";

  auto entry = dir_entries_[offset_];

  if (with_attr) {
    MemoryMetaSystem::PBInode inode;
    if (dumy_system_->GetInode(entry.ino, inode)) {
      entry.attr = ToAttr(inode);
    }
  }

  return entry;
}

void DirIterator::Next() {
  if (++offset_ < dir_entries_.size()) {
    return;
  }

  dir_entries_.clear();
}

void DirIterator::SetDirEntries(std::vector<DirEntry>&& dir_entries) {
  dir_entries_ = std::move(dir_entries);
}

DirIteratorManager::DirIteratorManager() {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

DirIteratorManager::~DirIteratorManager() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destroy mutex fail.";
}

void DirIteratorManager::Put(uint64_t fh, DirIteratorSPtr dir_iterator) {
  BAIDU_SCOPED_LOCK(mutex_);

  dir_iterator_map_[fh] = dir_iterator;
}

DirIteratorSPtr DirIteratorManager::Get(uint64_t fh) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = dir_iterator_map_.find(fh);
  if (it != dir_iterator_map_.end()) {
    return it->second;
  }
  return nullptr;
}

void DirIteratorManager::Delete(uint64_t fh) {
  BAIDU_SCOPED_LOCK(mutex_);

  dir_iterator_map_.erase(fh);
}

bool MemoryMetaSystem::GetDescription(Json::Value& value) {
  // Implement your get description logic here
  return true;
}

}  // namespace memory
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
