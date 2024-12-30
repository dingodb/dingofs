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

#include "dingofs/src/mdsv2/filesystem/filesystem.h"

#include <sys/stat.h>

#include <cstdint>
#include <memory>
#include <string>

#include "bthread/mutex.h"
#include "dingofs/proto/error.pb.h"
#include "dingofs/proto/mdsv2.pb.h"
#include "dingofs/src/mdsv2/common/helper.h"
#include "dingofs/src/mdsv2/common/logging.h"
#include "dingofs/src/mdsv2/common/status.h"
#include "dingofs/src/mdsv2/filesystem/codec.h"
#include "dingofs/src/mdsv2/storage/storage.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

namespace dingofs {
namespace mdsv2 {

static const uint64_t kRootInodeId = 1;

static const std::string kFsTableName = "dingofs";

static const std::string kStatsName = ".stats";
static const std::string kRecyleName = ".recycle";

bool IsReserveNode(uint64_t inode_id) { return inode_id == kRootInodeId; }

bool IsReserveName(const std::string& name) { return name == kStatsName || name == kRecyleName; }

bool FileSystem::Init() { return true; }

Status FileSystem::GenIno(int64_t& ino) {
  bool ret = id_generator_->GenID(ino);
  return ret ? Status::OK() : Status(pb::error::EGEN_FSID, "generate inode id fail");
}

static Status ValidateMkNodRequest(const pb::mdsv2::MkNodRequest* request) {
  if (request->name().empty()) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Name is empty.");
  }

  if (request->fs_id() == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid fs_id.");
  }

  if (request->parent_inode_id() == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid parent inode id.");
  }

  if (request->type() != pb::mdsv2::FileType::FILE) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid file type.");
  }

  return Status::OK();
}

// create file, need below steps:
// 1. create inode
// 2. create dentry
// 3. update parent inode, add nlink and update mtime and ctime
Status FileSystem::MkNod(const pb::mdsv2::MkNodRequest* request) {
  CHECK(request != nullptr) << "request is nullptr.";

  uint32_t fs_id = request->fs_id();
  uint64_t parent_ino = request->parent_inode_id();

  // validate request
  auto status = ValidateMkNodRequest(request);
  if (!status.ok()) {
    return status;
  }

  // get parent inode
  auto parent_inode = inode_map_.GetInode(parent_ino);
  if (parent_inode == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("parent inode({}) not found.", parent_ino));
  }

  // generate inode id
  int64_t ino = 0;
  status = GenIno(ino);
  if (!status.ok()) {
    return status;
  }

  // build inode
  auto inode = Inode::New(fs_id, ino);
  inode->SetLength(request->length());

  uint64_t now_time = Helper::TimestampNs();
  inode->SetCtime(now_time);
  inode->SetMtime(now_time);
  inode->SetAtime(now_time);

  inode->SetUid(request->uid());
  inode->SetGid(request->gid());
  inode->SetMode(request->mode());
  inode->SetNlink(1);
  inode->SetType(request->type());
  inode->SetRdev(request->rdev());

  // build dentry
  auto dentry = Dentry::New(fs_id, request->name());
  dentry->SetIno(ino);
  dentry->SetParentIno(request->parent_inode_id());
  dentry->SetType(request->type());
  dentry->SetFlag(request->flag());
  dentry->SetInode(inode);

  // generate parent-inode/dentry/inode key/value
  KeyValue inode_kv, dentry_kv, parent_inode_kv;
  inode_kv.key = inode->GetType() == pb::mdsv2::FileType::DIRECTORY ? MetaDataCodec::EncodeDirInodeKey(fs_id, ino)
                                                                    : MetaDataCodec::EncodeFileInodeKey(fs_id, ino);
  inode_kv.value = inode->SerializeAsString();

  dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, ino, dentry->GetName());
  dentry_kv.value = dentry->SerializeAsString();

  parent_inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, ino);
  parent_inode_kv.value = parent_inode->SerializeAsString();

  // put key/value to kv storage
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, inode_kv);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, "put fs info fail");
  }

  kv_storage_->Put(option, {dentry_kv, parent_inode_kv});
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, "put fs info fail");
  }

  return Status::OK();
}

static Status ValidateMkDirRequest(const pb::mdsv2::MkDirRequest* request) {
  if (request->name().empty()) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Name is empty.");
  }

  if (request->fs_id() == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid fs_id.");
  }

  if (request->parent_inode_id() == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid parent inode id.");
  }

  if (request->type() != pb::mdsv2::FileType::DIRECTORY) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid file type.");
  }

  return Status::OK();
}

Status FileSystem::MkDir(const pb::mdsv2::MkDirRequest* request) {
  CHECK(request != nullptr) << "request is nullptr.";

  uint32_t fs_id = request->fs_id();
  uint64_t parent_ino = request->parent_inode_id();

  // validate request
  auto status = ValidateMkDirRequest(request);
  if (!status.ok()) {
    return status;
  }

  // get parent inode
  auto parent_inode = inode_map_.GetInode(parent_ino);
  if (parent_inode == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("parent inode({}) not found.", parent_ino));
  }

  // generate inode id
  int64_t ino = 0;
  status = GenIno(ino);
  if (!status.ok()) {
    return status;
  }

  // build inode
  auto inode = Inode::New(fs_id, ino);
  inode->SetLength(request->length());

  uint64_t now_time = Helper::TimestampNs();
  inode->SetCtime(now_time);
  inode->SetMtime(now_time);
  inode->SetAtime(now_time);

  inode->SetUid(request->uid());
  inode->SetGid(request->gid());
  inode->SetMode(request->mode());
  inode->SetNlink(1);
  inode->SetType(request->type());
  inode->SetRdev(request->rdev());

  // build dentry
  auto dentry = Dentry::New(fs_id, request->name());
  dentry->SetIno(ino);
  dentry->SetParentIno(request->parent_inode_id());
  dentry->SetType(request->type());
  dentry->SetFlag(request->flag());
  dentry->SetInode(inode);

  // generate parent-inode/dentry/inode key/value
  KeyValue inode_kv, dentry_kv, parent_inode_kv;
  inode_kv.key = inode->GetType() == pb::mdsv2::FileType::DIRECTORY ? MetaDataCodec::EncodeDirInodeKey(fs_id, ino)
                                                                    : MetaDataCodec::EncodeFileInodeKey(fs_id, ino);
  inode_kv.value = inode->SerializeAsString();

  dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, ino, dentry->GetName());
  dentry_kv.value = dentry->SerializeAsString();

  parent_inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, ino);
  parent_inode_kv.value = parent_inode->SerializeAsString();

  // put key/value to kv storage
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, inode_kv);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, "put fs info fail");
  }

  kv_storage_->Put(option, {dentry_kv, parent_inode_kv});
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, "put fs info fail");
  }

  return Status::OK();
}

Status FileSystem::RmDir(const pb::mdsv2::RmDirRequest* request) { return Status::OK(); }

// create hard link for file
// 1. create dentry
// 2. update inode mtime/ctime/nlink
// 3. update parent inode mtime/ctime/nlink
Status FileSystem::Link(uint64_t parent_ino, const std::string& name, uint64_t ino) {
  // get parent inode
  auto parent_inode = inode_map_.GetInode(parent_ino);
  if (parent_inode == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("parent inode({}) not found.", parent_ino));
  }

  // get inode
  auto inode = inode_map_.GetInode(ino);
  if (inode == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("inode({}) not found.", ino));
  }

  uint32_t fs_id = inode->GetFsId();

  KeyValue dentry_kv, parent_inode_kv, inode_kv;

  // build dentry
  auto dentry = Dentry::New(fs_id, name);
  dentry->SetIno(ino);
  dentry->SetParentIno(parent_ino);
  dentry->SetType(pb::mdsv2::FileType::FILE);
  // dentry->SetFlag(request->flag());
  dentry->SetInode(inode);

  dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, ino, name);
  dentry_kv.value = dentry->SerializeAsString();

  uint64_t now_ns = Helper::TimestampNs();

  // update inode mtime/ctime/nlink
  Inode inode_copy(*inode);
  inode_copy.SetCtime(now_ns);
  inode_copy.SetMtime(now_ns);
  inode_copy.SetNlink(inode->GetNlink() + 1);

  inode_kv.key = MetaDataCodec::EncodeFileInodeKey(fs_id, ino);
  inode_kv.value = inode->SerializeAsString();

  // update parent inode mtime/ctime/nlink
  Inode parent_inode_copy(*parent_inode);
  parent_inode_copy.SetCtime(now_ns);
  parent_inode_copy.SetMtime(now_ns);
  parent_inode_copy.SetNlink(parent_inode->GetNlink() + 1);

  parent_inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, ino);
  parent_inode_kv.value = parent_inode->SerializeAsString();

  // put key/value to kv storage
  KVStorage::WriteOption option;
  auto status = kv_storage_->Put(option, inode_kv);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, "put inode fail");
  }

  status = kv_storage_->Put(option, {parent_inode_kv, dentry_kv});
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, "put dentry fail");
  }

  // update cache
  inode->SetCtime(now_ns);
  inode->SetMtime(now_ns);
  inode->SetNlink(inode_copy.GetNlink());

  parent_inode->SetCtime(now_ns);
  parent_inode->SetMtime(now_ns);
  parent_inode->SetNlink(parent_inode_copy.GetNlink());

  return Status::OK();
}

// delete hard link for file
// 1. delete dentry
// 2. update inode mtime/ctime/nlink
// 3. update parent inode mtime/ctime/nlink
Status FileSystem::UnLink(uint64_t parent_ino, const std::string& name) {
  // get parent dentry
  auto parent_dentry = dentry_map_.GetDentry(parent_ino);
  if (parent_dentry == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("parent dentry({}) not found.", parent_ino));
  }

  auto dentry = parent_dentry->GetChildDentry(name);
  if (dentry == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("child dentry({}) not found.", name));
  }

  uint64_t ino = dentry->GetIno();
  auto inode = dentry->GetInode();
  if (inode == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("inode({}) not found.", ino));
  }

  uint32_t fs_id = inode->GetFsId();

  KeyValue dentry_kv, parent_inode_kv, inode_kv;

  uint64_t now_ns = Helper::TimestampNs();

  // update inode mtime/ctime/nlink
  Inode inode_copy(*inode);
  inode_copy.SetCtime(now_ns);
  inode_copy.SetMtime(now_ns);
  inode_copy.SetNlink(inode->GetNlink() + 1);

  inode_kv.key = MetaDataCodec::EncodeFileInodeKey(fs_id, ino);
  inode_kv.value = inode->SerializeAsString();

  // update parent inode mtime/ctime/nlink
  auto parent_inode = parent_dentry->GetInode();
  Inode parent_inode_copy(*parent_inode);
  parent_inode_copy.SetCtime(now_ns);
  parent_inode_copy.SetMtime(now_ns);
  parent_inode_copy.SetNlink(parent_inode->GetNlink() + 1);

  parent_inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, ino);
  parent_inode_kv.value = parent_inode->SerializeAsString();

  // delete dentry
  dentry_kv.opt_type = KeyValue::OpType::kDelete;
  dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, ino, name);

  // put key/value to kv storage
  KVStorage::WriteOption option;
  auto status = kv_storage_->Put(option, inode_kv);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, "put inode fail");
  }

  status = kv_storage_->Put(option, {parent_inode_kv, dentry_kv});
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, "put dentry fail");
  }

  // update cache
  inode->SetCtime(now_ns);
  inode->SetMtime(now_ns);
  inode->SetNlink(inode_copy.GetNlink());

  parent_inode->SetCtime(now_ns);
  parent_inode->SetMtime(now_ns);
  parent_inode->SetNlink(parent_inode_copy.GetNlink());

  return Status::OK();
}

Status ValidateSymlink(const pb::mdsv2::SymlinkRequest* request) { return Status::OK(); }

// create symbol link
// 1. create inode
// 2. create dentry
// 3. update parent inode mtime/ctime/nlink
Status FileSystem::Symlink(const pb::mdsv2::SymlinkRequest* request) {
  CHECK(request != nullptr) << "request is nullptr.";

  uint32_t fs_id = request->fs_id();
  uint64_t parent_ino = request->parent_inode_id();

  // validate request
  auto status = ValidateSymlink(request);
  if (!status.ok()) {
    return status;
  }

  // get parent inode
  auto parent_inode = inode_map_.GetInode(parent_ino);
  if (parent_inode == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("parent inode({}) not found.", parent_ino));
  }

  // generate inode id
  int64_t ino = 0;
  status = GenIno(ino);
  if (!status.ok()) {
    return status;
  }

  // build inode
  auto inode = Inode::New(fs_id, ino);
  inode->SetLength(request->length());

  uint64_t now_time = Helper::TimestampNs();
  inode->SetCtime(now_time);
  inode->SetMtime(now_time);
  inode->SetAtime(now_time);

  inode->SetUid(request->uid());
  inode->SetGid(request->gid());
  inode->SetMode(request->mode());
  inode->SetNlink(1);
  inode->SetType(pb::mdsv2::FileType::SYM_LINK);
  inode->SetRdev(request->rdev());

  // build dentry
  auto dentry = Dentry::New(fs_id, request->name());
  dentry->SetIno(ino);
  dentry->SetParentIno(request->parent_inode_id());
  dentry->SetType(pb::mdsv2::FileType::SYM_LINK);
  dentry->SetFlag(request->flag());
  dentry->SetInode(inode);

  // generate parent-inode/dentry/inode key/value
  KeyValue inode_kv, dentry_kv, parent_inode_kv;
  inode_kv.key = inode->GetType() == pb::mdsv2::FileType::DIRECTORY ? MetaDataCodec::EncodeDirInodeKey(fs_id, ino)
                                                                    : MetaDataCodec::EncodeFileInodeKey(fs_id, ino);
  inode_kv.value = inode->SerializeAsString();

  dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, ino, dentry->GetName());
  dentry_kv.value = dentry->SerializeAsString();

  parent_inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, ino);
  parent_inode_kv.value = parent_inode->SerializeAsString();

  // put key/value to kv storage
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, inode_kv);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, "put fs info fail");
  }

  kv_storage_->Put(option, {dentry_kv, parent_inode_kv});
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, "put fs info fail");
  }

  return Status::OK();
}

Status FileSystem::ReadLink(uint64_t ino, std::string& link) {
  auto inode = inode_map_.GetInode(ino);
  if (inode == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("inode({}) not found.", ino));
  }

  if (inode->GetType() != pb::mdsv2::FileType::SYM_LINK) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "not symlink inode.");
  }

  link = inode->GetSymlink();

  return Status::OK();
}

DentryPtr FileSystem::GetDentry(uint64_t ino) { return dentry_map_.GetDentry(ino); }

DentryPtr FileSystem::GetDentry(const std::string& name) { return dentry_map_.GetDentry(name); }

std::vector<DentryPtr> FileSystem::GetDentries(uint64_t ino, const std::string& last_name, uint32_t limit,
                                               bool is_only_dir) {
  auto dentry = dentry_map_.GetDentry(ino);
  if (dentry == nullptr) {
    return {};
  }

  return dentry->GetChildDentries(last_name, limit, is_only_dir);
}

Status FileSystem::GetInode(uint64_t parent_ino, const std::string& name, InodePtr& out_inode) {
  auto inode = GetInodeFromCache(parent_ino, name);
  if (inode != nullptr) {
    out_inode = inode;
    return Status::OK();
  }

  // todo: take from kv storage

  return Status(pb::error::ENOT_FOUND, fmt::format("inode({}/{}) not found.", parent_ino, name));
}

Status FileSystem::GetInode(uint64_t ino, InodePtr& out_inode) {
  auto inode = GetInodeFromCache(ino);
  if (inode != nullptr) {
    out_inode = inode;
    return Status::OK();
  }

  // todo: take from kv storage

  return Status(pb::error::ENOT_FOUND, fmt::format("inode({}) not found.", ino));
}

InodePtr FileSystem::GetInodeFromCache(uint64_t parent_ino, const std::string& name) {
  auto dentry = dentry_map_.GetDentry(parent_ino);
  if (dentry == nullptr) {
    DINGO_LOG(INFO) << fmt::format("parent dentry({}) not found.", parent_ino);
    return nullptr;
  }

  auto child_dentry = dentry->GetChildDentry(name);
  if (child_dentry == nullptr) {
    DINGO_LOG(INFO) << fmt::format("child dentry({}) not found.", name);
    return nullptr;
  }

  auto inode = inode_map_.GetInode(child_dentry->GetIno());
  if (inode == nullptr) {
    DINGO_LOG(INFO) << fmt::format("inode({}) not found.", child_dentry->GetIno());
    return nullptr;
  }

  return inode;
}

InodePtr FileSystem::GetInodeFromCache(uint64_t ino) { return inode_map_.GetInode(ino); }

Status FileSystem::UpdateInode(const UpdateInodeParam& param, InodePtr& out_inode) { return Status::OK(); }

Status FileSystem::GetXAttr(uint64_t ino, Inode::XAttrMap& xattr) {
  auto inode = inode_map_.GetInode(ino);
  if (inode == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("inode({}) not found.", ino));
  }

  xattr = inode->GetXAttrMap();

  return Status::OK();
}

Status FileSystem::GetXAttr(uint64_t ino, const std::string& name, std::string& value) {
  auto inode = inode_map_.GetInode(ino);
  if (inode == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("inode({}) not found.", ino));
  }

  value = inode->GetXAttr(name);

  return Status::OK();
}

Status FileSystem::SetXAttr(uint64_t ino, const std::map<std::string, std::string>& xattr) { return Status::OK(); }

Status FileSystem::UpdateS3Chunk() { return Status::OK(); }

FileSystemSet::FileSystemSet(IdGeneratorPtr id_generator, KVStoragePtr kv_storage)
    : id_generator_(id_generator), kv_storage_(kv_storage) {
  bthread_mutex_init(&mutex_, nullptr);
};

FileSystemSet::~FileSystemSet() { bthread_mutex_destroy(&mutex_); }

bool FileSystemSet::Init() {
  if (IsExistFsTable()) {
    return true;
  }

  auto status = CreateFsTable();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "create fs table fail, error: " << status.error_str();
    return false;
  }

  return true;
}

Status FileSystemSet::GenFsId(int64_t& fs_id) {
  bool ret = id_generator_->GenID(fs_id);
  return ret ? Status::OK() : Status(pb::error::EGEN_FSID, "generate fs id fail");
}

std::string FileSystemSet::GenFsValue(int64_t fs_id, const CreateFsParam& param) {
  pb::mdsv2::FsInfo fs_info;
  fs_info.set_fs_id(fs_id);
  fs_info.set_fs_name(param.fs_name);
  fs_info.set_fs_type(param.fs_type);
  fs_info.set_status(::dingofs::pb::mdsv2::FsStatus::NEW);
  fs_info.set_block_size(param.block_size);
  fs_info.set_enable_sum_in_dir(param.enable_sum_in_dir);
  fs_info.set_owner(param.owner);
  fs_info.set_capacity(param.capacity);
  fs_info.set_recycle_time_hour(param.recycle_time_hour);
  fs_info.mutable_detail()->CopyFrom(param.fs_detail);

  return fs_info.SerializeAsString();
}

Status FileSystemSet::CreateFsTable() {
  int64_t table_id = 0;
  KVStorage::TableOption option;
  MetaDataCodec::GetFsTableRange(option.start_key, option.end_key);
  DINGO_LOG(INFO) << fmt::format("Create fs table, start_key({}), end_key({}).", Helper::StringToHex(option.start_key),
                                 Helper::StringToHex(option.end_key));
  return kv_storage_->CreateTable(kFsTableName, option, table_id);
}

bool FileSystemSet::IsExistFsTable() {
  std::string start_key, end_key;
  MetaDataCodec::GetFsTableRange(start_key, end_key);
  DINGO_LOG(INFO) << fmt::format("Check fs table, start_key({}), end_key({}).", Helper::StringToHex(start_key),
                                 Helper::StringToHex(end_key));
  auto status = kv_storage_->IsExistTable(start_key, end_key);
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      DINGO_LOG(ERROR) << "check fs table exist fail, error: " << status.error_str();
    }
    return false;
  }

  return true;
}

// todo: create fs/dentry/inode table
Status FileSystemSet::CreateFs(const CreateFsParam& param, int64_t& fs_id) {
  auto status = GenFsId(fs_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  // when create fs fail, clean up
  auto cleanup = [&](int64_t dentry_table_id, int64_t file_inode_table_id, const std::string& fs_key) {
    // clean dentry table
    if (dentry_table_id > 0) {
      auto status = kv_storage_->DropTable(dentry_table_id);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("Clean dentry table({}) fail, error: {}", dentry_table_id, status.error_str());
      }
    }

    // clean file inode table
    if (file_inode_table_id > 0) {
      auto status = kv_storage_->DropTable(file_inode_table_id);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("Clean file inode table({}) fail, error: {}", file_inode_table_id,
                                  status.error_str());
      }
    }

    // clean fs info
    if (!fs_key.empty()) {
      auto status = kv_storage_->Delete(fs_key);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("Clean fs info fail, error: {}", status.error_str());
      }
    }
  };

  std::string fs_key = MetaDataCodec::EncodeFSKey(param.fs_name);
  // check fs exist
  {
    std::string value;
    Status status = kv_storage_->Get(fs_key, value);
    if (!status.ok()) {
      return Status(pb::error::EINTERNAL, "Get fs info fail");
    }
  }

  // create dentry/inode table
  int64_t dentry_table_id = 0;
  {
    KVStorage::TableOption option;
    MetaDataCodec::GetDentryTableRange(fs_id, option.start_key, option.end_key);
    Status status = kv_storage_->CreateTable(param.fs_name, option, dentry_table_id);
    if (!status.ok()) {
      return Status(pb::error::EINTERNAL, "Create dentry table fail");
    }
  }

  // create file inode talbe
  int64_t file_inode_table_id = 0;
  {
    KVStorage::TableOption option;
    MetaDataCodec::GetFileInodeTableRange(fs_id, option.start_key, option.end_key);
    Status status = kv_storage_->CreateTable(param.fs_name, option, file_inode_table_id);
    if (!status.ok()) {
      cleanup(dentry_table_id, 0, "");
      return Status(pb::error::EINTERNAL, "Create file inode table fail");
    }
  }

  // create fs
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, fs_key, GenFsValue(fs_id, param));
  if (!status.ok()) {
    cleanup(dentry_table_id, file_inode_table_id, "");
    return Status(pb::error::EINTERNAL, "Put fs info fail");
  }

  // create root inode
  {
    pb::mdsv2::Inode inode;
    inode.set_fs_id(fs_id);
    inode.set_inode_id(kRootInodeId);
    inode.set_length(0);

    uint64_t now_ns = Helper::TimestampNs();
    inode.set_ctime(now_ns);
    inode.set_mtime(now_ns);
    inode.set_atime(now_ns);

    inode.set_uid(0);
    inode.set_gid(0);
    inode.set_mode(S_IFDIR | 01777);
    inode.set_nlink(1);
    inode.set_type(pb::mdsv2::FileType::DIRECTORY);
    inode.set_rdev(0);

    std::string key = MetaDataCodec::EncodeDirInodeKey(inode.fs_id(), inode.inode_id());
    std::string value = inode.SerializeAsString();
    Status status = kv_storage_->Put(option, key, value);
    if (!status.ok()) {
      cleanup(dentry_table_id, file_inode_table_id, fs_key);
      return Status(pb::error::EINTERNAL, "Put root inode info fail");
    }
  }

  return Status::OK();
}

bool IsExistMountPoint(const pb::mdsv2::FsInfo& fs_info, const pb::mdsv2::MountPoint& mount_point) {
  for (const auto& mp : fs_info.mount_points()) {
    if (mp.path() == mount_point.path() && mp.hostname() == mount_point.hostname()) {
      return true;
    }
  }

  return false;
}

Status FileSystemSet::MountFs(const std::string& fs_name, const pb::mdsv2::MountPoint& mount_point) {
  CHECK(!fs_name.empty()) << "Fs name is empty.";

  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("Not found fs({}).", fs_name));
  }

  pb::mdsv2::FsInfo fs_info;
  CHECK(fs_info.ParseFromString(value)) << "Parse fs info fail.";

  if (IsExistMountPoint(fs_info, mount_point)) {
    return Status(pb::error::EEXISTED, "MountPoint already exist.");
  }

  fs_info.add_mount_points()->CopyFrom(mount_point);
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, fs_key, fs_info.SerializeAsString());
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, "Put root inode info fail");
  }

  return Status::OK();
}

void RemoveMountPoint(pb::mdsv2::FsInfo& fs_info, const pb::mdsv2::MountPoint& mount_point) {
  for (int i = 0; i < fs_info.mount_points_size(); i++) {
    if (fs_info.mount_points(i).path() == mount_point.path() &&
        fs_info.mount_points(i).hostname() == mount_point.hostname()) {
      fs_info.mutable_mount_points()->SwapElements(i, fs_info.mount_points_size() - 1);
      fs_info.mutable_mount_points()->RemoveLast();
      return;
    }
  }
}

Status FileSystemSet::UmountFs(const std::string& fs_name, const pb::mdsv2::MountPoint& mount_point) {
  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("Not found fs({}).", fs_name));
  }

  pb::mdsv2::FsInfo fs_info;
  CHECK(fs_info.ParseFromString(value)) << "Parse fs info fail.";

  RemoveMountPoint(fs_info, mount_point);

  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, fs_key, fs_info.SerializeAsString());
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, "Put fs fail");
  }

  return Status::OK();
}

// check if fs is mounted
// rename fs name to oldname+"_deleting"
Status FileSystemSet::DeleteFs(const std::string& fs_name) {
  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("Not found fs({}).", fs_name));
  }

  pb::mdsv2::FsInfo fs_info;
  CHECK(fs_info.ParseFromString(value)) << "Parse fs info fail.";

  if (fs_info.mount_points_size() > 0) {
    return Status(pb::error::EEXISTED, "Fs exist mount point.");
  }

  status = kv_storage_->Delete(fs_key);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, "Delete fs fail");
  }

  KVStorage::WriteOption option;
  std::string delete_fs_name = fmt::format("{}_deleting", fs_name);
  status = kv_storage_->Put(option, MetaDataCodec::EncodeFSKey(delete_fs_name), fs_info.SerializeAsString());
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, "Put fs fail");
  }

  return Status::OK();
}

Status FileSystemSet::GetFsInfo(const std::string& fs_name, pb::mdsv2::FsInfo& fs_info) {
  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("Not found fs({}).", fs_name));
  }

  CHECK(fs_info.ParseFromString(value)) << "Parse fs info fail.";

  return Status::OK();
}

FileSystemPtr FileSystemSet::GetFileSystem(uint32_t fs_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = fs_map_.find(fs_id);
  return it != fs_map_.end() ? it->second : nullptr;
}

}  // namespace mdsv2
}  // namespace dingofs
