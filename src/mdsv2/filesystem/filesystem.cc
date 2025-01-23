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

#include "mdsv2/filesystem/filesystem.h"

#include <sys/stat.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/codec.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

static const int64_t kInoTableId = 1001;
static const int64_t kInoBatchSize = 32;
static const int64_t kInoStartId = 100000;

static const uint64_t kRootIno = 1;
static const uint64_t kRootParentIno = 0;

static const std::string kFsTableName = "dingofs";

static const std::string kStatsName = ".stats";
static const std::string kRecyleName = ".recycle";

DEFINE_uint32(filesystem_name_max_size, 1024, "Max size of filesystem name.");

bool IsReserveNode(uint64_t ino) { return ino == kRootIno; }

bool IsReserveName(const std::string& name) { return name == kStatsName || name == kRecyleName; }

bool IsInvalidName(const std::string& name) { return name.empty() || name.size() > FLAGS_filesystem_name_max_size; }

static inline bool IsDir(uint64_t ino) { return (ino & 1) == 1; }

static inline bool IsFile(uint64_t ino) { return (ino & 1) == 0; }

// odd number is dir inode, even number is file inode
Status FileSystem::GenDirIno(int64_t& ino) {
  bool ret = id_generator_->GenID(ino);
  ino = (ino << 1) + 1;

  return ret ? Status::OK() : Status(pb::error::EGEN_FSID, "generate inode id fail");
}

// odd number is dir inode, even number is file inode
Status FileSystem::GenFileIno(int64_t& ino) {
  bool ret = id_generator_->GenID(ino);
  ino = ino << 1;

  return ret ? Status::OK() : Status(pb::error::EGEN_FSID, "generate inode id fail");
}

Status FileSystem::CreateRoot() {
  uint32_t fs_id = fs_info_.fs_id();
  CHECK(fs_id > 0) << "fs_id is invalid.";

  // when create root fail, clean up
  auto cleanup = [&](const std::string& inode_key) {
    // clean inode
    if (!inode_key.empty()) {
      auto status = kv_storage_->Delete(inode_key);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("delete dentry fail, {}", status.error_str());
      }
    }
  };

  auto inode = Inode::New(fs_id, kRootIno);
  inode->SetLength(0);

  inode->SetUid(1008);
  inode->SetGid(1008);
  inode->SetMode(S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  inode->SetNlink(2);
  inode->SetType(pb::mdsv2::FileType::DIRECTORY);
  inode->SetRdev(0);

  uint64_t now_ns = Helper::TimestampNs();
  inode->SetCtime(now_ns);
  inode->SetMtime(now_ns);
  inode->SetAtime(now_ns);

  std::string inode_key = MetaDataCodec::EncodeDirInodeKey(fs_id, inode->Ino());
  std::string inode_value = MetaDataCodec::EncodeDirInodeValue(inode->CopyTo());
  KVStorage::WriteOption option;
  auto status = kv_storage_->Put(option, inode_key, inode_value);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put root inode fail, {}", status.error_str()));
  }

  Dentry dentry(fs_id, "/", kRootParentIno, kRootIno, pb::mdsv2::FileType::DIRECTORY, 0, inode);

  std::string dentry_key = MetaDataCodec::EncodeDentryKey(fs_id, dentry.ParentIno(), dentry.Name());
  std::string dentry_value = MetaDataCodec::EncodeDentryValue(dentry.CopyTo());
  status = kv_storage_->Put(option, dentry_key, dentry_value);
  if (!status.ok()) {
    cleanup(inode_key);
    return Status(pb::error::EBACKEND_STORE, fmt::format("put root dentry fail, {}", status.error_str()));
  }

  inode_cache_.PutInode(inode->Ino(), inode);
  dentry_cache_.Put(dentry.Ino(), DentrySet::New(inode));

  DINGO_LOG(INFO) << fmt::format("create filesystem({}) root success.", fs_id);

  return Status::OK();
}

Status FileSystem::Lookup(uint64_t parent_ino, const std::string& name, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("Lookup parent_ino({}), name({}).", parent_ino, name);

  DentrySetPtr dentry_set;
  auto status = GetDentrySet(parent_ino, dentry_set);
  if (!status.ok()) {
    return status;
  }

  Dentry dentry;
  if (!dentry_set->GetChild(name, dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("dentry({}) not found.", name));
  }

  InodePtr inode = dentry.Inode();
  if (inode == nullptr) {
    auto status = GetInode(dentry.Ino(), inode);
    if (!status.ok()) {
      return status;
    }

    dentry_set->PutChild(Dentry(dentry, inode));
  }

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

// create file, need below steps:
// 1. create inode
// 2. create dentry
// 3. update parent inode, add nlink and update mtime and ctime
Status FileSystem::MkNod(const MkNodParam& param, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("MkNod parent_ino({}), name({}).", param.parent_ino, param.name);

  uint32_t fs_id = fs_info_.fs_id();
  uint64_t parent_ino = param.parent_ino;

  // when fail, clean up
  auto cleanup = [&](const std::string& inode_key) {
    // clean inode
    if (!inode_key.empty()) {
      auto status = kv_storage_->Delete(inode_key);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("Clean inode kv fail, error: {}", status.error_str());
      }
    }
  };

  // check request
  if (param.name.empty()) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "name is empty");
  }

  if (param.parent_ino == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "invalid parent inode id");
  }

  // get dentry set
  DentrySetPtr dentry_set;
  auto status = GetDentrySet(parent_ino, dentry_set);
  if (!status.ok()) {
    return status;
  }
  auto parent_inode = dentry_set->ParentInode();

  // generate inode id
  int64_t ino = 0;
  status = GenFileIno(ino);
  if (!status.ok()) {
    return status;
  }

  // build inode
  auto inode = Inode::New(fs_id, ino);
  inode->SetLength(0);

  uint64_t now_time = Helper::TimestampNs();
  inode->SetCtime(now_time);
  inode->SetMtime(now_time);
  inode->SetAtime(now_time);

  inode->SetUid(param.uid);
  inode->SetGid(param.gid);
  inode->SetMode(S_IFREG | param.mode);
  inode->SetNlink(1);
  inode->SetType(pb::mdsv2::FileType::FILE);
  inode->SetRdev(param.rdev);

  // build dentry
  Dentry dentry(fs_id, param.name, parent_ino, ino, pb::mdsv2::FileType::FILE, param.flag, inode);

  // generate parent-inode/dentry/inode key/value
  KeyValue inode_kv, dentry_kv, parent_inode_kv;
  inode_kv.key = MetaDataCodec::EncodeFileInodeKey(fs_id, ino);
  inode_kv.value = MetaDataCodec::EncodeFileInodeValue(inode->CopyTo());

  dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, parent_ino, dentry.Name());
  dentry_kv.value = MetaDataCodec::EncodeDentryValue(dentry.CopyTo());

  parent_inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, parent_ino);
  parent_inode_kv.value = MetaDataCodec::EncodeDirInodeValue(parent_inode->CopyTo());

  // put key/value to kv storage
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, inode_kv);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
  }

  status = kv_storage_->Put(option, {dentry_kv, parent_inode_kv});
  if (!status.ok()) {
    cleanup(inode_kv.key);
    return Status(pb::error::EBACKEND_STORE, fmt::format("put pinode/dentry fail, {}", status.error_str()));
  }

  inode_cache_.PutInode(ino, inode);
  dentry_set->PutChild(dentry);

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

Status FileSystem::Open(uint64_t ino) {
  auto inode = open_files_.IsOpened(ino);
  if (inode != nullptr) {
    return Status::OK();
  }

  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  open_files_.Open(ino, inode);

  return Status::OK();
}

Status FileSystem::Release(uint64_t ino) {
  open_files_.Close(ino);

  return Status::OK();
}

Status FileSystem::MkDir(const MkDirParam& param, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("MkDir parent_ino({}), name({}).", param.parent_ino, param.name);

  uint32_t fs_id = fs_info_.fs_id();
  uint64_t parent_ino = param.parent_ino;

  // when fail, clean up
  auto cleanup = [&](const std::string& inode_key) {
    // clean inode
    if (!inode_key.empty()) {
      auto status = kv_storage_->Delete(inode_key);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("clean inode kv fail, error: {}", status.error_str());
      }
    }
  };

  // check request
  if (param.name.empty()) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "name is empty.");
  }

  if (param.parent_ino == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "invalid parent inode id.");
  }

  // get parent dentry
  DentrySetPtr dentry_set;
  auto status = GetDentrySet(parent_ino, dentry_set);
  if (!status.ok()) {
    return status;
  }
  auto parent_inode = dentry_set->ParentInode();

  // generate inode id
  int64_t ino = 0;
  status = GenDirIno(ino);
  if (!status.ok()) {
    return status;
  }

  // build inode
  auto inode = Inode::New(fs_id, ino);
  inode->SetLength(4096);

  uint64_t now_time = Helper::TimestampNs();
  inode->SetCtime(now_time);
  inode->SetMtime(now_time);
  inode->SetAtime(now_time);

  inode->SetUid(param.uid);
  inode->SetGid(param.gid);
  inode->SetMode(S_IFDIR | param.mode);
  inode->SetNlink(2);
  inode->SetType(pb::mdsv2::FileType::DIRECTORY);
  inode->SetRdev(param.rdev);

  // build dentry
  Dentry dentry(fs_id, param.name, parent_ino, ino, pb::mdsv2::FileType::DIRECTORY, param.flag, inode);

  // generate parent-inode/dentry/inode key/value
  KeyValue inode_kv, dentry_kv, parent_inode_kv;
  inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, ino);
  inode_kv.value = MetaDataCodec::EncodeDirInodeValue(inode->CopyTo());

  dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, parent_ino, dentry.Name());
  dentry_kv.value = MetaDataCodec::EncodeDentryValue(dentry.CopyTo());

  parent_inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, parent_ino);
  parent_inode_kv.value = MetaDataCodec::EncodeDirInodeValue(parent_inode->CopyTo());

  // put key/value to kv storage
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, inode_kv);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
  }

  status = kv_storage_->Put(option, {dentry_kv, parent_inode_kv});
  if (!status.ok()) {
    cleanup(inode_kv.key);
    return Status(pb::error::EBACKEND_STORE, fmt::format("put pinode/dentry fail, {}", status.error_str()));
  }

  inode_cache_.PutInode(ino, inode);
  dentry_set->PutChild(dentry);
  dentry_cache_.Put(ino, DentrySet::New(inode));

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

Status FileSystem::RmDir(uint64_t parent_ino, const std::string& name) {
  DINGO_LOG(DEBUG) << fmt::format("RmDir parent_ino({}), name({}).", parent_ino, name);

  DentrySetPtr parent_dentry_set;
  auto status = GetDentrySet(parent_ino, parent_dentry_set);
  if (!status.ok()) {
    return status;
  }

  Dentry dentry;
  if (!parent_dentry_set->GetChild(name, dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("child dentry({}) not found.", name));
  }

  DentrySetPtr dentry_set;
  status = GetDentrySet(dentry.Ino(), dentry_set);
  if (!status.ok()) {
    return status;
  }

  InodePtr inode = dentry_set->ParentInode();
  CHECK(inode != nullptr) << fmt::format("inode({}) is null.", dentry.Ino());

  DINGO_LOG(INFO) << fmt::format("remove dir {}/{} nlink({}).", parent_ino, name, inode->Nlink());

  // check whether dir is empty
  if (dentry_set->HasChild() || inode->Nlink() > 2) {
    return Status(pb::error::ENOT_EMPTY, fmt::format("dir({}/{}) is not empty.", parent_ino, name));
  }

  // delete store inode/dentry and update parent nlink
  uint32_t fs_id = inode->FsId();
  uint64_t now_ns = Helper::TimestampNs();

  auto parent_inode = parent_dentry_set->ParentInode();
  Inode parent_inode_copy(*parent_inode);
  parent_inode_copy.SetNlinkDelta(-1, now_ns);

  KeyValue inode_kv, dentry_kv, parent_inode_kv;

  inode_kv.opt_type = KeyValue::OpType::kDelete;
  inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, dentry.Ino());

  dentry_kv.opt_type = KeyValue::OpType::kDelete;
  dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, parent_ino, name);

  parent_inode_kv.opt_type = KeyValue::OpType::kPut;
  parent_inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, parent_ino);
  parent_inode_kv.value = MetaDataCodec::EncodeDirInodeValue(parent_inode_copy.CopyTo());

  status = kv_storage_->Put(KVStorage::WriteOption(), {inode_kv, dentry_kv, parent_inode_kv});
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("delete dentry fail, {}", status.error_str()));
  }

  parent_dentry_set->DeleteChild(name);
  dentry_cache_.Delete(dentry.Ino());
  parent_inode->SetNlinkDelta(-1, now_ns);

  return Status::OK();
}

Status FileSystem::ReadDir(uint64_t ino, const std::string& last_name, uint limit, bool with_attr,
                           std::vector<EntryOut>& entry_outs) {
  DINGO_LOG(DEBUG) << fmt::format("ReadDir ino({}), last_name({}), limit({}), with_attr({}).", ino, last_name, limit,
                                  with_attr);
  DentrySetPtr dentry_set;
  auto status = GetDentrySet(ino, dentry_set);
  if (!status.ok()) {
    return status;
  }

  entry_outs.reserve(limit);
  auto dentries = dentry_set->GetChildren(last_name, limit, false);
  for (auto& dentry : dentries) {
    EntryOut entry_out;
    entry_out.name = dentry.Name();
    entry_out.inode.set_ino(dentry.Ino());

    if (with_attr) {
      // need inode attr
      InodePtr inode = dentry.Inode();
      if (inode == nullptr) {
        auto status = GetInode(dentry.Ino(), inode);
        if (!status.ok()) {
          return status;
        }

        // update dentry cache
        dentry_set->PutChild(Dentry(dentry, inode));
      }

      entry_out.inode = inode->CopyTo();
    }

    entry_outs.push_back(std::move(entry_out));
  }

  return Status::OK();
}

// create hard link for file
// 1. create dentry
// 2. update inode mtime/ctime/nlink
// 3. update parent inode mtime/ctime/nlink
Status FileSystem::Link(uint64_t ino, uint64_t new_parent_ino, const std::string& new_name, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("Link ino({}), new_parent_ino({}), new_name({}).", ino, new_parent_ino, new_name);

  DentrySetPtr dentry_set;
  auto status = GetDentrySet(new_parent_ino, dentry_set);
  if (!status.ok()) {
    return status;
  }
  auto parent_inode = dentry_set->ParentInode();

  // get inode
  InodePtr inode;
  status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  uint32_t fs_id = inode->FsId();

  // build dentry
  Dentry dentry(fs_id, new_name, new_parent_ino, ino, pb::mdsv2::FileType::FILE, 0, inode);

  KeyValue dentry_kv, parent_inode_kv, inode_kv;

  dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, new_parent_ino, new_name);
  dentry_kv.value = MetaDataCodec::EncodeDentryValue(dentry.CopyTo());

  uint64_t now_ns = Helper::TimestampNs();

  // update inode mtime/ctime/nlink
  Inode inode_copy(*inode);
  inode_copy.SetNlinkDelta(1, now_ns);

  inode_kv.key = MetaDataCodec::EncodeFileInodeKey(fs_id, ino);
  inode_kv.value = MetaDataCodec::EncodeFileInodeValue(inode_copy.CopyTo());

  // update parent inode mtime/ctime/nlink
  Inode parent_inode_copy(*parent_inode);
  parent_inode_copy.SetNlinkDelta(1, now_ns);

  parent_inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, ino);
  parent_inode_kv.value = MetaDataCodec::EncodeDirInodeValue(parent_inode_copy.CopyTo());

  // put key/value to kv storage
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, inode_kv);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
  }

  status = kv_storage_->Put(option, {parent_inode_kv, dentry_kv});
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put dentry fail, {}", status.error_str()));
  }

  // update cache
  inode->SetNlinkDelta(1, now_ns);
  parent_inode->SetNlinkDelta(1, now_ns);

  inode_cache_.PutInode(ino, inode);
  dentry_set->PutChild(dentry);

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

// delete hard link for file
// 1. delete dentry
// 2. update inode mtime/ctime/nlink
// 3. update parent inode mtime/ctime/nlink
Status FileSystem::UnLink(uint64_t parent_ino, const std::string& name) {
  DINGO_LOG(DEBUG) << fmt::format("UnLink parent_ino({}), name({}).", parent_ino, name);

  DentrySetPtr dentry_set;
  auto status = GetDentrySet(parent_ino, dentry_set);
  if (!status.ok()) {
    return status;
  }

  Dentry dentry;
  if (!dentry_set->GetChild(name, dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found dentry({}/{})", parent_ino, name));
  }

  uint64_t ino = dentry.Ino();
  InodePtr inode;
  status = GetInodeFromDentry(dentry, dentry_set, inode);
  if (!status.ok()) {
    return status;
  }

  uint32_t fs_id = inode->FsId();
  uint64_t now_ns = Helper::TimestampNs();

  KeyValue dentry_kv, parent_inode_kv, inode_kv;

  // update inode mtime/ctime/nlink
  Inode inode_copy(*inode);
  inode_copy.SetNlinkDelta(1, now_ns);

  inode_kv.opt_type = KeyValue::OpType::kPut;
  inode_kv.key = MetaDataCodec::EncodeFileInodeKey(fs_id, ino);
  inode_kv.value = MetaDataCodec::EncodeFileInodeValue(inode_copy.CopyTo());

  // update parent inode mtime/ctime/nlink
  auto parent_inode = dentry_set->ParentInode();
  Inode parent_inode_copy(*parent_inode);
  parent_inode_copy.SetNlinkDelta(1, now_ns);

  parent_inode_kv.opt_type = KeyValue::OpType::kPut;
  parent_inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, ino);
  parent_inode_kv.value = MetaDataCodec::EncodeDirInodeValue(parent_inode_copy.CopyTo());

  // delete dentry
  dentry_kv.opt_type = KeyValue::OpType::kDelete;
  dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, parent_ino, name);

  // put key/value to kv storage
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, inode_kv);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
  }

  status = kv_storage_->Put(option, {parent_inode_kv, dentry_kv});
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put pinode/dentry fail, {}", status.error_str()));
  }

  // update cache
  inode->SetNlinkDelta(1, now_ns);
  parent_inode->SetNlinkDelta(1, now_ns);

  dentry_set->DeleteChild(name);

  // if nlink is 0, delete inode
  if (inode->Nlink() == 0) {
    return DestoryInode(fs_id, ino);
  }

  return Status::OK();
}

Status FileSystem::DestoryInode(uint32_t fs_id, uint64_t ino) {
  DINGO_LOG(DEBUG) << fmt::format("destory inode {} on fs({}).", ino, fs_id);

  std::string inode_key = MetaDataCodec::EncodeFileInodeKey(fs_id, ino);
  auto status = kv_storage_->Delete(inode_key);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("delete inode fail, {}", status.error_str()));
  }

  inode_cache_.DeleteInode(ino);

  return Status::OK();
}

// create symbol link
// 1. create inode
// 2. create dentry
// 3. update parent inode mtime/ctime/nlink
Status FileSystem::Symlink(const std::string& symlink, uint64_t new_parent_ino, const std::string& new_name,
                           uint32_t uid, uint32_t gid, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("Symlink new_parent_ino({}), new_name({}) symlink({}).", new_parent_ino, new_name,
                                  symlink);

  if (new_parent_ino == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid parent_ino param.");
  }
  if (IsInvalidName(new_name)) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid name param.");
  }

  uint32_t fs_id = fs_info_.fs_id();

  DentrySetPtr dentry_set;
  auto status = GetDentrySet(new_parent_ino, dentry_set);
  if (!status.ok()) {
    return status;
  }

  // get parent inode
  auto parent_inode = dentry_set->ParentInode();

  // generate inode id
  int64_t ino = 0;
  status = GenFileIno(ino);
  if (!status.ok()) {
    return status;
  }

  // build inode
  auto inode = Inode::New(fs_id, ino);
  inode->SetSymlink(symlink);
  inode->SetLength(symlink.size());
  inode->SetUid(uid);
  inode->SetGid(gid);
  inode->SetMode(S_IFLNK | 0777);
  inode->SetNlink(1);
  inode->SetType(pb::mdsv2::FileType::SYM_LINK);
  inode->SetRdev(0);

  uint64_t now_time = Helper::TimestampNs();
  inode->SetCtime(now_time);
  inode->SetMtime(now_time);
  inode->SetAtime(now_time);

  // build dentry
  Dentry dentry(fs_id, new_name, new_parent_ino, ino, pb::mdsv2::FileType::SYM_LINK, 0, inode);

  // generate parent-inode/dentry/inode key/value
  KeyValue inode_kv, dentry_kv, parent_inode_kv;
  inode_kv.key = inode->Type() == pb::mdsv2::FileType::DIRECTORY ? MetaDataCodec::EncodeDirInodeKey(fs_id, ino)
                                                                 : MetaDataCodec::EncodeFileInodeKey(fs_id, ino);
  inode_kv.value = inode->Type() == pb::mdsv2::FileType::DIRECTORY
                       ? MetaDataCodec::EncodeDirInodeValue(inode->CopyTo())
                       : MetaDataCodec::EncodeFileInodeValue(inode->CopyTo());

  dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, new_parent_ino, dentry.Name());
  dentry_kv.value = MetaDataCodec::EncodeDentryValue(dentry.CopyTo());

  parent_inode_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, ino);
  parent_inode_kv.value = MetaDataCodec::EncodeDirInodeValue(parent_inode->CopyTo());

  // put key/value to kv storage
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, inode_kv);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
  }

  kv_storage_->Put(option, {dentry_kv, parent_inode_kv});
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put pinode/dentry fail, {}", status.error_str()));
  }

  inode_cache_.PutInode(ino, inode);
  dentry_set->PutChild(dentry);

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

Status FileSystem::ReadLink(uint64_t ino, std::string& link) {
  DINGO_LOG(DEBUG) << fmt::format("ReadLink ino({}).", ino);

  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  if (inode->Type() != pb::mdsv2::FileType::SYM_LINK) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "not symlink inode");
  }

  link = inode->Symlink();

  return Status::OK();
}

Status FileSystem::GetDentrySet(uint64_t parent_ino, DentrySetPtr& out_dentry_set) {
  auto dentry_set = GetDentrySetFromCache(parent_ino);
  if (dentry_set != nullptr) {
    out_dentry_set = dentry_set;
    return Status::OK();
  }

  DINGO_LOG(INFO) << fmt::format("dentry set cache missing {}.", parent_ino);

  auto status = GetDentrySetFromStore(parent_ino, out_dentry_set);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found dentry_set({}), {}.", parent_ino, status.error_str()));
  }

  return Status::OK();
}

DentrySetPtr FileSystem::GetDentrySetFromCache(uint64_t parent_ino) { return dentry_cache_.Get(parent_ino); }

Status FileSystem::GetDentrySetFromStore(uint64_t parent_ino, DentrySetPtr& out_dentry_set) {
  const uint32_t fs_id = fs_info_.fs_id();

  // scan dentry from store
  Range range;
  MetaDataCodec::EncodeDentryRange(fs_id, parent_ino, range.start_key, range.end_key);

  std::vector<KeyValue> kvs;
  auto status = kv_storage_->Scan(range, kvs);
  if (!status.ok()) {
    return status;
  }

  if (kvs.empty()) {
    return Status(pb::error::ENOT_FOUND, "not found kv");
  }

  auto& parent_kv = kvs.at(0);
  CHECK(parent_kv.key == range.start_key) << fmt::format(
      "Invalid parent key({}/{}).", Helper::StringToHex(parent_kv.key), Helper::StringToHex(range.start_key));

  // build dentry set
  auto inode = Inode::New(MetaDataCodec::DecodeDirInodeValue(parent_kv.value));
  auto dentry_set = DentrySet::New(inode);

  // add child dentry
  for (size_t i = 1; i < kvs.size(); ++i) {
    const auto& kv = kvs.at(i);
    auto dentry = MetaDataCodec::DecodeDentryValue(kv.value);
    dentry_set->PutChild(dentry);
  }

  out_dentry_set = dentry_set;

  return Status::OK();
}

Status FileSystem::GetInodeFromDentry(const Dentry& dentry, DentrySetPtr& dentry_set, InodePtr& out_inode) {
  InodePtr inode = dentry.Inode();
  if (inode != nullptr) {
    out_inode = inode;
    return Status::OK();
  }

  auto status = GetInode(dentry.Ino(), out_inode);
  if (!status.ok()) {
    return status;
  }

  dentry_set->PutChild(Dentry(dentry, out_inode));
  return Status::OK();
}

Status FileSystem::GetInode(uint64_t ino, InodePtr& out_inode) {
  auto inode = GetInodeFromCache(ino);
  if (inode != nullptr) {
    out_inode = inode;
    return Status::OK();
  }

  DINGO_LOG(INFO) << fmt::format("inode cache missing {}.", ino);

  auto status = GetInodeFromStore(ino, out_inode);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

InodePtr FileSystem::GetInodeFromCache(uint64_t ino) { return inode_cache_.GetInode(ino); }

Status FileSystem::GetInodeFromStore(uint64_t ino, InodePtr& out_inode) {
  std::string key = IsDir(ino) ? MetaDataCodec::EncodeDirInodeKey(fs_info_.fs_id(), ino)
                               : MetaDataCodec::EncodeFileInodeKey(fs_info_.fs_id(), ino);
  std::string value;
  auto status = kv_storage_->Get(key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found inode({}), {}", ino, status.error_str()));
  }

  out_inode =
      Inode::New(IsDir(ino) ? MetaDataCodec::DecodeDirInodeValue(value) : MetaDataCodec::DecodeFileInodeValue(value));

  inode_cache_.PutInode(ino, out_inode);

  return Status::OK();
}

Status FileSystem::GetAttr(uint64_t ino, EntryOut& entry_out) {
  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  inode->CopyTo(entry_out.inode);

  return Status::OK();
}

Status FileSystem::SetAttr(uint64_t ino, const SetAttrParam& param, EntryOut& entry_out) {
  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  // update store inode
  Inode inode_copy(*inode);
  inode_copy.SetAttr(param.inode, param.to_set);

  KeyValue inode_kv;
  inode_kv.key = MetaDataCodec::EncodeFileInodeKey(fs_info_.fs_id(), ino);
  inode_kv.value = MetaDataCodec::EncodeFileInodeValue(inode_copy.CopyTo());

  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, inode_kv);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("put fail, error: {}.", status.error_str());
    return Status(pb::error::EBACKEND_STORE, "put store inode fail");
  }

  // update cache inode
  inode->SetAttr(param.inode, param.to_set);

  inode->CopyTo(entry_out.inode);

  return Status::OK();
}

Status FileSystem::GetXAttr(uint64_t ino, Inode::XAttrMap& xattr) {
  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  xattr = inode->GetXAttrMap();

  return Status::OK();
}

Status FileSystem::GetXAttr(uint64_t ino, const std::string& name, std::string& value) {
  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  value = inode->GetXAttr(name);

  return Status::OK();
}

Status FileSystem::SetXAttr(uint64_t ino, const std::map<std::string, std::string>& xattr) {
  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  // update store inode
  Inode inode_copy(*inode);
  inode_copy.SetXAttr(xattr);

  KeyValue inode_kv;
  inode_kv.key = MetaDataCodec::EncodeFileInodeKey(fs_info_.fs_id(), ino);
  inode_kv.value = MetaDataCodec::EncodeFileInodeValue(inode_copy.CopyTo());

  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, inode_kv);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("put fail, error: {}.", status.error_str());
    return Status(pb::error::EBACKEND_STORE, "put store inode fail");
  }

  // update cache inode
  inode->SetXAttr(xattr);

  return Status::OK();
}

Status FileSystem::UpdateS3Chunk() { return Status::OK(); }  // NOLINT

Status FileSystem::Rename(uint64_t old_parent_ino, const std::string& old_name, uint64_t new_parent_ino,
                          const std::string& new_name) {
  uint32_t fs_id = fs_info_.fs_id();
  uint64_t now_ns = Helper::TimestampNs();

  // check name is valid
  if (new_name.size() > FLAGS_filesystem_name_max_size) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "new name is too long.");
  }

  // check old parent dentry/inode
  DentrySetPtr old_parent_dentry_set;
  auto status = GetDentrySet(old_parent_ino, old_parent_dentry_set);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND,
                  fmt::format("not found old parent dentry set({}), {}", old_parent_ino, status.error_str()));
  }
  InodePtr old_parent_inode = old_parent_dentry_set->ParentInode();

  // check new parent dentry/inode
  DentrySetPtr new_parent_dentry_set;
  status = GetDentrySet(old_parent_ino, new_parent_dentry_set);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND,
                  fmt::format("not found new parent dentry set({}), {}", old_parent_ino, status.error_str()));
  }
  InodePtr new_parent_inode = new_parent_dentry_set->ParentInode();

  // check old name dentry
  Dentry old_dentry;
  if (!old_parent_dentry_set->GetChild(old_name, old_dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found old dentry({}/{})", old_parent_ino, old_name));
  }

  InodePtr old_inode;
  status = GetInodeFromDentry(old_dentry, old_parent_dentry_set, old_inode);
  if (!status.ok()) {
    return status;
  }

  std::vector<KeyValue> kvs;

  InodePtr new_inode;
  bool is_exist_new_dentry = false;
  // check new name dentry
  Dentry new_dentry;
  if (new_parent_dentry_set->GetChild(new_name, new_dentry)) {
    is_exist_new_dentry = true;

    if (new_dentry.Type() != old_dentry.Type()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, fmt::format("dentry type is different, old({}), new({}).",
                                                                pb::mdsv2::FileType_Name(old_dentry.Type()),
                                                                pb::mdsv2::FileType_Name(new_dentry.Type())));
    }

    if (new_dentry.Type() == pb::mdsv2::FileType::DIRECTORY) {
      // check whether dir is empty
      DentrySetPtr new_dentry_set;
      auto status = GetDentrySet(new_dentry.Ino(), new_dentry_set);
      if (!status.ok()) {
        return Status(pb::error::ENOT_FOUND,
                      fmt::format("not found new dentry set({}), {}", new_dentry.Ino(), status.error_str()));
      }

      if (new_dentry_set->HasChild()) {
        return Status(pb::error::ENOT_EMPTY, fmt::format("new dentry({}/{}) is not empty.", new_parent_ino, new_name));
      }
    }

    // unlink new dentry inode
    status = GetInodeFromDentry(new_dentry, new_parent_dentry_set, new_inode);
    if (!status.ok()) {
      return status;
    }

    Inode new_inode_copy(*new_inode);
    new_inode_copy.SetNlinkDelta(-1, now_ns);

    if (new_inode_copy.Nlink() == 0) {
      // destory inode
      status = DestoryInode(fs_id, new_dentry.Ino());
      if (!status.ok()) {
        return status;
      }
    } else {
      // update new inode nlink
      KeyValue new_inode_kv;
      new_inode_kv.opt_type = KeyValue::OpType::kPut;
      new_inode_kv.key = new_inode->Type() == pb::mdsv2::FileType::DIRECTORY
                             ? MetaDataCodec::EncodeDirInodeKey(fs_id, new_dentry.Ino())
                             : MetaDataCodec::EncodeFileInodeKey(fs_id, new_dentry.Ino());
      new_inode_kv.value = new_inode->Type() == pb::mdsv2::FileType::DIRECTORY
                               ? MetaDataCodec::EncodeDirInodeValue(new_inode_copy.CopyTo())
                               : MetaDataCodec::EncodeFileInodeValue(new_inode_copy.CopyTo());

      kvs.push_back(new_inode_kv);
    }
  }

  // delete old dentry
  KeyValue old_dentry_kv;
  old_dentry_kv.opt_type = KeyValue::OpType::kDelete;
  old_dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, old_parent_ino, old_name);

  kvs.push_back(old_dentry_kv);

  // update old parent inode nlink
  KeyValue old_parent_kv;
  old_parent_kv.opt_type = KeyValue::OpType::kPut;
  old_parent_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, old_parent_ino);

  Inode old_parent_inode_copy(*old_parent_inode);
  old_parent_inode_copy.SetNlinkDelta(-1, now_ns);
  old_parent_kv.value = MetaDataCodec::EncodeDirInodeValue(old_parent_inode_copy.CopyTo());

  kvs.push_back(old_parent_kv);

  if (!is_exist_new_dentry) {
    // add new dentry
    KeyValue new_dentry_kv;
    new_dentry_kv.opt_type = KeyValue::OpType::kPut;
    new_dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, new_parent_ino, new_name);
    new_dentry_kv.value = MetaDataCodec::EncodeDentryValue(old_dentry.CopyTo());

    kvs.push_back(new_dentry_kv);

    // update new parent inode nlink
    KeyValue new_parent_kv;
    new_parent_kv.opt_type = KeyValue::OpType::kPut;
    new_parent_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, new_parent_ino);

    Inode new_parent_inode_copy(*new_parent_inode);
    new_parent_inode_copy.SetNlinkDelta(1, now_ns);
    new_parent_kv.value = MetaDataCodec::EncodeDirInodeValue(new_parent_inode_copy.CopyTo());

    kvs.push_back(new_parent_kv);
  }

  status = kv_storage_->Put(KVStorage::WriteOption(), kvs);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put fail, {}", status.error_str()));
  }

  // delete old dentry at cache
  old_parent_dentry_set->DeleteChild(old_name);
  // update old parent inode nlink at cache
  old_parent_inode->SetNlinkDelta(1, now_ns);

  if (is_exist_new_dentry) {
    // delete new dentry at cache
    new_parent_dentry_set->DeleteChild(new_name);
  } else {
    // update new parent inode nlink at cache
    new_parent_inode->SetNlinkDelta(1, now_ns);
  }

  // add new dentry at cache
  new_parent_dentry_set->PutChild(
      Dentry(fs_id, new_name, new_parent_ino, old_inode->Ino(), old_inode->Type(), 0, old_inode));

  // delete old dentry set at cache
  if (new_inode != nullptr && new_inode->Type() == pb::mdsv2::FileType::DIRECTORY) {
    dentry_cache_.Delete(new_inode->Ino());
  }

  return Status::OK();
}

FileSystemSet::FileSystemSet(CoordinatorClientPtr coordinator_client, IdGeneratorPtr id_generator,
                             KVStoragePtr kv_storage)
    : coordinator_client_(coordinator_client), id_generator_(std::move(id_generator)), kv_storage_(kv_storage) {}

FileSystemSet::~FileSystemSet() {}  // NOLINT

bool FileSystemSet::Init() {
  if (!IsExistFsTable()) {
    auto status = CreateFsTable();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "create fs table fail, error: " << status.error_str();
      return false;
    }
  }

  if (!LoadFileSystems()) {
    DINGO_LOG(ERROR) << "load already exist file systems fail.";
    return false;
  }

  return true;
}

Status FileSystemSet::GenFsId(int64_t& fs_id) {
  bool ret = id_generator_->GenID(fs_id);
  return ret ? Status::OK() : Status(pb::error::EGEN_FSID, "generate fs id fail");
}

pb::mdsv2::FsInfo FileSystemSet::GenFsInfo(int64_t fs_id, const CreateFsParam& param) {
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

  fs_info.set_epoch(1);
  fs_info.add_mds_ids(param.mds_id);

  return fs_info;
}

Status FileSystemSet::CreateFsTable() {
  int64_t table_id = 0;
  KVStorage::TableOption option;
  MetaDataCodec::GetFsTableRange(option.start_key, option.end_key);
  DINGO_LOG(INFO) << fmt::format("create fs table, start_key({}), end_key({}).", Helper::StringToHex(option.start_key),
                                 Helper::StringToHex(option.end_key));
  return kv_storage_->CreateTable(kFsTableName, option, table_id);
}

bool FileSystemSet::IsExistFsTable() {
  std::string start_key, end_key;
  MetaDataCodec::GetFsTableRange(start_key, end_key);
  DINGO_LOG(INFO) << fmt::format("check fs table, start_key({}), end_key({}).", Helper::StringToHex(start_key),
                                 Helper::StringToHex(end_key));
  auto status = kv_storage_->IsExistTable(start_key, end_key);
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      DINGO_LOG(ERROR) << "check fs table exist fail, error: " << status.error_str();
    }
    return false;
  }

  DINGO_LOG(INFO) << "exist fs table.";

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
        LOG(ERROR) << fmt::format("clean dentry table({}) fail, error: {}", dentry_table_id, status.error_str());
      }
    }

    // clean file inode table
    if (file_inode_table_id > 0) {
      auto status = kv_storage_->DropTable(file_inode_table_id);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("clean file inode table({}) fail, error: {}", file_inode_table_id,
                                  status.error_str());
      }
    }

    // clean fs info
    if (!fs_key.empty()) {
      auto status = kv_storage_->Delete(fs_key);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("clean fs info fail, error: {}", status.error_str());
      }
    }
  };

  std::string fs_key = MetaDataCodec::EncodeFSKey(param.fs_name);
  // check fs exist
  {
    std::string value;
    Status status = kv_storage_->Get(fs_key, value);
    if (!status.ok() && status.error_code() != pb::error::ENOT_FOUND) {
      return Status(pb::error::EINTERNAL, "get fs info fail");
    }

    if (status.ok() && !value.empty()) {
      return Status(pb::error::EEXISTED, fmt::format("fs({}) exist.", param.fs_name));
    }
  }

  // create dentry/inode table
  int64_t dentry_table_id = 0;
  {
    KVStorage::TableOption option;
    MetaDataCodec::GetDentryTableRange(fs_id, option.start_key, option.end_key);
    std::string table_name = fmt::format("{}_{}_dentry", param.fs_name, fs_id);
    Status status = kv_storage_->CreateTable(table_name, option, dentry_table_id);
    if (!status.ok()) {
      return Status(pb::error::EINTERNAL, fmt::format("create dentry table fail, {}", status.error_str()));
    }
  }

  // create file inode talbe
  int64_t file_inode_table_id = 0;
  {
    KVStorage::TableOption option;
    MetaDataCodec::GetFileInodeTableRange(fs_id, option.start_key, option.end_key);
    std::string table_name = fmt::format("{}_{}_finode", param.fs_name, fs_id);
    Status status = kv_storage_->CreateTable(table_name, option, file_inode_table_id);
    if (!status.ok()) {
      cleanup(dentry_table_id, 0, "");
      return Status(pb::error::EINTERNAL, fmt::format("create file inode table fail, {}", status.error_str()));
    }
  }

  auto fs_info = GenFsInfo(fs_id, param);

  // create fs
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, fs_key, MetaDataCodec::EncodeFSValue(fs_info));
  if (!status.ok()) {
    cleanup(dentry_table_id, file_inode_table_id, "");
    return Status(pb::error::EBACKEND_STORE, fmt::format("put store fs fail, {}", status.error_str()));
  }

  // create FileSystem instance
  auto id_generator = AutoIncrementIdGenerator::New(coordinator_client_, kInoTableId, kInoStartId, kInoBatchSize);
  CHECK(id_generator != nullptr) << "new id generator fail.";
  auto fs = FileSystem::New(fs_info, std::move(id_generator), kv_storage_);
  CHECK(AddFileSystem(fs)) << fmt::format("add FileSystem({}) fail.", fs->FsId());

  // create root inode
  status = fs->CreateRoot();
  if (!status.ok()) {
    cleanup(dentry_table_id, file_inode_table_id, fs_key);
    return Status(pb::error::EINTERNAL, fmt::format("create root fail, {}", status.error_str()));
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
  CHECK(!fs_name.empty()) << "fs name is empty.";

  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}), {}.", fs_name, status.error_str()));
  }

  auto fs_info = MetaDataCodec::DecodeFSValue(value);

  if (IsExistMountPoint(fs_info, mount_point)) {
    return Status(pb::error::EEXISTED, "mountPoint already exist.");
  }

  fs_info.add_mount_points()->CopyFrom(mount_point);
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, fs_key, MetaDataCodec::EncodeFSValue(fs_info));
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put store fs fail, {}", status.error_str()));
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
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}), {}.", fs_name, status.error_str()));
  }

  auto fs_info = MetaDataCodec::DecodeFSValue(value);

  RemoveMountPoint(fs_info, mount_point);

  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, fs_key, MetaDataCodec::EncodeFSValue(fs_info));
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put store fs fail, {}", status.error_str()));
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
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}), {}.", fs_name, status.error_str()));
  }

  auto fs_info = MetaDataCodec::DecodeFSValue(value);
  if (fs_info.mount_points_size() > 0) {
    return Status(pb::error::EEXISTED, "Fs exist mount point.");
  }

  status = kv_storage_->Delete(fs_key);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("Delete fs fail, {}", status.error_str()));
  }

  KVStorage::WriteOption option;
  std::string delete_fs_name = fmt::format("{}_deleting", fs_name);
  status = kv_storage_->Put(option, MetaDataCodec::EncodeFSKey(delete_fs_name), MetaDataCodec::EncodeFSValue(fs_info));
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put store fs fail, {}", status.error_str()));
  }

  DeleteFileSystem(fs_info.fs_id());

  return Status::OK();
}

Status FileSystemSet::GetFsInfo(const std::string& fs_name, pb::mdsv2::FsInfo& fs_info) {
  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}), {}.", fs_name, status.error_str()));
  }

  fs_info = MetaDataCodec::DecodeFSValue(value);

  return Status::OK();
}

bool FileSystemSet::AddFileSystem(FileSystemPtr fs) {
  utils::WriteLockGuard lk(lock_);

  DINGO_LOG(INFO) << fmt::format("add filesystem {} {}.", fs->FsName(), fs->FsId());

  auto it = fs_map_.find(fs->FsId());
  if (it != fs_map_.end()) {
    DINGO_LOG(ERROR) << fmt::format("fs({}) already exist.", fs->FsId());
    return false;
  }

  fs_map_[fs->FsId()] = fs;

  return true;
}

void FileSystemSet::DeleteFileSystem(uint32_t fs_id) {
  utils::WriteLockGuard lk(lock_);

  fs_map_.erase(fs_id);
}

FileSystemPtr FileSystemSet::GetFileSystem(uint32_t fs_id) {
  utils::ReadLockGuard lk(lock_);

  auto it = fs_map_.find(fs_id);
  return it != fs_map_.end() ? it->second : nullptr;
}

std::vector<FileSystemPtr> FileSystemSet::GetAllFileSystem() {
  utils::ReadLockGuard lk(lock_);

  std::vector<FileSystemPtr> fses;
  fses.reserve(fs_map_.size());
  for (const auto& [fs_id, fs] : fs_map_) {
    fses.push_back(fs);
  }

  return fses;
}

bool FileSystemSet::LoadFileSystems() {
  Range range;
  MetaDataCodec::GetFsTableRange(range.start_key, range.end_key);

  // scan fs table from kv storage
  std::vector<KeyValue> kvs;
  auto status = kv_storage_->Scan(range, kvs);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("scan fs table fail, error: {}.", status.error_str());
    return false;
  }

  for (const auto& kv : kvs) {
    auto id_generator = AutoIncrementIdGenerator::New(coordinator_client_, kInoTableId, kInoStartId, kInoBatchSize);
    CHECK(id_generator != nullptr) << "new id generator fail.";

    auto fs_info = MetaDataCodec::DecodeFSValue(kv.value);
    DINGO_LOG(INFO) << fmt::format("load fs info({}).", fs_info.ShortDebugString());
    auto fs = FileSystem::New(fs_info, std::move(id_generator), kv_storage_);
    CHECK(AddFileSystem(fs)) << fmt::format("add FileSystem({}) fail.", fs->FsId());
  }

  return true;
}

}  // namespace mdsv2
}  // namespace dingofs
