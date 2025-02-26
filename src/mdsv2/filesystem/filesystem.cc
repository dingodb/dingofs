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

#include <butil/status.h>
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
#include "mdsv2/filesystem/id_generator.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/filesystem/mutation_merger.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/server.h"
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
DEFINE_uint32(filesystem_hash_bucket_num, 1024, "Filesystem hash bucket num.");

bool IsReserveNode(uint64_t ino) { return ino == kRootIno; }

bool IsReserveName(const std::string& name) { return name == kStatsName || name == kRecyleName; }

bool IsInvalidName(const std::string& name) { return name.empty() || name.size() > FLAGS_filesystem_name_max_size; }

static inline bool IsDir(uint64_t ino) { return (ino & 1) == 1; }

static inline bool IsFile(uint64_t ino) { return (ino & 1) == 0; }

FileSystem::FileSystem(int64_t self_mds_id, const pb::mdsv2::FsInfo& fs_info, IdGeneratorPtr id_generator,
                       KVStoragePtr kv_storage, RenamerPtr renamer, MutationMergerPtr mutation_merger)
    : self_mds_id_(self_mds_id),
      fs_info_(fs_info),
      id_generator_(std::move(id_generator)),
      kv_storage_(kv_storage),
      renamer_(renamer),
      mutation_merger_(mutation_merger) {
  can_serve_ = CanServe(self_mds_id);
};

FileSystemPtr FileSystem::GetSelfPtr() { return std::dynamic_pointer_cast<FileSystem>(shared_from_this()); }

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

bool FileSystem::CanServe(int64_t self_mds_id) {
  const auto& partition_policy = fs_info_.partition_policy();
  if (partition_policy.type() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    return partition_policy.mono().mds_id() == self_mds_id;
  } else if (partition_policy.type() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    return partition_policy.parent_hash().distributions().contains(self_mds_id);
  }

  return false;
}

Status FileSystem::GetPartition(uint64_t parent_ino, PartitionPtr& out_partition) {
  auto partition = GetPartitionFromCache(parent_ino);
  if (partition != nullptr) {
    out_partition = partition;
    return Status::OK();
  }

  DINGO_LOG(INFO) << fmt::format("dentry set cache missing {}.", parent_ino);

  auto status = GetPartitionFromStore(parent_ino, out_partition);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found partition({}), {}.", parent_ino, status.error_str()));
  }

  return Status::OK();
}

PartitionPtr FileSystem::GetPartitionFromCache(uint64_t parent_ino) { return partition_cache_.Get(parent_ino); }

Status FileSystem::GetPartitionFromStore(uint64_t parent_ino, PartitionPtr& out_partition) {
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
  auto partition = Partition::New(inode);

  // add child dentry
  for (size_t i = 1; i < kvs.size(); ++i) {
    const auto& kv = kvs.at(i);
    auto dentry = MetaDataCodec::DecodeDentryValue(kv.value);
    partition->PutChild(dentry);
  }

  out_partition = partition;

  return Status::OK();
}

Status FileSystem::GetInodeFromDentry(const Dentry& dentry, PartitionPtr& partition, InodePtr& out_inode) {
  InodePtr inode = dentry.Inode();
  if (inode != nullptr) {
    out_inode = inode;
    return Status::OK();
  }

  auto status = GetInode(dentry.Ino(), out_inode);
  if (!status.ok()) {
    return status;
  }

  partition->PutChild(Dentry(dentry, out_inode));
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
  partition_cache_.Put(dentry.Ino(), Partition::New(inode));

  DINGO_LOG(INFO) << fmt::format("create filesystem({}) root success.", fs_id);

  return Status::OK();
}

Status FileSystem::Lookup(uint64_t parent_ino, const std::string& name, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("Lookup parent_ino({}), name({}).", parent_ino, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  PartitionPtr partition;
  auto status = GetPartition(parent_ino, partition);
  if (!status.ok()) {
    return status;
  }

  Dentry dentry;
  if (!partition->GetChild(name, dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("dentry({}) not found.", name));
  }

  InodePtr inode = dentry.Inode();
  if (inode == nullptr) {
    auto status = GetInode(dentry.Ino(), inode);
    if (!status.ok()) {
      return status;
    }

    partition->PutChild(Dentry(dentry, inode));
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

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

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
  PartitionPtr partition;
  auto status = GetPartition(parent_ino, partition);
  if (!status.ok()) {
    return status;
  }
  auto parent_inode = partition->ParentInode();

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

  // update parent inode
  Inode parent_inode_copy(*parent_inode);
  parent_inode_copy.SetNlinkDelta(1, now_time);

  // update backend store
  bthread::CountdownEvent count_down(2);

  uint64_t start_us = Helper::TimestampUs();
  DINGO_LOG(INFO) << fmt::format("mknod {} start.", param.name);

  std::vector<Mutation> mutations;

  butil::Status rpc_status;
  mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, inode->CopyTo()}, &count_down, &rpc_status));

  butil::Status rpc_parent_status;
  mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, parent_inode_copy.CopyTo()},
                               {Mutation::OpType::kPut, dentry.CopyTo()}, &count_down, &rpc_parent_status));

  if (!mutation_merger_->CommitMutation(mutations)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  DINGO_LOG(INFO) << fmt::format("mknod {} finish, elapsed_time({}us) rpc_status({}) rpc_parent_status({}).",
                                 param.name, Helper::TimestampUs() - start_us, rpc_status.error_str(),
                                 rpc_parent_status.error_str());

  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", rpc_status.error_str()));
  }
  if (!rpc_parent_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put parent fail, {}", rpc_parent_status.error_str()));
  }

  // update cache
  inode_cache_.PutInode(ino, inode);
  partition->PutChild(dentry);
  parent_inode->SetNlinkDelta(1, now_time);

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

Status FileSystem::Open(uint64_t ino) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

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
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  open_files_.Close(ino);

  return Status::OK();
}

Status FileSystem::MkDir(const MkDirParam& param, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("MkDir parent_ino({}), name({}).", param.parent_ino, param.name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

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
  PartitionPtr partition;
  auto status = GetPartition(parent_ino, partition);
  if (!status.ok()) {
    return status;
  }
  auto parent_inode = partition->ParentInode();

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

  // update parent inode
  Inode parent_inode_copy(*parent_inode);
  parent_inode_copy.SetNlinkDelta(1, now_time);

  // update backend store
  bthread::CountdownEvent count_down(2);

  uint64_t start_us = Helper::TimestampUs();
  DINGO_LOG(INFO) << fmt::format("mkdir {} start.", param.name);

  std::vector<Mutation> mutations;

  butil::Status rpc_status;
  mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, inode->CopyTo()}, &count_down, &rpc_status));

  butil::Status rpc_parent_status;
  mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, parent_inode_copy.CopyTo()},
                               {Mutation::OpType::kPut, dentry.CopyTo()}, &count_down, &rpc_parent_status));

  if (!mutation_merger_->CommitMutation(mutations)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  DINGO_LOG(INFO) << fmt::format("mkdir {} finish, elapsed_time({}us) rpc_status({}) rpc_parent_status({}).",
                                 param.name, Helper::TimestampUs() - start_us, rpc_status.error_str(),
                                 rpc_parent_status.error_str());

  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", rpc_status.error_str()));
  }
  if (!rpc_parent_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put parent fail, {}", rpc_parent_status.error_str()));
  }

  DINGO_LOG(INFO) << "here 0005.";

  // update cache
  inode_cache_.PutInode(ino, inode);
  partition->PutChild(dentry);
  partition_cache_.Put(ino, Partition::New(inode));
  parent_inode->SetNlinkDelta(1, now_time);

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

Status FileSystem::RmDir(uint64_t parent_ino, const std::string& name) {
  DINGO_LOG(DEBUG) << fmt::format("RmDir parent_ino({}), name({}).", parent_ino, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  PartitionPtr parent_partition;
  auto status = GetPartition(parent_ino, parent_partition);
  if (!status.ok()) {
    return status;
  }

  Dentry dentry;
  if (!parent_partition->GetChild(name, dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("child dentry({}) not found.", name));
  }

  PartitionPtr partition;
  status = GetPartition(dentry.Ino(), partition);
  if (!status.ok()) {
    return status;
  }

  InodePtr inode = partition->ParentInode();
  CHECK(inode != nullptr) << fmt::format("inode({}) is null.", dentry.Ino());

  DINGO_LOG(INFO) << fmt::format("remove dir {}/{} nlink({}).", parent_ino, name, inode->Nlink());

  // check whether dir is empty
  if (partition->HasChild() || inode->Nlink() > 2) {
    return Status(pb::error::ENOT_EMPTY, fmt::format("dir({}/{}) is not empty.", parent_ino, name));
  }

  uint32_t fs_id = inode->FsId();
  uint64_t now_ns = Helper::TimestampNs();

  // update parent inode
  auto parent_inode = parent_partition->ParentInode();
  Inode parent_inode_copy(*parent_inode);
  parent_inode_copy.SetNlinkDelta(-1, now_ns);

  // update backend store
  bthread::CountdownEvent count_down(2);

  uint64_t start_us = Helper::TimestampUs();
  DINGO_LOG(INFO) << fmt::format("rmdir {}/{} start.", parent_ino, name);

  std::vector<Mutation> mutations;

  butil::Status rpc_status;
  mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, inode->CopyTo()}, &count_down, &rpc_status));

  butil::Status rpc_parent_status;
  mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, parent_inode_copy.CopyTo()},
                               {Mutation::OpType::kDelete, dentry.CopyTo()}, &count_down, &rpc_parent_status));

  if (!mutation_merger_->CommitMutation(mutations)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  DINGO_LOG(INFO) << fmt::format("rmdir {}/{} finish, elapsed_time({}us) rpc_status({}) rpc_parent_status({}).",
                                 parent_ino, name, Helper::TimestampUs() - start_us, rpc_status.error_str(),
                                 rpc_parent_status.error_str());

  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", rpc_status.error_str()));
  }
  if (!rpc_parent_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put parent fail, {}", rpc_parent_status.error_str()));
  }

  // update cache
  parent_partition->DeleteChild(name);
  partition_cache_.Delete(dentry.Ino());
  parent_inode->SetNlinkDelta(-1, now_ns);

  return Status::OK();
}

Status FileSystem::ReadDir(uint64_t ino, const std::string& last_name, uint limit, bool with_attr,
                           std::vector<EntryOut>& entry_outs) {
  DINGO_LOG(DEBUG) << fmt::format("ReadDir ino({}), last_name({}), limit({}), with_attr({}).", ino, last_name, limit,
                                  with_attr);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  PartitionPtr partition;
  auto status = GetPartition(ino, partition);
  if (!status.ok()) {
    return status;
  }

  entry_outs.reserve(limit);
  auto dentries = partition->GetChildren(last_name, limit, false);
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
        partition->PutChild(Dentry(dentry, inode));
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

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  PartitionPtr partition;
  auto status = GetPartition(new_parent_ino, partition);
  if (!status.ok()) {
    return status;
  }
  auto parent_inode = partition->ParentInode();

  // get inode
  InodePtr inode;
  status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  uint32_t fs_id = inode->FsId();

  // build dentry
  Dentry dentry(fs_id, new_name, new_parent_ino, ino, pb::mdsv2::FileType::FILE, 0, inode);

  uint64_t now_time = Helper::TimestampNs();

  // update inode mtime/ctime/nlink
  Inode inode_copy(*inode);
  inode_copy.SetNlinkDelta(1, now_time);

  // update parent inode mtime/ctime/nlink
  Inode parent_inode_copy(*parent_inode);
  parent_inode_copy.SetNlinkDelta(1, now_time);

  // update backend store
  bthread::CountdownEvent count_down(2);

  uint64_t start_us = Helper::TimestampUs();
  DINGO_LOG(INFO) << fmt::format("link {} -> {}/{} start.", ino, new_parent_ino, new_name);

  std::vector<Mutation> mutations;

  butil::Status rpc_status;
  mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, inode_copy.CopyTo()}, &count_down, &rpc_status));

  butil::Status rpc_parent_status;
  mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, parent_inode_copy.CopyTo()},
                               {Mutation::OpType::kPut, dentry.CopyTo()}, &count_down, &rpc_parent_status));

  if (!mutation_merger_->CommitMutation(mutations)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  DINGO_LOG(INFO) << fmt::format("link {} -> {}/{} finish, elapsed_time({}us) rpc_status({}) rpc_parent_status({}).",
                                 ino, new_parent_ino, new_name, Helper::TimestampUs() - start_us,
                                 rpc_status.error_str(), rpc_parent_status.error_str());

  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", rpc_status.error_str()));
  }
  if (!rpc_parent_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put parent fail, {}", rpc_parent_status.error_str()));
  }

  // update cache
  inode->SetNlinkDelta(1, now_time);
  parent_inode->SetNlinkDelta(1, now_time);

  inode_cache_.PutInode(ino, inode);
  partition->PutChild(dentry);

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

// delete hard link for file
// 1. delete dentry
// 2. update inode mtime/ctime/nlink
// 3. update parent inode mtime/ctime/nlink
Status FileSystem::UnLink(uint64_t parent_ino, const std::string& name) {
  DINGO_LOG(DEBUG) << fmt::format("UnLink parent_ino({}), name({}).", parent_ino, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  PartitionPtr partition;
  auto status = GetPartition(parent_ino, partition);
  if (!status.ok()) {
    return status;
  }

  Dentry dentry;
  if (!partition->GetChild(name, dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found dentry({}/{})", parent_ino, name));
  }

  uint64_t ino = dentry.Ino();
  InodePtr inode;
  status = GetInodeFromDentry(dentry, partition, inode);
  if (!status.ok()) {
    return status;
  }

  uint32_t fs_id = inode->FsId();
  uint64_t now_time = Helper::TimestampNs();

  // update inode mtime/ctime/nlink
  Inode inode_copy(*inode);
  inode_copy.SetNlinkDelta(-1, now_time);

  // update parent inode mtime/ctime/nlink
  auto parent_inode = partition->ParentInode();
  Inode parent_inode_copy(*parent_inode);
  parent_inode_copy.SetNlinkDelta(1, now_time);

  // update backend store
  bthread::CountdownEvent count_down(2);

  uint64_t start_us = Helper::TimestampUs();
  DINGO_LOG(INFO) << fmt::format("unlink {}/{} start.", parent_ino, name);

  std::vector<Mutation> mutations;

  butil::Status rpc_status;
  mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, inode_copy.CopyTo()}, &count_down, &rpc_status));

  butil::Status rpc_parent_status;
  mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, parent_inode_copy.CopyTo()},
                               {Mutation::OpType::kDelete, dentry.CopyTo()}, &count_down, &rpc_parent_status));

  if (!mutation_merger_->CommitMutation(mutations)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  DINGO_LOG(INFO) << fmt::format("unlink {}/{} finish, elapsed_time({}us) rpc_status({}) rpc_parent_status({}).",
                                 parent_ino, name, Helper::TimestampUs() - start_us, rpc_status.error_str(),
                                 rpc_parent_status.error_str());

  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", rpc_status.error_str()));
  }
  if (!rpc_parent_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put parent fail, {}", rpc_parent_status.error_str()));
  }

  // update cache
  inode->SetNlinkDelta(-1, now_time);
  parent_inode->SetNlinkDelta(1, now_time);

  partition->DeleteChild(name);

  // if nlink is 0, delete inode
  if (inode->Nlink() == 0) {
    return DestoryInode(fs_id, ino);
  }

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

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  if (new_parent_ino == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid parent_ino param.");
  }
  if (IsInvalidName(new_name)) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid name param.");
  }

  uint32_t fs_id = fs_info_.fs_id();

  PartitionPtr partition;
  auto status = GetPartition(new_parent_ino, partition);
  if (!status.ok()) {
    return status;
  }

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

  // update parent inode
  auto parent_inode = partition->ParentInode();
  Inode parent_inode_copy(*parent_inode);
  parent_inode_copy.SetNlinkDelta(1, now_time);

  // update backend store
  bthread::CountdownEvent count_down(2);

  uint64_t start_us = Helper::TimestampUs();
  DINGO_LOG(INFO) << fmt::format("symlink {}/{} start.", new_parent_ino, new_name);

  std::vector<Mutation> mutations;

  butil::Status rpc_status;
  mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, inode->CopyTo()}, &count_down, &rpc_status));

  butil::Status rpc_parent_status;
  mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, parent_inode_copy.CopyTo()},
                               {Mutation::OpType::kDelete, dentry.CopyTo()}, &count_down, &rpc_parent_status));

  if (!mutation_merger_->CommitMutation(mutations)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  DINGO_LOG(INFO) << fmt::format("symlink {}/{} finish, elapsed_time({}us) rpc_status({}) rpc_parent_status({}).",
                                 new_parent_ino, new_name, Helper::TimestampUs() - start_us, rpc_status.error_str(),
                                 rpc_parent_status.error_str());

  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", rpc_status.error_str()));
  }
  if (!rpc_parent_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put parent fail, {}", rpc_parent_status.error_str()));
  }

  // update cache
  inode_cache_.PutInode(ino, inode);
  partition->PutChild(dentry);
  parent_inode->SetNlinkDelta(1, now_time);

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

Status FileSystem::ReadLink(uint64_t ino, std::string& link) {
  DINGO_LOG(DEBUG) << fmt::format("ReadLink ino({}).", ino);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

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

Status FileSystem::GetAttr(uint64_t ino, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("GetAttr ino({}).", ino);

  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  inode->CopyTo(entry_out.inode);

  return Status::OK();
}

Status FileSystem::SetAttr(uint64_t ino, const SetAttrParam& param, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("SetAttr ino({}).", ino);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  uint32_t fs_id = fs_info_.fs_id();

  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  // update store inode
  Inode inode_copy(*inode);
  inode_copy.SetAttr(param.inode, param.to_set);

  // update backend store
  bthread::CountdownEvent count_down(1);

  butil::Status rpc_status;
  Mutation file_mutation(fs_id, {Mutation::OpType::kPut, inode_copy.CopyTo()}, &count_down, &rpc_status);

  if (!mutation_merger_->CommitMutation(file_mutation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put fail, {}", rpc_status.error_str()));
  }

  // update cache
  inode->SetAttr(param.inode, param.to_set);

  inode->CopyTo(entry_out.inode);

  return Status::OK();
}

Status FileSystem::GetXAttr(uint64_t ino, Inode::XAttrMap& xattr) {
  DINGO_LOG(DEBUG) << fmt::format("GetXAttr ino({}).", ino);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  xattr = inode->GetXAttrMap();

  return Status::OK();
}

Status FileSystem::GetXAttr(uint64_t ino, const std::string& name, std::string& value) {
  DINGO_LOG(DEBUG) << fmt::format("GetXAttr ino({}), name({}).", ino, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  value = inode->GetXAttr(name);

  return Status::OK();
}

Status FileSystem::SetXAttr(uint64_t ino, const std::map<std::string, std::string>& xattr) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  uint32_t fs_id = fs_info_.fs_id();

  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  // update store inode
  Inode inode_copy(*inode);
  inode_copy.SetXAttr(xattr);

  // update backend store
  bthread::CountdownEvent count_down(1);

  butil::Status rpc_status;
  Mutation file_mutation(fs_id, {Mutation::OpType::kPut, inode_copy.CopyTo()}, &count_down, &rpc_status);

  if (!mutation_merger_->CommitMutation(file_mutation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put fail, {}", rpc_status.error_str()));
  }

  // update cache
  inode->SetXAttr(xattr);

  return Status::OK();
}

// Status FileSystem::Rename(uint64_t old_parent_ino, const std::string& old_name, uint64_t new_parent_ino,
//                           const std::string& new_name) {
//   DINGO_LOG(INFO) << fmt::format("rename {}/{} to {}/{}.", old_parent_ino, old_name, new_parent_ino, new_name);

//   uint32_t fs_id = fs_info_.fs_id();
//   uint64_t now_ns = Helper::TimestampNs();

//   // check name is valid
//   if (new_name.size() > FLAGS_filesystem_name_max_size) {
//     return Status(pb::error::EILLEGAL_PARAMTETER, "new name is too long.");
//   }

//   if (old_parent_ino == new_parent_ino && old_name == new_name) {
//     return Status(pb::error::EILLEGAL_PARAMTETER, "not allow same name");
//   }

//   // check old parent dentry/inode
//   PartitionPtr old_parent_partition;
//   auto status = GetPartition(old_parent_ino, old_parent_partition);
//   if (!status.ok()) {
//     return Status(pb::error::ENOT_FOUND,
//                   fmt::format("not found old parent dentry set({}), {}", old_parent_ino, status.error_str()));
//   }
//   InodePtr old_parent_inode = old_parent_partition->ParentInode();

//   // check new parent dentry/inode
//   PartitionPtr new_parent_partition;
//   status = GetPartition(new_parent_ino, new_parent_partition);
//   if (!status.ok()) {
//     return Status(pb::error::ENOT_FOUND,
//                   fmt::format("not found new parent dentry set({}), {}", old_parent_ino, status.error_str()));
//   }
//   InodePtr new_parent_inode = new_parent_partition->ParentInode();

//   // check old name dentry
//   Dentry old_dentry;
//   if (!old_parent_partition->GetChild(old_name, old_dentry)) {
//     return Status(pb::error::ENOT_FOUND, fmt::format("not found old dentry({}/{})", old_parent_ino, old_name));
//   }

//   InodePtr old_inode;
//   status = GetInodeFromDentry(old_dentry, old_parent_partition, old_inode);
//   if (!status.ok()) {
//     return status;
//   }

//   uint64_t start_us = Helper::TimestampUs();
//   DINGO_LOG(INFO) << fmt::format("rename {}/{} -> {}/{} start.", old_parent_ino, old_name, new_parent_ino, new_name);

//   std::vector<Mutation> mutations;

//   bthread::CountdownEvent count_down(0);

//   butil::Status already_new_inode_rpc_status;
//   butil::Status rpc_old_parent_status;
//   butil::Status rpc_new_parent_status;

//   bool is_exist_new_dentry = false;
//   InodePtr new_inode;
//   Dentry new_dentry;
//   // check new name dentry
//   if (new_parent_partition->GetChild(new_name, new_dentry)) {
//     is_exist_new_dentry = true;

//     if (new_dentry.Type() != old_dentry.Type()) {
//       return Status(pb::error::EILLEGAL_PARAMTETER, fmt::format("dentry type is different, old({}), new({}).",
//                                                                 pb::mdsv2::FileType_Name(old_dentry.Type()),
//                                                                 pb::mdsv2::FileType_Name(new_dentry.Type())));
//     }

//     if (new_dentry.Type() == pb::mdsv2::FileType::DIRECTORY) {
//       // check whether dir is empty
//       PartitionPtr new_partition;
//       auto status = GetPartition(new_dentry.Ino(), new_partition);
//       if (!status.ok()) {
//         return Status(pb::error::ENOT_FOUND,
//                       fmt::format("not found new dentry set({}), {}", new_dentry.Ino(), status.error_str()));
//       }

//       if (new_partition->HasChild()) {
//         return Status(pb::error::ENOT_EMPTY, fmt::format("new dentry({}/{}) is not empty.", new_parent_ino,
//         new_name));
//       }
//     }

//     // unlink new dentry inode
//     status = GetInodeFromDentry(new_dentry, new_parent_partition, new_inode);
//     if (!status.ok()) {
//       return status;
//     }

//     Inode new_inode_copy(*new_inode);
//     new_inode_copy.SetNlinkDelta(-1, now_ns);

//     if (new_inode_copy.Nlink() == 0) {
//       // destory inode
//       status = DestoryInode(fs_id, new_dentry.Ino());
//       if (!status.ok()) {
//         return status;
//       }
//     } else {
//       // update already new inode nlink
//       mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, new_inode_copy.CopyTo()}, &count_down,
//                                    &already_new_inode_rpc_status));
//       count_down.add_count(1);
//     }
//   }

//   // update old parent inode nlink and delete old dentry
//   Inode old_parent_inode_copy(*old_parent_inode);
//   old_parent_inode_copy.SetNlinkDelta(-1, now_ns);

//   mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, old_parent_inode_copy.CopyTo()},
//                                {Mutation::OpType::kDelete, old_dentry.CopyTo()}, &count_down,
//                                &rpc_old_parent_status));
//   count_down.add_count(1);

//   if (!is_exist_new_dentry) {
//     new_dentry = Dentry(fs_id, new_name, new_parent_ino, old_dentry.Ino(), old_dentry.Type(), 0, old_inode);

//     Inode new_parent_inode_copy(*new_parent_inode);
//     new_parent_inode_copy.SetNlinkDelta(1, now_ns);

//     // update new parent inode nlink and add new dentry
//     mutations.push_back(Mutation(fs_id, {Mutation::OpType::kPut, new_parent_inode_copy.CopyTo()},
//                                  {Mutation::OpType::kPut, new_dentry.CopyTo()}, &count_down,
//                                  &rpc_new_parent_status));
//     count_down.add_count(1);
//   }

//   if (!mutation_merger_->CommitMutation(mutations)) {
//     return Status(pb::error::EINTERNAL, "commit mutation fail");
//   }

//   CHECK(count_down.wait() == 0) << "count down wait fail.";

//   DINGO_LOG(INFO) << fmt::format(
//       "rename {}/{} -> {}/{} finish, elapsed_time({}us) already_new_inode_rpc_status({}) rpc_old_parent_status({}) "
//       "rpc_parent_status({}) rpc_new_parent_status({}).",
//       old_parent_ino, old_name, new_parent_ino, new_name, Helper::TimestampUs() - start_us,
//       already_new_inode_rpc_status.error_str(), rpc_old_parent_status.error_str(),
//       rpc_new_parent_status.error_str());

//   if (!already_new_inode_rpc_status.ok()) {
//     return Status(pb::error::EBACKEND_STORE,
//                   fmt::format("put already new inode fail, {}", already_new_inode_rpc_status.error_str()));
//   }

//   if (!rpc_old_parent_status.ok()) {
//     return Status(pb::error::EBACKEND_STORE,
//                   fmt::format("put old parent inode fail, {}", rpc_old_parent_status.error_str()));
//   }

//   if (!rpc_new_parent_status.ok()) {
//     return Status(pb::error::EBACKEND_STORE,
//                   fmt::format("put new parent inode fail, {}", rpc_new_parent_status.error_str()));
//   }

//   // delete old dentry at cache
//   old_parent_partition->DeleteChild(old_name);
//   // update old parent inode nlink at cache
//   old_parent_inode->SetNlinkDelta(1, now_ns);

//   if (is_exist_new_dentry) {
//     // delete new dentry at cache
//     new_parent_partition->DeleteChild(new_name);
//   } else {
//     // update new parent inode nlink at cache
//     new_parent_inode->SetNlinkDelta(1, now_ns);
//   }

//   // add new dentry at cache
//   new_parent_partition->PutChild(new_dentry);

//   // delete old partition at cache
//   if (new_inode != nullptr && new_inode->Type() == pb::mdsv2::FileType::DIRECTORY) {
//     partition_cache_.Delete(new_inode->Ino());
//   }

//   return Status::OK();
// }

Status FileSystem::Rename(uint64_t old_parent_ino, const std::string& old_name, uint64_t new_parent_ino,
                          const std::string& new_name) {
  DINGO_LOG(INFO) << fmt::format("Rename {}/{} to {}/{}.", old_parent_ino, old_name, new_parent_ino, new_name);

  uint32_t fs_id = fs_info_.fs_id();
  uint64_t now_ns = Helper::TimestampNs();

  // check name is valid
  if (new_name.size() > FLAGS_filesystem_name_max_size) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "new name is too long.");
  }

  if (old_parent_ino == new_parent_ino && old_name == new_name) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "not allow same name");
  }

  // check old parent dentry/inode
  PartitionPtr old_parent_partition;
  auto status = GetPartition(old_parent_ino, old_parent_partition);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND,
                  fmt::format("not found old parent dentry set({}), {}", old_parent_ino, status.error_str()));
  }
  InodePtr old_parent_inode = old_parent_partition->ParentInode();

  // check new parent dentry/inode
  PartitionPtr new_parent_partition;
  status = GetPartition(new_parent_ino, new_parent_partition);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND,
                  fmt::format("not found new parent dentry set({}), {}", old_parent_ino, status.error_str()));
  }
  InodePtr new_parent_inode = new_parent_partition->ParentInode();

  // check old name dentry
  Dentry old_dentry;
  if (!old_parent_partition->GetChild(old_name, old_dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found old dentry({}/{})", old_parent_ino, old_name));
  }

  InodePtr old_inode;
  status = GetInodeFromDentry(old_dentry, old_parent_partition, old_inode);
  if (!status.ok()) {
    DINGO_LOG(INFO) << "rename 0001";
    return status;
  }

  std::vector<KeyValue> kvs;

  bool is_exist_new_dentry = false;
  InodePtr exist_new_inode;
  Dentry exist_new_dentry;
  // check exist new name dentry
  if (new_parent_partition->GetChild(new_name, exist_new_dentry)) {
    DINGO_LOG(INFO) << "rename 0002";
    is_exist_new_dentry = true;

    if (exist_new_dentry.Type() != old_dentry.Type()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, fmt::format("dentry type is different, old({}), new({}).",
                                                                pb::mdsv2::FileType_Name(old_dentry.Type()),
                                                                pb::mdsv2::FileType_Name(exist_new_dentry.Type())));
    }

    if (exist_new_dentry.Type() == pb::mdsv2::FileType::DIRECTORY) {
      DINGO_LOG(INFO) << "rename 0003";
      // check whether dir is empty
      PartitionPtr new_partition;
      auto status = GetPartition(exist_new_dentry.Ino(), new_partition);
      if (!status.ok()) {
        return Status(pb::error::ENOT_FOUND,
                      fmt::format("not found new dentry set({}), {}", exist_new_dentry.Ino(), status.error_str()));
      }

      if (new_partition->HasChild()) {
        return Status(pb::error::ENOT_EMPTY, fmt::format("new dentry({}/{}) is not empty.", new_parent_ino, new_name));
      }
    }

    DINGO_LOG(INFO) << "rename 0004";

    // unlink exist new dentry inode
    status = GetInodeFromDentry(exist_new_dentry, new_parent_partition, exist_new_inode);
    if (!status.ok()) {
      DINGO_LOG(INFO) << "rename 0005";
      return status;
    }

    Inode exist_new_inode_copy(*exist_new_inode);
    exist_new_inode_copy.SetNlinkDelta(-1, now_ns);

    KeyValue exist_new_inode_kv;
    if (exist_new_inode_copy.Nlink() == 0) {
      DINGO_LOG(INFO) << "rename 0006";
      // delete inode
      exist_new_inode_kv.opt_type = KeyValue::OpType::kDelete;
      exist_new_inode_kv.key = exist_new_inode->Type() == pb::mdsv2::FileType::DIRECTORY
                                   ? MetaDataCodec::EncodeDirInodeKey(fs_id, exist_new_dentry.Ino())
                                   : MetaDataCodec::EncodeFileInodeKey(fs_id, exist_new_dentry.Ino());

    } else {
      DINGO_LOG(INFO) << "rename 0007";
      // update exist new inode nlink
      exist_new_inode_kv.opt_type = KeyValue::OpType::kPut;
      exist_new_inode_kv.key = exist_new_inode->Type() == pb::mdsv2::FileType::DIRECTORY
                                   ? MetaDataCodec::EncodeDirInodeKey(fs_id, exist_new_dentry.Ino())
                                   : MetaDataCodec::EncodeFileInodeKey(fs_id, exist_new_dentry.Ino());
      exist_new_inode_kv.value = exist_new_inode->Type() == pb::mdsv2::FileType::DIRECTORY
                                     ? MetaDataCodec::EncodeDirInodeValue(exist_new_inode_copy.CopyTo())
                                     : MetaDataCodec::EncodeFileInodeValue(exist_new_inode_copy.CopyTo());
    }

    kvs.push_back(exist_new_inode_kv);
  }
  DINGO_LOG(INFO) << "rename 0008";

  // update old parent inode nlink and delete old dentry
  KeyValue old_dentry_kv;
  old_dentry_kv.opt_type = KeyValue::OpType::kDelete;
  old_dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, old_parent_ino, old_name);
  kvs.push_back(old_dentry_kv);

  Inode old_parent_inode_copy(*old_parent_inode);
  old_parent_inode_copy.SetNlinkDelta(-1, now_ns);

  KeyValue old_parent_kv;
  old_parent_kv.opt_type = KeyValue::OpType::kPut;
  old_parent_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, old_parent_ino);
  old_parent_kv.value = MetaDataCodec::EncodeDirInodeValue(old_parent_inode_copy.CopyTo());
  kvs.push_back(old_parent_kv);

  // add or update new dentry
  Dentry new_dentry(fs_id, new_name, new_parent_ino, old_dentry.Ino(), old_dentry.Type(), 0, old_inode);
  KeyValue new_dentry_kv;
  new_dentry_kv.opt_type = KeyValue::OpType::kPut;
  new_dentry_kv.key = MetaDataCodec::EncodeDentryKey(fs_id, new_parent_ino, new_name);
  new_dentry_kv.value = MetaDataCodec::EncodeDentryValue(new_dentry.CopyTo());
  kvs.push_back(new_dentry_kv);

  // update new parent inode nlink/ctime/mtime
  Inode new_parent_inode_copy(*new_parent_inode);
  if (is_exist_new_dentry) {
    new_parent_inode_copy.SetNlinkDelta(0, now_ns);
  } else {
    new_parent_inode_copy.SetNlinkDelta(1, now_ns);
  }

  KeyValue new_parent_kv;
  new_parent_kv.opt_type = KeyValue::OpType::kPut;
  new_parent_kv.key = MetaDataCodec::EncodeDirInodeKey(fs_id, new_parent_ino);
  new_parent_kv.value = MetaDataCodec::EncodeDirInodeValue(new_parent_inode_copy.CopyTo());
  kvs.push_back(new_parent_kv);

  uint64_t start_us = Helper::TimestampUs();
  DINGO_LOG(INFO) << fmt::format("rename {}/{} -> {}/{} start.", old_parent_ino, old_name, new_parent_ino, new_name);

  status = kv_storage_->Put(KVStorage::WriteOption(), kvs);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put fail, {}", status.error_str()));
  }

  DINGO_LOG(INFO) << fmt::format("rename {}/{} -> {}/{} finish, elapsed_time({}us) status({}).", old_parent_ino,
                                 old_name, new_parent_ino, new_name, Helper::TimestampUs() - start_us,
                                 status.error_str());

  // delete old dentry at cache
  old_parent_partition->DeleteChild(old_name);
  // update old parent inode nlink at cache
  old_parent_inode->SetNlinkDelta(1, now_ns);

  if (is_exist_new_dentry) {
    DINGO_LOG(INFO) << "rename 0010";
    // delete new dentry at cache
    new_parent_partition->DeleteChild(new_name);
  } else {
    DINGO_LOG(INFO) << "rename 0011";
    // update new parent inode nlink at cache
    new_parent_inode->SetNlinkDelta(1, now_ns);
  }

  // add new dentry at cache
  new_parent_partition->PutChild(new_dentry);

  // delete exist new partition at cache
  // need notify mds to delete partition
  if (exist_new_inode != nullptr && exist_new_inode->Type() == pb::mdsv2::FileType::DIRECTORY) {
    DINGO_LOG(INFO) << "rename 0012";
    partition_cache_.Delete(exist_new_inode->Ino());
  }

  DINGO_LOG(INFO) << "rename 0013";

  return Status::OK();
}

Status FileSystem::AsyncRename(uint64_t old_parent_ino, const std::string& old_name, uint64_t new_parent_ino,
                               const std::string& new_name, RenameCbFunc cb) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  DINGO_LOG(INFO) << "here 0001";

  bool ret = renamer_->Execute(GetSelfPtr(), old_parent_ino, old_name, new_parent_ino, new_name, cb);

  return ret ? Status::OK() : Status(pb::error::EINTERNAL, "async rename commit fail");
}

Status FileSystem::WriteSlice(uint64_t ino, uint64_t chunk_index, const pb::mdsv2::SliceList& slice_list) {
  DINGO_LOG(DEBUG) << fmt::format("WriteSlice ino({}), chunk_index({}), slice_list.size({}).", ino, chunk_index,
                                  slice_list.slices_size());

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  Inode inode_copy(*inode);

  KeyValue kv;
  kv.opt_type = KeyValue::OpType::kPut;
  kv.key = MetaDataCodec::EncodeFileInodeKey(fs_info_.fs_id(), ino);

  inode_copy.AppendChunk(chunk_index, slice_list);
  kv.value = MetaDataCodec::EncodeFileInodeValue(inode_copy.CopyTo());

  status = kv_storage_->Put(KVStorage::WriteOption(), kv);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put fail, {}", status.error_str()));
  }

  inode->AppendChunk(chunk_index, slice_list);

  return Status::OK();
}

Status FileSystem::ReadSlice(uint64_t ino, uint64_t chunk_index, pb::mdsv2::SliceList& out_slice_list) {
  DINGO_LOG(DEBUG) << fmt::format("ReadSlice ino({}), chunk_index({}).", ino, chunk_index);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodePtr inode;
  auto status = GetInode(ino, inode);
  if (!status.ok()) {
    return status;
  }

  out_slice_list = inode->GetChunk(chunk_index);

  return Status::OK();
}

Status FileSystem::UpdatePartitionPolicy(uint64_t mds_id) {
  CHECK(fs_info_.partition_policy().type() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION)
      << "invalid partition polocy type.";

  auto fs_info_copy = fs_info_;
  auto* mono = fs_info_copy.mutable_partition_policy()->mutable_mono();
  mono->set_epoch(mono->epoch() + 1);
  mono->set_mds_id(mds_id);

  std::string key = MetaDataCodec::EncodeFSKey(fs_info_copy.fs_name());
  std::string value = MetaDataCodec::EncodeFSValue(fs_info_copy);

  KVStorage::WriteOption option;
  auto status = kv_storage_->Put(option, key, value);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put store fs fail, {}", status.error_str()));
  }

  *fs_info_.mutable_partition_policy()->mutable_mono() = *mono;

  return Status::OK();
}

Status FileSystem::UpdatePartitionPolicy(const std::map<uint64_t, pb::mdsv2::HashPartition::BucketSet>& distributions) {
  CHECK(fs_info_.partition_policy().type() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION)
      << "invalid partition polocy type.";

  auto fs_info_copy = fs_info_;
  auto* hash = fs_info_copy.mutable_partition_policy()->mutable_parent_hash();
  hash->set_epoch(hash->epoch() + 1);
  hash->mutable_distributions()->clear();
  for (const auto& [mds_id, bucket_set] : distributions) {
    hash->mutable_distributions()->insert({mds_id, bucket_set});
  }

  std::string key = MetaDataCodec::EncodeFSKey(fs_info_copy.fs_name());
  std::string value = MetaDataCodec::EncodeFSValue(fs_info_copy);

  KVStorage::WriteOption option;
  auto status = kv_storage_->Put(option, key, value);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put store fs fail, {}", status.error_str()));
  }

  *fs_info_.mutable_partition_policy()->mutable_parent_hash() = *hash;

  return Status::OK();
}

FileSystemSet::FileSystemSet(CoordinatorClientPtr coordinator_client, IdGeneratorPtr id_generator,
                             KVStoragePtr kv_storage, MDSMeta self_mds_meta, MDSMetaMapPtr mds_meta_map,
                             RenamerPtr renamer, MutationMergerPtr mutation_merger)
    : coordinator_client_(coordinator_client),
      id_generator_(std::move(id_generator)),
      kv_storage_(kv_storage),
      self_mds_meta_(self_mds_meta),
      mds_meta_map_(mds_meta_map),
      renamer_(renamer),
      mutation_merger_(mutation_merger) {}

FileSystemSet::~FileSystemSet() {}  // NOLINT

bool FileSystemSet::Init() {
  CHECK(coordinator_client_ != nullptr) << "coordinator client is null.";
  CHECK(kv_storage_ != nullptr) << "kv_storage is null.";
  CHECK(mds_meta_map_ != nullptr) << "mds_meta_map is null.";
  CHECK(renamer_ != nullptr) << "renamer is null.";
  CHECK(mutation_merger_ != nullptr) << "mutation_merger is null.";

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

// gerenate parent hash partition
std::map<uint64_t, pb::mdsv2::HashPartition::BucketSet> GenParentHashDistribution(const std::vector<MDSMeta>& mds_metas,
                                                                                  uint32_t bucket_num) {
  std::map<uint64_t, pb::mdsv2::HashPartition::BucketSet> mds_bucket_map;
  for (const auto& mds_meta : mds_metas) {
    mds_bucket_map[mds_meta.ID()] = pb::mdsv2::HashPartition::BucketSet();
  }

  for (uint32_t i = 0; i < bucket_num; ++i) {
    const auto& mds_meta = mds_metas[i % mds_metas.size()];
    mds_bucket_map[mds_meta.ID()].add_bucket_ids(i);
  }

  return mds_bucket_map;
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
  fs_info.mutable_extra()->CopyFrom(param.fs_extra);

  auto mds_metas = mds_meta_map_->GetAllMDSMeta();
  auto* partition_policy = fs_info.mutable_partition_policy();
  partition_policy->set_type(param.partition_type);
  if (param.partition_type == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    auto* mono = partition_policy->mutable_mono();
    mono->set_epoch(1);
    int select_offset = Helper::GenerateRealRandomInteger(0, 1000) % mds_metas.size();
    mono->set_mds_id(mds_metas.at(select_offset).ID());

  } else if (param.partition_type == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    auto* parent_hash = partition_policy->mutable_parent_hash();
    parent_hash->set_epoch(1);
    parent_hash->set_bucket_num(FLAGS_filesystem_hash_bucket_num);

    auto mds_bucket_map = GenParentHashDistribution(mds_metas, FLAGS_filesystem_hash_bucket_num);
    for (const auto& [mds_id, bucket_set] : mds_bucket_map) {
      parent_hash->mutable_distributions()->insert({mds_id, bucket_set});
    }
  }

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
Status FileSystemSet::CreateFs(const CreateFsParam& param, pb::mdsv2::FsInfo& fs_info) {
  int64_t fs_id = 0;
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

  fs_info = GenFsInfo(fs_id, param);

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
  CHECK(id_generator->Init()) << "init id generator fail.";

  auto fs =
      FileSystem::New(self_mds_meta_.ID(), fs_info, std::move(id_generator), kv_storage_, renamer_, mutation_merger_);
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

Status FileSystemSet::RefreshFsInfo(const std::string& fs_name) {
  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}), {}.", fs_name, status.error_str()));
  }

  auto id_generator = AutoIncrementIdGenerator::New(coordinator_client_, kInoTableId, kInoStartId, kInoBatchSize);
  CHECK(id_generator != nullptr) << "new id generator fail.";

  auto fs_info = MetaDataCodec::DecodeFSValue(value);
  DINGO_LOG(INFO) << fmt::format("refresh fs info name({}) id({}).", fs_info.fs_name(), fs_info.fs_id());
  auto fs =
      FileSystem::New(self_mds_meta_.ID(), fs_info, std::move(id_generator), kv_storage_, renamer_, mutation_merger_);
  AddFileSystem(fs, true);

  return Status::OK();
}

Status FileSystemSet::AllocSliceId(uint32_t slice_num, std::vector<uint64_t>& slice_ids) {
  for (uint32_t i = 0; i < slice_num; ++i) {
    int64_t slice_id = 0;
    if (!slice_id_generator_->GenID(slice_id)) {
      return Status(pb::error::EINTERNAL, "generate slice id fail");
    }

    slice_ids.push_back(slice_id);
  }

  return Status::OK();
}

bool FileSystemSet::AddFileSystem(FileSystemPtr fs, bool is_force) {
  utils::WriteLockGuard lk(lock_);

  auto it = fs_map_.find(fs->FsId());
  if (it != fs_map_.end() && !is_force) {
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
    DINGO_LOG(INFO) << fmt::format("load fs info name({}) id({}).", fs_info.fs_name(), fs_info.fs_id());
    auto fs =
        FileSystem::New(self_mds_meta_.ID(), fs_info, std::move(id_generator), kv_storage_, renamer_, mutation_merger_);
    AddFileSystem(fs);
  }

  return true;
}

}  // namespace mdsv2
}  // namespace dingofs
