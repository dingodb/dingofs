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

#include <bthread/bthread.h>
#include <gflags/gflags_declare.h>
#include <sys/stat.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "butil/status.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mdsv2/common/constant.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/filesystem/codec.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/fs_info.h"
#include "mdsv2/filesystem/id_generator.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/filesystem/mutation_processor.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/server.h"
#include "mdsv2/service/service_access.h"
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

DECLARE_uint32(txn_max_retry_times);

bool IsReserveNode(uint64_t ino) { return ino == kRootIno; }

bool IsReserveName(const std::string& name) { return name == kStatsName || name == kRecyleName; }

bool IsInvalidName(const std::string& name) { return name.empty() || name.size() > FLAGS_filesystem_name_max_size; }

static inline bool IsDir(uint64_t ino) { return (ino & 1) == 1; }

static inline bool IsFile(uint64_t ino) { return (ino & 1) == 0; }

FileSystem::FileSystem(int64_t self_mds_id, FsInfoUPtr fs_info, IdGeneratorPtr id_generator, KVStoragePtr kv_storage,
                       RenamerPtr renamer, MutationProcessorPtr mutation_processor)
    : self_mds_id_(self_mds_id),
      fs_info_(std::move(fs_info)),
      fs_id_(fs_info_->GetFsId()),
      id_generator_(std::move(id_generator)),
      kv_storage_(kv_storage),
      renamer_(renamer),
      mutation_processor_(mutation_processor) {
  can_serve_ = CanServe(self_mds_id);
};

FileSystemPtr FileSystem::GetSelfPtr() { return std::dynamic_pointer_cast<FileSystem>(shared_from_this()); }

uint64_t FileSystem::Epoch() const {
  auto partition_policy = fs_info_->GetPartitionPolicy();
  if (partition_policy.type() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    return partition_policy.mono().epoch();

  } else if (partition_policy.type() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    return partition_policy.parent_hash().epoch();
  }

  return 0;
}

pb::mdsv2::PartitionType FileSystem::PartitionType() const { return fs_info_->GetPartitionType(); }

bool FileSystem::IsMonoPartition() const {
  return fs_info_->GetPartitionType() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION;
}
bool FileSystem::IsParentHashPartition() const {
  return fs_info_->GetPartitionType() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION;
}

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
  const auto& partition_policy = fs_info_->GetPartitionPolicy();
  if (partition_policy.type() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    return partition_policy.mono().mds_id() == self_mds_id;
  } else if (partition_policy.type() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    return partition_policy.parent_hash().distributions().contains(self_mds_id);
  }

  return false;
}

Status FileSystem::GetPartition(Context& ctx, uint64_t parent, PartitionPtr& out_partition) {
  return GetPartition(ctx, ctx.GetInodeVersion(), parent, out_partition);
}

Status FileSystem::GetPartition(Context& ctx, uint64_t version, uint64_t parent, PartitionPtr& out_partition) {
  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();

  if (bypass_cache) {
    auto status = GetPartitionFromStore(parent, "Bypass", out_partition);
    if (!status.ok()) {
      return Status(pb::error::ENOT_FOUND, fmt::format("not found partition({}), {}.", parent, status.error_str()));
    }

    return status;
  }

  auto partition = GetPartitionFromCache(parent);
  if (partition == nullptr) {
    auto status = GetPartitionFromStore(parent, "CacheMiss", out_partition);
    if (!status.ok()) {
      return Status(pb::error::ENOT_FOUND, fmt::format("not found partition({}), {}.", parent, status.error_str()));
    }

    return status;
  }

  auto parent_inode = partition->ParentInode();
  if (version > parent_inode->Version()) {
    auto status = GetPartitionFromStore(parent, "OutOfDate", out_partition);
    if (!status.ok()) {
      return Status(pb::error::ENOT_FOUND, fmt::format("not found partition({}), {}.", parent, status.error_str()));
    }

    return status;
  }

  trace.SetHitPartition();
  out_partition = partition;

  return Status::OK();
}

PartitionPtr FileSystem::GetPartitionFromCache(uint64_t parent_ino) { return partition_cache_.Get(parent_ino); }

Status FileSystem::GetPartitionFromStore(uint64_t parent_ino, const std::string& reason, PartitionPtr& out_partition) {
  // scan dentry from store
  Range range;
  MetaDataCodec::EncodeDentryRange(fs_id_, parent_ino, range.start_key, range.end_key);

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
      "invalid parent key({}/{}).", Helper::StringToHex(parent_kv.key), Helper::StringToHex(range.start_key));

  // build partition
  auto parent_inode = Inode::New(MetaDataCodec::DecodeDirInodeValue(parent_kv.value));
  auto partition = Partition::New(parent_inode);

  // add child dentry
  for (size_t i = 1; i < kvs.size(); ++i) {
    const auto& kv = kvs.at(i);
    auto dentry = MetaDataCodec::DecodeDentryValue(kv.value);
    partition->PutChild(dentry);
  }

  partition_cache_.Put(parent_ino, partition);

  out_partition = partition;

  DINGO_LOG(INFO) << fmt::format("[fs.{}] fetch partition({}), reason({}).", fs_id_, parent_ino, reason);

  return Status::OK();
}

Status FileSystem::GetInode(Context& ctx, Dentry& dentry, PartitionPtr partition, InodePtr& out_inode) {
  return GetInode(ctx, ctx.GetInodeVersion(), dentry, partition, out_inode);
}

Status FileSystem::GetInode(Context& ctx, uint64_t version, Dentry& dentry, PartitionPtr partition,
                            InodePtr& out_inode) {
  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();

  bool is_fetch = false;
  Status status;
  do {
    if (bypass_cache) {
      status = GetInodeFromStore(dentry.Ino(), "Bypass", out_inode);
      is_fetch = true;
      break;
    }

    auto inode = dentry.Inode();
    if (inode == nullptr) {
      status = GetInodeFromStore(dentry.Ino(), "CacheMiss", out_inode);
      is_fetch = true;
      break;
    }

    if (inode->Version() < version) {
      status = GetInodeFromStore(dentry.Ino(), "OutOfDate", out_inode);
      is_fetch = true;
      break;
    }

    out_inode = inode;
    trace.SetHitInode();

  } while (false);

  if (is_fetch && status.ok()) {
    partition->PutChild(Dentry(dentry, out_inode));
  }

  return status;
}

Status FileSystem::GetInode(Context& ctx, uint64_t ino, InodePtr& out_inode) {
  return GetInode(ctx, ctx.GetInodeVersion(), ino, out_inode);
}

Status FileSystem::GetInode(Context& ctx, uint64_t version, uint64_t ino, InodePtr& out_inode) {
  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();

  if (bypass_cache) {
    return GetInodeFromStore(ino, "Bypass", out_inode);
  }

  auto inode = GetInodeFromCache(ino);
  if (inode == nullptr) {
    return GetInodeFromStore(ino, "CacheMiss", out_inode);
  }

  if (inode->Version() < version) {
    return GetInodeFromStore(ino, "OutOfDate", out_inode);
  }

  out_inode = inode;
  trace.SetHitInode();

  return Status::OK();
}

InodePtr FileSystem::GetInodeFromCache(uint64_t ino) { return inode_cache_.GetInode(ino); }

Status FileSystem::GetInodeFromStore(uint64_t ino, const std::string& reason, InodePtr& out_inode) {
  std::string key =
      IsDir(ino) ? MetaDataCodec::EncodeDirInodeKey(fs_id_, ino) : MetaDataCodec::EncodeFileInodeKey(fs_id_, ino);
  std::string value;
  auto status = kv_storage_->Get(key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found inode({}), {}", ino, status.error_str()));
  }

  out_inode =
      Inode::New(IsDir(ino) ? MetaDataCodec::DecodeDirInodeValue(value) : MetaDataCodec::DecodeFileInodeValue(value));

  inode_cache_.PutInode(ino, out_inode);

  DINGO_LOG(INFO) << fmt::format("[fs.{}] fetch inode({}), reason({}).", fs_id_, ino, reason);

  return Status::OK();
}

Status FileSystem::DestoryInode(uint32_t fs_id, uint64_t ino) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] destory inode {}.", fs_id_, ino);

  std::string inode_key = MetaDataCodec::EncodeFileInodeKey(fs_id, ino);
  auto status = kv_storage_->Delete(inode_key);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("delete inode fail, {}", status.error_str()));
  }

  inode_cache_.DeleteInode(ino);

  return Status::OK();
}

Status FileSystem::CreateRoot() {
  CHECK(fs_id_ > 0) << "fs_id is invalid.";

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

  uint64_t now_ns = Helper::TimestampNs();

  pb::mdsv2::Inode pb_inode;
  pb_inode.set_fs_id(fs_id_);
  pb_inode.set_ino(kRootIno);
  pb_inode.set_length(0);
  pb_inode.set_uid(1008);
  pb_inode.set_gid(1008);
  pb_inode.set_mode(S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  pb_inode.set_nlink(kEmptyDirMinLinkNum);
  pb_inode.set_type(pb::mdsv2::FileType::DIRECTORY);
  pb_inode.set_rdev(0);

  pb_inode.set_ctime(now_ns);
  pb_inode.set_mtime(now_ns);
  pb_inode.set_atime(now_ns);

  auto inode = Inode::New(pb_inode);

  std::string inode_key = MetaDataCodec::EncodeDirInodeKey(fs_id_, inode->Ino());
  std::string inode_value = MetaDataCodec::EncodeDirInodeValue(inode->CopyTo());
  KVStorage::WriteOption option;
  auto status = kv_storage_->Put(option, inode_key, inode_value);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put root inode fail, {}", status.error_str()));
  }

  Dentry dentry(fs_id_, "/", kRootParentIno, kRootIno, pb::mdsv2::FileType::DIRECTORY, 0, inode);

  std::string dentry_key = MetaDataCodec::EncodeDentryKey(fs_id_, dentry.ParentIno(), dentry.Name());
  std::string dentry_value = MetaDataCodec::EncodeDentryValue(dentry.CopyTo());
  status = kv_storage_->Put(option, dentry_key, dentry_value);
  if (!status.ok()) {
    cleanup(inode_key);
    return Status(pb::error::EBACKEND_STORE, fmt::format("put root dentry fail, {}", status.error_str()));
  }

  inode_cache_.PutInode(inode->Ino(), inode);
  partition_cache_.Put(dentry.Ino(), Partition::New(inode));

  DINGO_LOG(INFO) << fmt::format("[fs.{}] create filesystem root success.", fs_id_);

  return Status::OK();
}

Status FileSystem::Lookup(Context& ctx, uint64_t parent_ino, const std::string& name, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] lookup parent_ino({}), name({}).", fs_id_, parent_ino, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  PartitionPtr partition;
  auto status = GetPartition(ctx, parent_ino, partition);
  if (!status.ok()) {
    return status;
  }

  Dentry dentry;
  if (!partition->GetChild(name, dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("dentry({}) not found.", name));
  }

  InodePtr inode;
  status = GetInode(ctx, 0, dentry, partition, inode);
  if (!status.ok()) {
    return status;
  }

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

Status FileSystem::CleanUpInode(InodePtr inode) {
  uint32_t fs_id = inode->FsId();
  uint64_t ino = inode->Ino();

  bthread::CountdownEvent count_down(1);
  MixMutation mix_mutation = {.fs_id = fs_id};

  Trace trace;
  Operation inode_operation(Operation::OpType::kDeleteInode, ino,
                            MetaDataCodec::EncodeFileInodeKey(fs_id, inode->Ino()), &count_down, &trace);
  inode_operation.SetDeleteInode(ino);
  mix_mutation.operations.push_back(&inode_operation);

  if (!mutation_processor_->Commit(mix_mutation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  butil::Status rpc_status;
  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("delete inode fail, {}.", rpc_status.error_str()));
  }

  return Status::OK();
}

Status FileSystem::CleanUpDentry(Dentry& dentry) {
  uint32_t fs_id = dentry.FsId();
  uint64_t ino = dentry.Ino();
  uint64_t parent_ino = dentry.ParentIno();

  bthread::CountdownEvent count_down(1);
  MixMutation mix_mutation = {.fs_id = fs_id};

  Trace trace;
  Operation dentry_operation(Operation::OpType::kDeleteDentry, parent_ino,
                             MetaDataCodec::EncodeDentryKey(fs_id, parent_ino, dentry.Name()), &count_down, &trace);
  dentry_operation.SetDeleteDentry(dentry.CopyTo(), 0);
  mix_mutation.operations.push_back(&dentry_operation);

  if (!mutation_processor_->Commit(mix_mutation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  butil::Status rpc_status = dentry_operation.status;

  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("delete dentry fail, {}.", rpc_status.error_str()));
  }

  return Status::OK();
}

Status FileSystem::RollbackFileNlink(uint32_t fs_id, uint64_t ino, int delta) {
  bthread::CountdownEvent count_down(1);
  MixMutation mix_mutation = {.fs_id = fs_id};

  Trace trace;
  Operation inode_operation(Operation::OpType::kUpdateInodeNlink, ino, MetaDataCodec::EncodeFileInodeKey(fs_id, ino),
                            &count_down, &trace);
  inode_operation.SetUpdateInodeNlink(ino, delta, 0);
  mix_mutation.operations.push_back(&inode_operation);

  if (!mutation_processor_->Commit(mix_mutation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  butil::Status rpc_status;
  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("delete dentry fail, {}.", rpc_status.error_str()));
  }

  return Status::OK();
}

uint64_t FileSystem::GetMdsIdByIno(uint64_t ino) {
  auto partition_policy = fs_info_->GetPartitionPolicy();
  const auto& parent_hash = partition_policy.parent_hash();

  uint64_t target_mds_id = 0;
  uint32_t target_bucket_id = ino % parent_hash.bucket_num();
  for (const auto& [mds_id, bucket_set] : parent_hash.distributions()) {
    for (const auto& bucket_id : bucket_set.bucket_ids()) {
      if (bucket_id == target_bucket_id) {
        target_mds_id = mds_id;
        break;
      }
    }

    if (target_mds_id > 0) {
      break;
    }
  }

  return target_mds_id;
}

// create file, need below steps:
// 1. create inode
// 2. create dentry and update parent inode(nlink/mtime/ctime)
Status FileSystem::MkNod(Context& ctx, const MkNodParam& param, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] mknod parent_ino({}), name({}).", fs_id_, param.parent_ino, param.name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();
  uint64_t parent_ino = param.parent_ino;

  // check request
  if (param.name.empty()) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "name is empty");
  }

  if (param.parent_ino == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "invalid parent inode id");
  }

  // get dentry set
  PartitionPtr partition;
  auto status = GetPartition(ctx, parent_ino, partition);
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
  uint64_t now_time = Helper::TimestampNs();

  pb::mdsv2::Inode pb_inode;
  pb_inode.set_fs_id(fs_id_);
  pb_inode.set_ino(ino);
  pb_inode.set_length(0);
  pb_inode.set_ctime(now_time);
  pb_inode.set_mtime(now_time);
  pb_inode.set_atime(now_time);
  pb_inode.set_uid(param.uid);
  pb_inode.set_gid(param.gid);
  pb_inode.set_mode(param.mode);
  pb_inode.set_nlink(1);
  pb_inode.set_type(pb::mdsv2::FileType::FILE);
  pb_inode.set_rdev(param.rdev);
  pb_inode.add_parent_inos(parent_ino);

  auto inode = Inode::New(pb_inode);

  // build dentry
  Dentry dentry(fs_id_, param.name, parent_ino, ino, pb::mdsv2::FileType::FILE, param.flag, inode);

  // update backend store
  bthread::CountdownEvent count_down(2);
  MixMutation mix_mutation = {.fs_id = fs_id_};

  Operation inode_operation(Operation::OpType::kCreateInode, ino,
                            MetaDataCodec::EncodeFileInodeKey(fs_id_, inode->Ino()), &count_down, &trace);
  inode_operation.SetCreateInode(inode->CopyTo());
  mix_mutation.operations.push_back(&inode_operation);

  Operation dentry_operation(Operation::OpType::kCreateDentry, parent_ino,
                             MetaDataCodec::EncodeDentryKey(fs_id_, parent_ino, dentry.Name()), &count_down, &trace);
  dentry_operation.SetCreateDentry(dentry.CopyTo(), now_time);
  mix_mutation.operations.push_back(&dentry_operation);

  if (!mutation_processor_->Commit(mix_mutation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  butil::Status& rpc_status = inode_operation.status;
  butil::Status& rpc_dentry_status = dentry_operation.status;
  auto& dentry_result = dentry_operation.result;

  DINGO_LOG(INFO) << fmt::format("[fs.{}] mknod {} finish, elapsed_time({}us) rpc_status({}) rpc_dentry_status({}).",
                                 fs_id_, param.name, (Helper::TimestampNs() - now_time) / 1000, rpc_status.error_str(),
                                 rpc_dentry_status.error_str());

  if (!rpc_status.ok() && !rpc_dentry_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", rpc_status.error_str()));

  } else if (!rpc_status.ok() && rpc_dentry_status.ok()) {
    CleanUpDentry(dentry);
    return rpc_status;

  } else if (rpc_status.ok() && !rpc_dentry_status.ok()) {
    CleanUpInode(inode);
    return rpc_dentry_status;
  }

  // update cache
  inode_cache_.PutInode(ino, inode);
  partition->PutChild(dentry);
  parent_inode->UpdateNlink(dentry_result.version, dentry_result.nlink, now_time);

  entry_out.inode.Swap(&pb_inode);

  return Status::OK();
}

Status FileSystem::Open(Context& ctx, uint64_t ino) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();

  auto inode = open_files_.IsOpened(ino);
  if (inode != nullptr) {
    return Status::OK();
  }

  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  open_files_.Open(ino, inode);

  return Status::OK();
}

Status FileSystem::Release(Context& ctx, uint64_t ino) {  // NOLINT
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  open_files_.Close(ino);

  return Status::OK();
}

// create directory, need below steps:
// 1. create inode
// 2. create dentry and update parent inode(nlink/mtime/ctime)
Status FileSystem::MkDir(Context& ctx, const MkDirParam& param, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] mkdir parent_ino({}), name({}).", fs_id_, param.parent_ino, param.name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();
  uint64_t parent_ino = param.parent_ino;

  // check request
  if (param.name.empty()) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "name is empty.");
  }

  if (param.parent_ino == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "invalid parent inode id.");
  }

  // get parent dentry
  PartitionPtr partition;
  auto status = GetPartition(ctx, parent_ino, partition);
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
  uint64_t now_time = Helper::TimestampNs();

  pb::mdsv2::Inode pb_inode;
  pb_inode.set_fs_id(fs_id_);
  pb_inode.set_ino(ino);
  pb_inode.set_length(4096);
  pb_inode.set_ctime(now_time);
  pb_inode.set_mtime(now_time);
  pb_inode.set_atime(now_time);
  pb_inode.set_uid(param.uid);
  pb_inode.set_gid(param.gid);
  pb_inode.set_mode(S_IFDIR | param.mode);
  pb_inode.set_nlink(kEmptyDirMinLinkNum);
  pb_inode.set_type(pb::mdsv2::FileType::DIRECTORY);
  pb_inode.set_rdev(param.rdev);

  auto inode = Inode::New(pb_inode);

  // build dentry
  Dentry dentry(fs_id_, param.name, parent_ino, ino, pb::mdsv2::FileType::DIRECTORY, param.flag, inode);

  // update backend store
  bthread::CountdownEvent count_down(2);
  MixMutation mix_mutation = {.fs_id = fs_id_};

  Operation inode_operation(Operation::OpType::kCreateInode, ino,
                            MetaDataCodec::EncodeDirInodeKey(fs_id_, inode->Ino()), &count_down, &trace);
  inode_operation.SetCreateInode(inode->CopyTo());
  mix_mutation.operations.push_back(&inode_operation);

  Operation dentry_operation(Operation::OpType::kCreateDentry, parent_ino,
                             MetaDataCodec::EncodeDentryKey(fs_id_, parent_ino, dentry.Name()), &count_down, &trace);
  dentry_operation.SetCreateDentry(dentry.CopyTo(), now_time);
  mix_mutation.operations.push_back(&dentry_operation);

  if (!mutation_processor_->Commit(mix_mutation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  butil::Status& rpc_status = inode_operation.status;
  butil::Status& rpc_dentry_status = dentry_operation.status;
  auto& dentry_result = dentry_operation.result;

  DINGO_LOG(INFO) << fmt::format("[fs.{}] mkdir {} finish, elapsed_time({}us) rpc_status({}) rpc_dentry_status({}).",
                                 fs_id_, param.name, (Helper::TimestampNs() - now_time) / 1000, rpc_status.error_str(),
                                 rpc_dentry_status.error_str());

  if (!rpc_status.ok() && !rpc_dentry_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", rpc_status.error_str()));

  } else if (!rpc_status.ok() && rpc_dentry_status.ok()) {
    CleanUpDentry(dentry);
    return rpc_status;

  } else if (rpc_status.ok() && !rpc_dentry_status.ok()) {
    CleanUpInode(inode);
    return rpc_dentry_status;
  }

  // update cache
  inode_cache_.PutInode(ino, inode);
  partition->PutChild(dentry);
  parent_inode->UpdateNlink(dentry_result.version, dentry_result.version, now_time);
  partition_cache_.Put(ino, Partition::New(inode));

  entry_out.inode.Swap(&pb_inode);

  return Status::OK();
}

Status FileSystem::RmDir(Context& ctx, uint64_t parent_ino, const std::string& name) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] rmdir parent_ino({}), name({}).", fs_id_, parent_ino, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();
  auto& trace_txn = trace.GetTxn();
  const bool bypass_cache = ctx.IsBypassCache();

  PartitionPtr parent_partition;
  auto status = GetPartition(ctx, parent_ino, parent_partition);
  if (!status.ok()) {
    return status;
  }

  Dentry dentry;
  if (!parent_partition->GetChild(name, dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("child dentry({}) not found.", name));
  }

  PartitionPtr partition = GetPartitionFromCache(dentry.Ino());
  if (partition != nullptr) {
    InodePtr inode = partition->ParentInode();
    if (inode->Nlink() > kEmptyDirMinLinkNum) {
      return Status(pb::error::ENOT_EMPTY,
                    fmt::format("dir({}/{}) is not empty, nlink({}).", parent_ino, name, inode->Nlink()));
    }
  }

  uint64_t now_ns = Helper::TimestampNs();

  pb::mdsv2::Inode pb_parent_inode;
  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

    std::string key = MetaDataCodec::EncodeDirInodeKey(fs_id_, dentry.Ino());
    std::string value;
    status = txn->Get(key, value);
    if (!status.ok()) {
      status = Status(pb::error::EBACKEND_STORE, fmt::format("get inode fail, {}", status.error_str()));
      break;
    }

    // check dir is empty by nlink
    pb::mdsv2::Inode pb_inode = MetaDataCodec::DecodeDirInodeValue(value);
    if (pb_inode.nlink() > kEmptyDirMinLinkNum) {
      status = Status(pb::error::ENOT_EMPTY,
                      fmt::format("dir({}/{}) is not empty, nlink({}).", parent_ino, name, pb_inode.nlink()));
      break;
    }

    // delete dir inode
    status = txn->Delete(key);
    CHECK(status.ok()) << fmt::format("delete inode({}) fail, {}", dentry.Ino(), status.error_str());

    // delete dentry
    status = txn->Delete(MetaDataCodec::EncodeDentryKey(fs_id_, parent_ino, name));
    CHECK(status.ok()) << fmt::format("delete dentry({}/{}) fail, {}", parent_ino, name, status.error_str());

    // update parent inode nlink/ctime/mtime
    std::string parent_key = MetaDataCodec::EncodeDirInodeKey(fs_id_, parent_ino);
    std::string parent_value;
    status = txn->Get(parent_key, parent_value);
    if (!status.ok()) {
      status = Status(pb::error::EBACKEND_STORE, fmt::format("get parent inode fail, {}", status.error_str()));
      break;
    }

    pb_parent_inode = MetaDataCodec::DecodeDirInodeValue(parent_value);
    pb_parent_inode.set_version(pb_parent_inode.version() + 1);
    pb_parent_inode.set_nlink(pb_parent_inode.nlink() - 1);
    pb_parent_inode.set_ctime(now_ns);
    pb_parent_inode.set_mtime(now_ns);

    status = txn->Put(parent_key, MetaDataCodec::EncodeDirInodeValue(pb_parent_inode));
    CHECK(status.ok()) << fmt::format("put parent inode({}) fail, {}", parent_ino, status.error_str());

    status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.txn_id = dentry.Ino();
  trace_txn.retry = retry;

  DINGO_LOG(INFO) << fmt::format("[fs.{}] rmdir dir {}/{} finish, elapsed_time({}us).", fs_id_, parent_ino, name,
                                 (Helper::TimestampNs() - now_ns) / 1000);
  if (!status.ok()) {
    return status;
  }

  // update cache
  parent_partition->DeleteChild(name);
  parent_partition->ParentInode()->UpdateNlink(pb_parent_inode.version(), pb_parent_inode.nlink(), now_ns);
  partition_cache_.Delete(dentry.Ino());

  return Status::OK();
}

Status FileSystem::ReadDir(Context& ctx, uint64_t ino, const std::string& last_name, uint limit, bool with_attr,
                           std::vector<EntryOut>& entry_outs) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] readdir ino({}), last_name({}), limit({}), with_attr({}).", fs_id_, ino,
                                  last_name, limit, with_attr);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  PartitionPtr partition;
  auto status = GetPartition(ctx, ino, partition);
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
      InodePtr inode;
      status = GetInode(ctx, 0, dentry, partition, inode);
      if (!status.ok()) {
        return status;
      }

      entry_out.inode = inode->CopyTo();
    }

    entry_outs.push_back(std::move(entry_out));
  }

  return Status::OK();
}

// create hard link for file
// 1. create dentry and update parent inode(nlink/mtime/ctime)
// 2. update inode(mtime/ctime/nlink)
Status FileSystem::Link(Context& ctx, uint64_t ino, uint64_t new_parent_ino, const std::string& new_name,
                        EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] link ino({}), new_parent_ino({}), new_name({}).", fs_id_, ino,
                                  new_parent_ino, new_name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  PartitionPtr partition;
  auto status = GetPartition(ctx, new_parent_ino, partition);
  if (!status.ok()) {
    return status;
  }
  auto parent_inode = partition->ParentInode();

  // get inode
  InodePtr inode;
  status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  uint32_t fs_id = inode->FsId();

  // build dentry
  Dentry dentry(fs_id, new_name, new_parent_ino, ino, pb::mdsv2::FileType::FILE, 0, inode);

  // update backend store
  uint64_t now_time = Helper::TimestampNs();

  bthread::CountdownEvent count_down(2);
  MixMutation mix_mutation = {.fs_id = fs_id};

  Operation inode_operation(Operation::OpType::kUpdateInodeNlink, ino,
                            MetaDataCodec::EncodeFileInodeKey(fs_id, inode->Ino()), &count_down, &trace);
  inode_operation.SetUpdateInodeNlink(ino, 1, now_time);
  mix_mutation.operations.push_back(&inode_operation);

  Operation dentry_operation(Operation::OpType::kCreateDentry, new_parent_ino,
                             MetaDataCodec::EncodeDentryKey(fs_id, new_parent_ino, dentry.Name()), &count_down, &trace);
  dentry_operation.SetCreateDentry(dentry.CopyTo(), now_time);
  mix_mutation.operations.push_back(&dentry_operation);

  if (!mutation_processor_->Commit(mix_mutation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  butil::Status& rpc_status = inode_operation.status;
  butil::Status& rpc_dentry_status = dentry_operation.status;
  auto& inode_result = inode_operation.result;
  auto& dentry_result = dentry_operation.result;

  DINGO_LOG(INFO) << fmt::format(
      "[fs.{}] link {} -> {}/{} finish, elapsed_time({}us) rpc_status({}) rpc_parent_status({}).", fs_id_, ino,
      new_parent_ino, new_name, (Helper::TimestampNs() - now_time) / 1000, rpc_status.error_str(),
      rpc_dentry_status.error_str());

  if (!rpc_status.ok() && !rpc_dentry_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", rpc_status.error_str()));

  } else if (!rpc_status.ok() && rpc_dentry_status.ok()) {
    RollbackFileNlink(fs_id, ino, -1);
    return rpc_status;

  } else if (rpc_status.ok() && !rpc_dentry_status.ok()) {
    CleanUpDentry(dentry);
    return rpc_dentry_status;
  }

  // update cache
  inode->UpdateNlink(inode_result.version, inode_result.nlink, now_time);
  parent_inode->UpdateNlink(dentry_result.version, dentry_result.nlink, now_time);

  inode_cache_.PutInode(ino, inode);
  partition->PutChild(dentry);

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

// delete hard link for file
// 1. delete dentry and update parent inode(nlink/mtime/ctime)
// 3. update inode(nlink/mtime/ctime)
Status FileSystem::UnLink(Context& ctx, uint64_t parent_ino, const std::string& name) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] unLink parent_ino({}), name({}).", fs_id_, parent_ino, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  PartitionPtr partition;
  auto status = GetPartition(ctx, parent_ino, partition);
  if (!status.ok()) {
    return status;
  }

  Dentry dentry;
  if (!partition->GetChild(name, dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found dentry({}/{})", parent_ino, name));
  }

  InodePtr inode;
  status = GetInode(ctx, dentry, partition, inode);
  if (!status.ok()) {
    return status;
  }

  uint64_t now_time = Helper::TimestampNs();
  // delete dentry
  {
    bthread::CountdownEvent count_down(1);
    MixMutation mix_mutation = {.fs_id = fs_id_};

    Operation dentry_operation(Operation::OpType::kDeleteDentry, parent_ino,
                               MetaDataCodec::EncodeDentryKey(fs_id_, parent_ino, dentry.Name()), &count_down, &trace);
    dentry_operation.SetDeleteDentry(dentry.CopyTo(), now_time);
    mix_mutation.operations.push_back(&dentry_operation);

    if (!mutation_processor_->Commit(mix_mutation)) {
      return Status(pb::error::EINTERNAL, "commit mutation fail");
    }

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    butil::Status& rpc_status = dentry_operation.status;
    auto& result = dentry_operation.result;

    DINGO_LOG(INFO) << fmt::format("[fs.{}] unlink {}/{} delete dentry finish, elapsed_time({}us) rpc_status({}).",
                                   fs_id_, parent_ino, name, (Helper::TimestampNs() - now_time) / 1000,
                                   rpc_status.error_str());

    if (!rpc_status.ok()) {
      return Status(pb::error::EBACKEND_STORE, fmt::format("delete dentry fail, {}", rpc_status.error_str()));
    }

    partition->ParentInode()->UpdateNlink(result.version, result.nlink, now_time);
    partition->DeleteChild(name);
  }

  // update file inode nlink
  {
    uint64_t start_time = Helper::TimestampNs();
    bthread::CountdownEvent count_down(1);
    MixMutation mix_mutation = {.fs_id = fs_id_};

    Operation file_operation(Operation::OpType::kUpdateInodeNlink, dentry.Ino(),
                             MetaDataCodec::EncodeFileInodeKey(fs_id_, dentry.Ino()), &count_down, &trace);
    file_operation.SetUpdateInodeNlink(dentry.Ino(), -1, now_time);
    mix_mutation.operations.push_back(&file_operation);

    if (!mutation_processor_->Commit(mix_mutation)) {
      return Status(pb::error::EINTERNAL, "commit mutation fail");
    }

    CHECK(count_down.wait() == 0) << "count down wait fail.";

    butil::Status& rpc_status = file_operation.status;
    auto& result = file_operation.result;

    DINGO_LOG(INFO) << fmt::format("[fs.{}] unlink {}/{} update nlink finish, elapsed_time({}us) rpc_status({}).",
                                   fs_id_, parent_ino, name, (Helper::TimestampNs() - start_time) / 1000,
                                   rpc_status.error_str());
    if (!rpc_status.ok()) {
      return Status(pb::error::EBACKEND_STORE, fmt::format("update nlink fail, {}", rpc_status.error_str()));
    }

    inode->UpdateNlink(result.version, result.nlink, now_time);
  }

  return Status::OK();
}

// create symbol link
// 1. create inode
// 2. create dentry
// 3. update parent inode mtime/ctime/nlink
Status FileSystem::Symlink(Context& ctx, const std::string& symlink, uint64_t new_parent_ino,
                           const std::string& new_name, uint32_t uid, uint32_t gid, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] symlink new_parent_ino({}), new_name({}) symlink({}).", fs_id_,
                                  new_parent_ino, new_name, symlink);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  if (new_parent_ino == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid parent_ino param.");
  }
  if (IsInvalidName(new_name)) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid name param.");
  }

  auto& trace = ctx.GetTrace();

  PartitionPtr partition;
  auto status = GetPartition(ctx, new_parent_ino, partition);
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
  uint64_t now_time = Helper::TimestampNs();

  pb::mdsv2::Inode pb_inode;
  pb_inode.set_fs_id(fs_id_);
  pb_inode.set_ino(ino);
  pb_inode.set_symlink(symlink);
  pb_inode.set_length(symlink.size());
  pb_inode.set_ctime(now_time);
  pb_inode.set_mtime(now_time);
  pb_inode.set_atime(now_time);
  pb_inode.set_uid(uid);
  pb_inode.set_gid(gid);
  pb_inode.set_mode(S_IFLNK | 0777);
  pb_inode.set_nlink(1);
  pb_inode.set_type(pb::mdsv2::FileType::SYM_LINK);
  pb_inode.set_rdev(1);
  pb_inode.add_parent_inos(new_parent_ino);

  auto inode = Inode::New(pb_inode);

  // build dentry
  Dentry dentry(fs_id_, new_name, new_parent_ino, ino, pb::mdsv2::FileType::SYM_LINK, 0, inode);

  // update backend store
  bthread::CountdownEvent count_down(2);
  MixMutation mix_mutation = {.fs_id = fs_id_};

  Operation inode_operation(Operation::OpType::kCreateInode, ino,
                            MetaDataCodec::EncodeFileInodeKey(fs_id_, inode->Ino()), &count_down, &trace);
  inode_operation.SetCreateInode(inode->CopyTo());
  mix_mutation.operations.push_back(&inode_operation);

  Operation dentry_operation(Operation::OpType::kCreateDentry, new_parent_ino,
                             MetaDataCodec::EncodeDentryKey(fs_id_, new_parent_ino, dentry.Name()), &count_down,
                             &trace);
  dentry_operation.SetCreateDentry(dentry.CopyTo(), now_time);
  mix_mutation.operations.push_back(&dentry_operation);

  if (!mutation_processor_->Commit(mix_mutation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  butil::Status& rpc_status = inode_operation.status;
  butil::Status& rpc_dentry_status = dentry_operation.status;
  auto& dentry_result = dentry_operation.result;

  DINGO_LOG(INFO) << fmt::format(
      "[fs.{}] symlink {}/{} finish, elapsed_time({}us) rpc_status({}) rpc_dentry_status({}).", fs_id_, new_parent_ino,
      new_name, (Helper::TimestampNs() - now_time) / 1000, rpc_status.error_str(), rpc_dentry_status.error_str());

  if (!rpc_status.ok() && !rpc_dentry_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", rpc_status.error_str()));

  } else if (!rpc_status.ok() && rpc_dentry_status.ok()) {
    CleanUpInode(inode);
    return rpc_status;

  } else if (rpc_status.ok() && !rpc_dentry_status.ok()) {
    CleanUpDentry(dentry);
    return rpc_dentry_status;
  }

  // update cache
  inode_cache_.PutInode(ino, inode);
  partition->PutChild(dentry);
  partition->ParentInode()->UpdateNlink(dentry_result.version, dentry_result.nlink, now_time);

  entry_out.inode.Swap(&pb_inode);

  return Status::OK();
}

Status FileSystem::ReadLink(Context& ctx, uint64_t ino, std::string& link) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] readlink ino({}).", fs_id_, ino);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodePtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  if (inode->Type() != pb::mdsv2::FileType::SYM_LINK) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "not symlink inode");
  }

  link = inode->Symlink();

  return Status::OK();
}

Status FileSystem::GetAttr(Context& ctx, uint64_t ino, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] getattr ino({}).", fs_id_, ino);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodePtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  inode->CopyTo(entry_out.inode);

  return Status::OK();
}

Status FileSystem::SetAttr(Context& ctx, uint64_t ino, const SetAttrParam& param, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] setattr ino({}).", fs_id_, ino);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  uint64_t now_time = Helper::TimestampNs();

  InodePtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  // update backend store
  bthread::CountdownEvent count_down(1);
  MixMutation mix_mutation = {.fs_id = fs_id_};

  std::string key = (inode->Type() == pb::mdsv2::DIRECTORY) ? MetaDataCodec::EncodeDirInodeKey(fs_id_, inode->Ino())
                                                            : MetaDataCodec::EncodeFileInodeKey(fs_id_, inode->Ino());
  Operation inode_operation(Operation::OpType::kUpdateInodeAttr, ino, key, &count_down, &trace);
  inode_operation.SetUpdateInodeAttr(param.inode, param.to_set);
  mix_mutation.operations.push_back(&inode_operation);

  if (!mutation_processor_->Commit(mix_mutation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  butil::Status& rpc_status = inode_operation.status;
  auto& result = inode_operation.result;

  DINGO_LOG(INFO) << fmt::format("[fs.{}] setattr {} finish, elapsed_time({}us) rpc_status({}).", fs_id_, ino,
                                 (Helper::TimestampNs() - now_time) / 1000, rpc_status.error_str());

  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", rpc_status.error_str()));
  }

  // update cache
  inode->UpdateAttr(result.version, param.inode, param.to_set);

  inode->CopyTo(entry_out.inode);

  return Status::OK();
}

Status FileSystem::GetXAttr(Context& ctx, uint64_t ino, Inode::XAttrMap& xattr) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] getxattr ino({}).", fs_id_, ino);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodePtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  xattr = inode->GetXAttrMap();

  return Status::OK();
}

Status FileSystem::GetXAttr(Context& ctx, uint64_t ino, const std::string& name, std::string& value) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] getxattr ino({}), name({}).", fs_id_, ino, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodePtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  value = inode->GetXAttr(name);

  return Status::OK();
}

Status FileSystem::SetXAttr(Context& ctx, uint64_t ino, const std::map<std::string, std::string>& xattrs) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  InodePtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  uint64_t now_time = Helper::TimestampNs();

  // update backend store
  bthread::CountdownEvent count_down(1);
  MixMutation mix_mutation = {.fs_id = fs_id_};

  std::string key = (inode->Type() == pb::mdsv2::DIRECTORY) ? MetaDataCodec::EncodeDirInodeKey(fs_id_, inode->Ino())
                                                            : MetaDataCodec::EncodeFileInodeKey(fs_id_, inode->Ino());
  Operation inode_operation(Operation::OpType::kUpdateInodeXAttr, ino, key, &count_down, &trace);
  inode_operation.SetUpdateInodeXAttr(xattrs);
  mix_mutation.operations.push_back(&inode_operation);

  if (!mutation_processor_->Commit(mix_mutation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  butil::Status& rpc_status = inode_operation.status;
  auto& result = inode_operation.result;

  DINGO_LOG(INFO) << fmt::format("[fs.{}] setxattr {} finish, elapsed_time({}us) rpc_status({}).", fs_id_, ino,
                                 (Helper::TimestampNs() - now_time) / 1000, rpc_status.error_str());

  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", rpc_status.error_str()));
  }

  // update cache
  inode->UpdateXAttr(result.version, xattrs);

  return Status::OK();
}

static void SendRefreshInode(uint64_t mds_id, uint32_t fs_id, const std::vector<uint64_t>& inoes) {
  MDSMeta mds_meta;
  if (!Server::GetInstance().GetMDSMetaMap()->GetMDSMeta(mds_id, mds_meta)) {
    DINGO_LOG(WARNING) << fmt::format("[fs.{}] not found mds({}) meta.", fs_id, mds_id);
    return;
  }

  butil::EndPoint endpoint;
  butil::str2endpoint(mds_meta.Host().c_str(), mds_meta.Port(), &endpoint);

  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] refresh inode({}) mds({}) fs({}).", fs_id, Helper::VectorToString(inoes),
                                  mds_id);
  auto status = ServiceAccess::RefreshInode(endpoint, fs_id, inoes);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("fs.{}] refresh inode({}) fail, mds({}) {}.", fs_id,
                                      Helper::VectorToString(inoes), mds_id, status.error_str());
  }
}

Status FileSystem::Rename(Context& ctx, uint64_t old_parent_ino, const std::string& old_name, uint64_t new_parent_ino,
                          const std::string& new_name, uint64_t& old_parent_version, uint64_t& new_parent_version) {
  DINGO_LOG(INFO) << fmt::format("fs.{}] rename {}/{} to {}/{}.", fs_id_, old_parent_ino, old_name, new_parent_ino,
                                 new_name);

  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();

  uint64_t now_ns = Helper::TimestampNs();

  // check name is valid
  if (new_name.size() > FLAGS_filesystem_name_max_size) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "new name is too long.");
  }

  if (old_parent_ino == new_parent_ino && old_name == new_name) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "not allow same name");
  }

  auto txn = kv_storage_->NewTxn();

  // get old parent inode
  std::string value;
  std::string old_parent_key = MetaDataCodec::EncodeDirInodeKey(fs_id_, old_parent_ino);
  auto status = txn->Get(old_parent_key, value);
  if (!status.ok()) {
    return Status(status.error_code(),
                  fmt::format("not found old parent inode({}), {}", old_parent_ino, status.error_str()));
  }

  pb::mdsv2::Inode pb_old_parent_inode = MetaDataCodec::DecodeDirInodeValue(value);

  // get old dentry
  std::string old_dentry_key = MetaDataCodec::EncodeDentryKey(fs_id_, old_parent_ino, old_name);
  value.clear();
  status = txn->Get(old_dentry_key, value);
  if (!status.ok()) {
    return Status(status.error_code(),
                  fmt::format("not found old dentry({}/{}) {}", old_parent_ino, old_name, status.error_str()));
  }

  pb::mdsv2::Dentry pb_old_dentry = MetaDataCodec::DecodeDentryValue(value);

  bool is_same_parent = (old_parent_ino == new_parent_ino);
  // get new parent inode
  pb::mdsv2::Inode pb_new_parent_inode;
  std::string new_parent_key = MetaDataCodec::EncodeDirInodeKey(fs_id_, new_parent_ino);
  if (!is_same_parent) {
    value.clear();
    status = txn->Get(new_parent_key, value);
    if (!status.ok()) {
      return Status(status.error_code(),
                    fmt::format("not found new parent inode({}), {}", new_parent_ino, status.error_str()));
    }
    pb_new_parent_inode = MetaDataCodec::DecodeDirInodeValue(value);

  } else {
    pb_new_parent_inode = pb_old_parent_inode;
  }

  // get new dentry
  std::string new_dentry_key = MetaDataCodec::EncodeDentryKey(fs_id_, new_parent_ino, new_name);
  value.clear();
  status = txn->Get(new_dentry_key, value);
  if (!status.ok() && status.error_code() != pb::error::ENOT_FOUND) {
    return Status(status.error_code(), fmt::format("get new dentry fail, {}", status.error_str()));
  }

  pb::mdsv2::Dentry pb_exist_new_dentry;
  bool is_exist_new_dentry = !value.empty();
  if (is_exist_new_dentry) {
    pb_exist_new_dentry = MetaDataCodec::DecodeDentryValue(value);

    // get exist new inode
    std::string new_inode_key = (pb_exist_new_dentry.type() == pb::mdsv2::DIRECTORY)
                                    ? MetaDataCodec::EncodeDirInodeKey(fs_id_, pb_exist_new_dentry.ino())
                                    : MetaDataCodec::EncodeFileInodeKey(fs_id_, pb_exist_new_dentry.ino());
    value.clear();
    status = txn->Get(new_inode_key, value);
    if (!status.ok() && status.error_code() != pb::error::ENOT_FOUND) {
      return Status(status.error_code(), fmt::format("get new inode fail, {}", status.error_str()));
    }

    bool is_exist_new_inode = !value.empty();
    if (is_exist_new_inode) {
      if (pb_exist_new_dentry.type() == pb::mdsv2::DIRECTORY) {
        pb::mdsv2::Inode pb_new_inode = MetaDataCodec::DecodeDirInodeValue(value);
        // check new dentry is empty
        if (pb_new_inode.nlink() > kEmptyDirMinLinkNum) {
          return Status(pb::error::ENOT_EMPTY,
                        fmt::format("new dentry({}/{}) is not empty.", new_parent_ino, new_name));
        }

        // delete exist new inode
        status = txn->Delete(new_inode_key);
        if (!status.ok()) {
          return Status(status.error_code(), fmt::format("delete new inode fail, {}", status.error_str()));
        }

      } else {
        // update exist new inode nlink
        pb::mdsv2::Inode pb_new_inode = MetaDataCodec::DecodeFileInodeValue(value);
        pb_new_inode.set_version(pb_new_inode.version() + 1);
        pb_new_inode.set_nlink(pb_new_inode.nlink() - 1);
        pb_new_inode.set_ctime(std::max(pb_new_inode.ctime(), now_ns));
        pb_new_inode.set_mtime(std::max(pb_new_inode.mtime(), now_ns));

        status = txn->Put(new_inode_key, MetaDataCodec::EncodeFileInodeValue(pb_new_inode));
        if (!status.ok()) {
          return Status(status.error_code(), fmt::format("put new inode fail, {}", status.error_str()));
        }
      }
    }
  }

  // delete old dentry
  status = txn->Delete(old_dentry_key);
  if (!status.ok()) {
    return Status(status.error_code(), fmt::format("delete old dentry fail, {}", status.error_str()));
  }

  // add new dentry
  pb::mdsv2::Dentry pb_new_dentry;
  pb_new_dentry.set_fs_id(fs_id_);
  pb_new_dentry.set_name(new_name);
  pb_new_dentry.set_ino(pb_old_dentry.ino());
  pb_new_dentry.set_type(pb_old_dentry.type());
  pb_new_dentry.set_parent_ino(new_parent_ino);

  status = txn->Put(new_dentry_key, MetaDataCodec::EncodeDentryValue(pb_new_dentry));
  if (!status.ok()) {
    return Status(status.error_code(), fmt::format("put new dentry fail, {}", status.error_str()));
  }

  if (is_same_parent) {
    // update parent inode ctime/mtime
    pb_old_parent_inode.set_version(pb_old_parent_inode.version() + 1);
    pb_old_parent_inode.set_ctime(now_ns);
    pb_old_parent_inode.set_mtime(now_ns);
    if (is_exist_new_dentry) {
      pb_old_parent_inode.set_nlink(pb_old_parent_inode.nlink() - 1);
    }

    status = txn->Put(old_parent_key, MetaDataCodec::EncodeDirInodeValue(pb_old_parent_inode));
    if (!status.ok()) {
      return Status(status.error_code(), fmt::format("put old parent inode fail, {}", status.error_str()));
    }

  } else {
    // update old parent inode nlink/ctime/mtime
    pb_old_parent_inode.set_version(pb_old_parent_inode.version() + 1);
    pb_old_parent_inode.set_ctime(now_ns);
    pb_old_parent_inode.set_mtime(now_ns);
    pb_old_parent_inode.set_nlink(pb_old_parent_inode.nlink() - 1);

    status = txn->Put(old_parent_key, MetaDataCodec::EncodeDirInodeValue(pb_old_parent_inode));
    if (!status.ok()) {
      return Status(status.error_code(), fmt::format("put old parent inode fail, {}", status.error_str()));
    }

    // update new parent inode nlink/ctime/mtime
    pb_new_parent_inode.set_version(pb_new_parent_inode.version() + 1);
    pb_new_parent_inode.set_ctime(now_ns);
    pb_new_parent_inode.set_mtime(now_ns);
    if (!is_exist_new_dentry) {
      pb_new_parent_inode.set_nlink(pb_new_parent_inode.nlink() + 1);
    }

    status = txn->Put(new_parent_key, MetaDataCodec::EncodeDirInodeValue(pb_new_parent_inode));
    if (!status.ok()) {
      return Status(status.error_code(), fmt::format("put new parent inode fail, {}", status.error_str()));
    }
  }

  status = txn->Commit();
  if (!status.ok()) {
    return Status(status.error_code(), fmt::format("commit fail, {}", status.error_str()));
  }

  old_parent_version = pb_old_parent_inode.version();
  new_parent_version = pb_new_parent_inode.version();

  DINGO_LOG(INFO) << fmt::format(
      "[fs.{}] rename {}/{} -> {}/{} finish, state({},{}) version({},{}) elapsed_time({}us) status({}).", fs_id_,
      old_parent_ino, old_name, new_parent_ino, new_name, is_same_parent, is_exist_new_dentry, old_parent_version,
      new_parent_version, (Helper::TimestampNs() - now_ns) / 1000, status.error_str());

  if (IsMonoPartition()) {
    // update cache
    PartitionPtr old_parent_partition;
    auto status = GetPartition(ctx, old_parent_ino, old_parent_partition);
    if (status.ok()) {
      // delete old dentry at cache
      old_parent_partition->DeleteChild(old_name);
      // update old parent inode nlink at cache
      old_parent_partition->ParentInode()->UpdateNlink(pb_old_parent_inode.version(), pb_old_parent_inode.nlink(),
                                                       now_ns);
    }

    // check new parent dentry/inode
    PartitionPtr new_parent_partition;
    status = GetPartition(ctx, new_parent_ino, new_parent_partition);
    if (status.ok()) {
      if (is_exist_new_dentry) {
        // delete new dentry at cache
        new_parent_partition->DeleteChild(new_name);
      } else {
        // update new parent inode nlink at cache
        new_parent_partition->ParentInode()->UpdateNlink(pb_new_parent_inode.version(), pb_new_parent_inode.nlink(),
                                                         now_ns);
      }

      // add new dentry at cache
      Dentry new_dentry(fs_id_, new_name, new_parent_ino, pb_old_dentry.ino(), pb_old_dentry.type(), 0,
                        GetInodeFromCache(pb_old_dentry.ino()));
      new_parent_partition->PutChild(new_dentry);
    }

    // delete exist new partition at cache
    // need notify mds to delete partition
    if (is_exist_new_dentry && pb_exist_new_dentry.type() == pb::mdsv2::FileType::DIRECTORY) {
      partition_cache_.Delete(pb_exist_new_dentry.ino());
    }
  } else {
    // notify mds(old_parent and new_parent) to update cache
    uint64_t old_mds_id = GetMdsIdByIno(old_parent_ino);
    uint64_t new_mds_id = GetMdsIdByIno(new_parent_ino);
    if (old_mds_id == new_mds_id) {
      SendRefreshInode(old_mds_id, fs_id_, {old_parent_ino, new_parent_ino});
    } else {
      SendRefreshInode(old_mds_id, fs_id_, {old_parent_ino});
      SendRefreshInode(new_mds_id, fs_id_, {new_parent_ino});
    }
  }

  return Status::OK();
}

Status FileSystem::RenameWithRetry(Context& ctx, uint64_t old_parent_ino, const std::string& old_name,
                                   uint64_t new_parent_ino, const std::string& new_name, uint64_t& old_parent_version,
                                   uint64_t& new_parent_version) {
  int retry = 0;
  do {
    auto status =
        Rename(ctx, old_parent_ino, old_name, new_parent_ino, new_name, old_parent_version, new_parent_version);
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      return status;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  return Status::OK();
}

Status FileSystem::CommitRename(Context& ctx, uint64_t old_parent_ino, const std::string& old_name,
                                uint64_t new_parent_ino, const std::string& new_name, uint64_t& old_parent_version,
                                uint64_t& new_parent_version) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  return renamer_->Execute(GetSelfPtr(), ctx, old_parent_ino, old_name, new_parent_ino, new_name, old_parent_version,
                           new_parent_version);
}

Status FileSystem::WriteSlice(Context& ctx, uint64_t ino, uint64_t chunk_index,
                              const pb::mdsv2::SliceList& slice_list) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] writeslice ino({}), chunk_index({}), slice_list.size({}).", fs_id_, ino,
                                  chunk_index, slice_list.slices_size());

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  InodePtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  uint64_t now_time = Helper::TimestampNs();

  bthread::CountdownEvent count_down(1);
  MixMutation mix_mutation = {.fs_id = fs_id_};

  Operation inode_operation(Operation::OpType::kUpdateInodeChunk, ino, MetaDataCodec::EncodeFileInodeKey(fs_id_, ino),
                            &count_down, &trace);
  inode_operation.SetUpdateInodeChunk(chunk_index, slice_list);
  mix_mutation.operations.push_back(&inode_operation);

  if (!mutation_processor_->Commit(mix_mutation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  butil::Status& rpc_status = inode_operation.status;
  auto& result = inode_operation.result;

  DINGO_LOG(INFO) << fmt::format("[fs.{}] writeslice {}/{} finish, elapsed_time({}us) rpc_status({}).", fs_id_, ino,
                                 chunk_index, (Helper::TimestampNs() - now_time) / 1000, rpc_status.error_str());

  if (!rpc_status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put fail, {}", rpc_status.error_str()));
  }

  // update cache
  inode->UpdateChunk(result.version, chunk_index, slice_list);

  return Status::OK();
}

Status FileSystem::ReadSlice(Context& ctx, uint64_t ino, uint64_t chunk_index, pb::mdsv2::SliceList& out_slice_list) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] readslice ino({}), chunk_index({}).", fs_id_, ino, chunk_index);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  InodePtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  out_slice_list = inode->GetChunk(chunk_index);

  return Status::OK();
}

Status FileSystem::RefreshInode(const std::vector<uint64_t>& inoes) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] refresh inode({}).", fs_id_, Helper::VectorToString(inoes));

  for (const auto& ino : inoes) {
    partition_cache_.Delete(ino);
    inode_cache_.DeleteInode(ino);
  }

  return Status::OK();
}

Status FileSystem::RefreshFsInfo() { return RefreshFsInfo(fs_info_->GetName()); }

Status FileSystem::RefreshFsInfo(const std::string& name) {
  DINGO_LOG(INFO) << fmt::format("[fs.{}] refresh fs({}) info.", fs_id_, name);

  std::string value;
  auto status = kv_storage_->Get(MetaDataCodec::EncodeFSKey(name), value);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("get fs info fail, {}", status.error_str()));
  }

  RefreshFsInfo(MetaDataCodec::DecodeFSValue(value));

  return Status::OK();
}

void FileSystem::RefreshFsInfo(const pb::mdsv2::FsInfo& fs_info) {
  fs_info_->Update(fs_info);

  can_serve_ = CanServe(self_mds_id_);
  DINGO_LOG(INFO) << fmt::format("[fs.{}] update fs({}) can_serve({}).", fs_id_, fs_info.fs_name(),
                                 can_serve_ ? "true" : "false");
}

Status FileSystem::UpdatePartitionPolicy(uint64_t mds_id) {
  std::string key = MetaDataCodec::EncodeFSKey(fs_info_->GetName());

  auto txn = kv_storage_->NewTxn();

  std::string value;
  auto status = txn->Get(key, value);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("get fs info fail, {}", status.error_str()));
  }

  pb::mdsv2::FsInfo fs_info = MetaDataCodec::DecodeFSValue(value);
  CHECK(fs_info.partition_policy().type() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION)
      << "invalid partition polocy type.";

  auto* mono = fs_info.mutable_partition_policy()->mutable_mono();
  mono->set_epoch(mono->epoch() + 1);
  mono->set_mds_id(mds_id);

  fs_info.set_last_update_time_ns(Helper::TimestampNs());

  status = txn->Put(key, MetaDataCodec::EncodeFSValue(fs_info));
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put store fs fail, {}", status.error_str()));
  }

  status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("commit fail, {}", status.error_str()));
  }

  fs_info_->Update(fs_info);

  can_serve_ = CanServe(self_mds_id_);
  DINGO_LOG(INFO) << fmt::format("[fs.{}] update fs({}) can_serve({}).", fs_id_, fs_info.fs_name(),
                                 can_serve_ ? "true" : "false");

  return Status::OK();
}

Status FileSystem::UpdatePartitionPolicy(const std::map<uint64_t, pb::mdsv2::HashPartition::BucketSet>& distributions) {
  std::string key = MetaDataCodec::EncodeFSKey(fs_info_->GetName());

  auto txn = kv_storage_->NewTxn();
  std::string value;
  auto status = txn->Get(key, value);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("get fs info fail, {}", status.error_str()));
  }

  pb::mdsv2::FsInfo fs_info = MetaDataCodec::DecodeFSValue(value);
  CHECK(fs_info.partition_policy().type() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION)
      << "invalid partition polocy type.";

  auto* hash = fs_info.mutable_partition_policy()->mutable_parent_hash();
  hash->set_epoch(hash->epoch() + 1);
  hash->mutable_distributions()->clear();
  for (const auto& [mds_id, bucket_set] : distributions) {
    hash->mutable_distributions()->insert({mds_id, bucket_set});
  }

  fs_info.set_last_update_time_ns(Helper::TimestampNs());

  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, key, MetaDataCodec::EncodeFSValue(fs_info));
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put store fs fail, {}", status.error_str()));
  }

  status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("commit fail, {}", status.error_str()));
  }

  fs_info_->Update(fs_info);

  return Status::OK();
}

FileSystemSet::FileSystemSet(CoordinatorClientPtr coordinator_client, IdGeneratorPtr id_generator,
                             KVStoragePtr kv_storage, MDSMeta self_mds_meta, MDSMetaMapPtr mds_meta_map,
                             RenamerPtr renamer, MutationProcessorPtr mutation_processor)
    : coordinator_client_(coordinator_client),
      id_generator_(std::move(id_generator)),
      kv_storage_(kv_storage),
      self_mds_meta_(self_mds_meta),
      mds_meta_map_(mds_meta_map),
      renamer_(renamer),
      mutation_processor_(mutation_processor) {}

FileSystemSet::~FileSystemSet() {}  // NOLINT

bool FileSystemSet::Init() {
  CHECK(coordinator_client_ != nullptr) << "coordinator client is null.";
  CHECK(kv_storage_ != nullptr) << "kv_storage is null.";
  CHECK(mds_meta_map_ != nullptr) << "mds_meta_map is null.";
  CHECK(renamer_ != nullptr) << "renamer is null.";
  CHECK(mutation_processor_ != nullptr) << "mutation_processor is null.";

  if (!IsExistFsTable()) {
    DINGO_LOG(ERROR) << "[fsset] not exist fs table.";
    return false;
  }

  if (!LoadFileSystems()) {
    DINGO_LOG(ERROR) << "[fsset] load already exist file systems fail.";
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

  fs_info.set_create_time_s(Helper::Timestamp());
  fs_info.set_last_update_time_ns(Helper::TimestampNs());

  return fs_info;
}

Status FileSystemSet::CreateFsTable() {
  int64_t table_id = 0;
  KVStorage::TableOption option;
  MetaDataCodec::GetFsTableRange(option.start_key, option.end_key);
  DINGO_LOG(INFO) << fmt::format("[fsset] create fs table, start_key({}), end_key({}).",
                                 Helper::StringToHex(option.start_key), Helper::StringToHex(option.end_key));
  return kv_storage_->CreateTable(kFsTableName, option, table_id);
}

bool FileSystemSet::IsExistFsTable() {
  std::string start_key, end_key;
  MetaDataCodec::GetFsTableRange(start_key, end_key);
  DINGO_LOG(DEBUG) << fmt::format("[fsset] check fs table, start_key({}), end_key({}).", Helper::StringToHex(start_key),
                                  Helper::StringToHex(end_key));

  auto status = kv_storage_->IsExistTable(start_key, end_key);
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      DINGO_LOG(ERROR) << "[fsset] check fs table exist fail, error: " << status.error_str();
    }
    return false;
  }

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
        LOG(ERROR) << fmt::format("[fsset] clean dentry table({}) fail, error: {}", dentry_table_id,
                                  status.error_str());
      }
    }

    // clean file inode table
    if (file_inode_table_id > 0) {
      auto status = kv_storage_->DropTable(file_inode_table_id);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("[fsset] clean file inode table({}) fail, error: {}", file_inode_table_id,
                                  status.error_str());
      }
    }

    // clean fs info
    if (!fs_key.empty()) {
      auto status = kv_storage_->Delete(fs_key);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("[fsset] clean fs info fail, error: {}", status.error_str());
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

  auto fs = FileSystem::New(self_mds_meta_.ID(), FsInfo::NewUnique(fs_info), std::move(id_generator), kv_storage_,
                            renamer_, mutation_processor_);
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

  std::string key = MetaDataCodec::EncodeFSKey(fs_name);

  auto txn = kv_storage_->NewTxn();

  std::string value;
  Status status = txn->Get(key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}), {}.", fs_name, status.error_str()));
  }

  auto fs_info = MetaDataCodec::DecodeFSValue(value);

  if (IsExistMountPoint(fs_info, mount_point)) {
    return Status(pb::error::EEXISTED, "mountPoint already exist.");
  }

  fs_info.add_mount_points()->CopyFrom(mount_point);
  fs_info.set_last_update_time_ns(Helper::TimestampNs());

  status = txn->Put(key, MetaDataCodec::EncodeFSValue(fs_info));
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put fs fail, {}", status.error_str()));
  }

  status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("commit fail, {}", status.error_str()));
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

  auto txn = kv_storage_->NewTxn();

  std::string value;
  Status status = txn->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}), {}.", fs_name, status.error_str()));
  }

  auto fs_info = MetaDataCodec::DecodeFSValue(value);

  RemoveMountPoint(fs_info, mount_point);

  fs_info.set_last_update_time_ns(Helper::TimestampNs());

  status = txn->Put(fs_key, MetaDataCodec::EncodeFSValue(fs_info));
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put store fs fail, {}", status.error_str()));
  }

  status = txn->Commit();
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("commit fail, {}", status.error_str()));
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

Status FileSystemSet::GetAllFsInfo(std::vector<pb::mdsv2::FsInfo>& fs_infoes) {
  Range range;
  MetaDataCodec::GetFsTableRange(range.start_key, range.end_key);

  // scan fs table from kv storage
  std::vector<KeyValue> kvs;
  auto status = kv_storage_->Scan(range, kvs);
  if (!status.ok()) {
    return status;
  }

  for (const auto& kv : kvs) {
    auto fs_info = MetaDataCodec::DecodeFSValue(kv.value);
    fs_infoes.push_back(std::move(fs_info));
  }

  return Status::OK();
}

Status FileSystemSet::RefreshFsInfo(const std::string& fs_name) {
  auto fs = GetFileSystem(fs_name);
  if (fs == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}).", fs_name));
  }

  return fs->RefreshFsInfo();
}

Status FileSystemSet::RefreshFsInfo(uint32_t fs_id) {
  auto fs = GetFileSystem(fs_id);
  if (fs == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}).", fs_id));
  }

  return fs->RefreshFsInfo();
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

FileSystemPtr FileSystemSet::GetFileSystem(const std::string& fs_name) {
  utils::ReadLockGuard lk(lock_);

  for (auto& [fs_id, fs] : fs_map_) {
    if (fs->FsName() == fs_name) {
      return fs;
    }
  }

  return nullptr;
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
    DINGO_LOG(ERROR) << fmt::format("[fsset] scan fs table fail, error: {}.", status.error_str());
    return false;
  }

  for (const auto& kv : kvs) {
    auto id_generator = AutoIncrementIdGenerator::New(coordinator_client_, kInoTableId, kInoStartId, kInoBatchSize);
    CHECK(id_generator != nullptr) << "new id generator fail.";

    auto fs_info = MetaDataCodec::DecodeFSValue(kv.value);
    auto file_system = GetFileSystem(fs_info.fs_id());
    if (file_system == nullptr) {
      DINGO_LOG(INFO) << fmt::format("[fsset] add fs name({}) id({}).", fs_info.fs_name(), fs_info.fs_id());
      auto fs = FileSystem::New(self_mds_meta_.ID(), FsInfo::NewUnique(fs_info), std::move(id_generator), kv_storage_,
                                renamer_, mutation_processor_);
      AddFileSystem(fs);

    } else {
      file_system->RefreshFsInfo(fs_info);
    }
  }

  return true;
}

}  // namespace mdsv2
}  // namespace dingofs
