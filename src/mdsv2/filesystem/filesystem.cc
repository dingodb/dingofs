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
#include "mdsv2/common/codec.h"
#include "mdsv2/common/constant.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/file_session.h"
#include "mdsv2/filesystem/fs_info.h"
#include "mdsv2/filesystem/id_generator.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/filesystem/mutation_processor.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/server.h"
#include "mdsv2/service/service_access.h"
#include "mdsv2/storage/storage.h"
#include "utils/uuid.h"

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

DEFINE_uint32(compact_slice_threshold_num, 16, "Compact slice threshold num.");

DECLARE_uint32(txn_max_retry_times);

DECLARE_int32(fs_scan_batch_size);

bool IsReserveNode(uint64_t ino) { return ino == kRootIno; }

bool IsReserveName(const std::string& name) { return name == kStatsName || name == kRecyleName; }

bool IsInvalidName(const std::string& name) { return name.empty() || name.size() > FLAGS_filesystem_name_max_size; }

static inline bool IsDir(uint64_t ino) { return (ino & 1) == 1; }

static inline bool IsFile(uint64_t ino) { return (ino & 1) == 0; }

FileSystem::FileSystem(int64_t self_mds_id, FsInfoUPtr fs_info, IdGeneratorPtr id_generator, KVStorageSPtr kv_storage,
                       RenamerPtr renamer, MutationProcessorSPtr mutation_processor, MDSMetaMapSPtr mds_meta_map)
    : self_mds_id_(self_mds_id),
      fs_info_(std::move(fs_info)),
      fs_id_(fs_info_->GetFsId()),
      id_generator_(std::move(id_generator)),
      kv_storage_(kv_storage),
      renamer_(renamer),
      mutation_processor_(mutation_processor),
      mds_meta_map_(mds_meta_map) {
  can_serve_ = CanServe(self_mds_id);

  file_session_manager_ = FileSessionManager::New(fs_id_, kv_storage_);
  chunk_cache_ = ChunkCache::New();
};

FileSystemSPtr FileSystem::GetSelfPtr() { return std::dynamic_pointer_cast<FileSystem>(shared_from_this()); }

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

std::map<uint64_t, PartitionPtr> FileSystem::GetAllPartitionsFromCache() { return partition_cache_.GetAll(); }

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
  auto parent_inode = Inode::New(MetaDataCodec::DecodeInodeValue(parent_kv.value));
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

Status FileSystem::GetDentryFromStore(uint64_t parent, const std::string& name, Dentry& dentry) {
  std::string value;
  auto status = kv_storage_->Get(MetaDataCodec::EncodeDentryKey(fs_id_, parent, name), value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found dentry({}/{}), {}", parent, name, status.error_str()));
  }

  DINGO_LOG(INFO) << fmt::format("[fs.{}] fetch dentry({}/{}).", fs_id_, parent, name);

  dentry = Dentry(MetaDataCodec::DecodeDentryValue(value));

  return Status::OK();
}

Status FileSystem::ListDentryFromStore(uint64_t parent, const std::string& last_name, uint32_t limit, bool is_only_dir,
                                       std::vector<Dentry>& dentries) {
  // scan dentry from store
  Range range;
  MetaDataCodec::EncodeDentryRange(fs_id_, parent, range.start_key, range.end_key);
  if (!last_name.empty()) {
    range.start_key = MetaDataCodec::EncodeDentryKey(fs_id_, parent, last_name);
  }

  auto txn = kv_storage_->NewTxn();

  std::vector<KeyValue> kvs;
  do {
    kvs.clear();
    auto status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      return status;
    }

    for (auto& kv : kvs) {
      auto dentry = MetaDataCodec::DecodeDentryValue(kv.value);
      if (is_only_dir && dentry.type() != pb::mdsv2::FileType::DIRECTORY) {
        continue;
      }

      dentries.push_back(Dentry(dentry));
      if (dentries.size() >= limit) {
        return Status::OK();
      }
    }

  } while (kvs.size() >= FLAGS_fs_scan_batch_size);

  return txn->Commit();
}

Status FileSystem::GetInode(Context& ctx, Dentry& dentry, PartitionPtr partition, InodeSPtr& out_inode) {
  return GetInode(ctx, ctx.GetInodeVersion(), dentry, partition, out_inode);
}

Status FileSystem::GetInode(Context& ctx, uint64_t version, Dentry& dentry, PartitionPtr partition,
                            InodeSPtr& out_inode) {
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

Status FileSystem::GetInode(Context& ctx, uint64_t ino, InodeSPtr& out_inode) {
  return GetInode(ctx, ctx.GetInodeVersion(), ino, out_inode);
}

Status FileSystem::GetInode(Context& ctx, uint64_t version, uint64_t ino, InodeSPtr& out_inode) {
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

InodeSPtr FileSystem::GetInodeFromCache(uint64_t ino) { return inode_cache_.GetInode(ino); }

std::map<uint64_t, InodeSPtr> FileSystem::GetAllInodesFromCache() { return inode_cache_.GetAllInodes(); }

Status FileSystem::GetInodeFromStore(uint64_t ino, const std::string& reason, InodeSPtr& out_inode) {
  std::string key = MetaDataCodec::EncodeInodeKey(fs_id_, ino);
  std::string value;
  auto status = kv_storage_->Get(key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found inode({}), {}", ino, status.error_str()));
  }

  out_inode = Inode::New(MetaDataCodec::DecodeInodeValue(value));

  inode_cache_.PutInode(ino, out_inode);

  DINGO_LOG(INFO) << fmt::format("[fs.{}] fetch inode({}), reason({}).", fs_id_, ino, reason);

  return Status::OK();
}

Status FileSystem::BatchGetInodeFromStore(std::vector<uint64_t> inoes, std::vector<InodeSPtr>& out_inodes) {
  std::vector<std::string> keys;
  keys.reserve(inoes.size());
  for (auto ino : inoes) {
    keys.push_back(MetaDataCodec::EncodeInodeKey(fs_id_, ino));
  }

  std::vector<KeyValue> kvs;
  auto status = kv_storage_->BatchGet(keys, kvs);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND,
                  fmt::format("not found inode({}), {}", Helper::VectorToString(inoes), status.error_str()));
  }

  for (const auto& kv : kvs) {
    uint32_t fs_id = 0;
    uint64_t ino = 0;
    MetaDataCodec::DecodeInodeKey(kv.key, fs_id, ino);
    out_inodes.push_back(Inode::New(MetaDataCodec::DecodeInodeValue(kv.value)));
  }

  return Status::OK();
}

Status FileSystem::DestoryInode(uint32_t fs_id, uint64_t ino) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] destory inode {}.", fs_id_, ino);

  std::string inode_key = MetaDataCodec::EncodeInodeKey(fs_id, ino);
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

  std::string inode_key = MetaDataCodec::EncodeInodeKey(fs_id_, inode->Ino());
  std::string inode_value = MetaDataCodec::EncodeInodeValue(inode->CopyTo());
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

  InodeSPtr inode;
  status = GetInode(ctx, 0, dentry, partition, inode);
  if (!status.ok()) {
    return status;
  }

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

Status FileSystem::CleanUpInode(InodeSPtr inode) {
  uint32_t fs_id = inode->FsId();
  uint64_t ino = inode->Ino();

  bthread::CountdownEvent count_down(1);
  MixMutation mix_mutation = {.fs_id = fs_id};

  Trace trace;
  Operation inode_operation(Operation::OpType::kDeleteInode, ino, MetaDataCodec::EncodeInodeKey(fs_id, inode->Ino()),
                            &count_down, &trace);
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
  Operation inode_operation(Operation::OpType::kUpdateInodeNlink, ino, MetaDataCodec::EncodeInodeKey(fs_id, ino),
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

  Operation inode_operation(Operation::OpType::kCreateInode, ino, MetaDataCodec::EncodeInodeKey(fs_id_, inode->Ino()),
                            &count_down, &trace);
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

Status FileSystem::Open(Context& ctx, uint64_t ino, std::string& session_id) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();
  const std::string& client_id = ctx.ClientId();

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  // O_TRUNC && (O_WRONLY || O_RDWR)
  // truncate file
  // set file length to 0 and update inode mtime/ctime

  FileSessionPtr file_session;
  status = file_session_manager_->Create(ino, client_id, file_session);
  if (!status.ok()) {
    return status;
  }

  session_id = file_session->SessionId();

  return Status::OK();
}

Status FileSystem::Release(Context& ctx, uint64_t ino, const std::string& session_id) {  // NOLINT
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto status = file_session_manager_->Delete(ino, session_id);
  if (!status.ok()) {
    return status;
  }

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

  Operation inode_operation(Operation::OpType::kCreateInode, ino, MetaDataCodec::EncodeInodeKey(fs_id_, inode->Ino()),
                            &count_down, &trace);
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
    InodeSPtr inode = partition->ParentInode();
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

    std::string key = MetaDataCodec::EncodeInodeKey(fs_id_, dentry.Ino());
    std::string value;
    status = txn->Get(key, value);
    if (!status.ok()) {
      status = Status(pb::error::EBACKEND_STORE, fmt::format("get inode fail, {}", status.error_str()));
      break;
    }

    // check dir is empty by nlink
    pb::mdsv2::Inode pb_inode = MetaDataCodec::DecodeInodeValue(value);
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
    std::string parent_key = MetaDataCodec::EncodeInodeKey(fs_id_, parent_ino);
    std::string parent_value;
    status = txn->Get(parent_key, parent_value);
    if (!status.ok()) {
      status = Status(pb::error::EBACKEND_STORE, fmt::format("get parent inode fail, {}", status.error_str()));
      break;
    }

    pb_parent_inode = MetaDataCodec::DecodeInodeValue(parent_value);
    pb_parent_inode.set_version(pb_parent_inode.version() + 1);
    pb_parent_inode.set_nlink(pb_parent_inode.nlink() - 1);
    pb_parent_inode.set_ctime(now_ns);
    pb_parent_inode.set_mtime(now_ns);

    status = txn->Put(parent_key, MetaDataCodec::EncodeInodeValue(pb_parent_inode));
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
      InodeSPtr inode;
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
  InodeSPtr inode;
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
                            MetaDataCodec::EncodeInodeKey(fs_id, inode->Ino()), &count_down, &trace);
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

  InodeSPtr inode;
  status = GetInode(ctx, dentry, partition, inode);
  if (!status.ok()) {
    return status;
  }

  if (inode->Type() == pb::mdsv2::FileType::DIRECTORY) {
    return Status(pb::error::ENOT_FILE, "directory not allow unlink");
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
  pb::mdsv2::Inode pb_inode;
  int retry = 0;
  do {
    uint64_t now_time = Helper::TimestampNs();
    auto txn = kv_storage_->NewTxn();
    std::string key = MetaDataCodec::EncodeInodeKey(fs_id_, dentry.Ino());
    std::string value;
    status = txn->Get(key, value);
    if (!status.ok()) {
      return Status(pb::error::EBACKEND_STORE, fmt::format("get inode fail, {}", status.error_str()));
    }

    pb_inode = MetaDataCodec::DecodeInodeValue(value);
    pb_inode.set_ctime(now_time);
    pb_inode.set_mtime(now_time);
    pb_inode.set_version(pb_inode.version() + 1);
    pb_inode.set_nlink(pb_inode.nlink() - 1);

    if (pb_inode.nlink() == 0) {
      // delete inode
      txn->Delete(key);
      // save delete file info
      txn->Put(MetaDataCodec::EncodeDelFileKey(fs_id_, dentry.Ino()), MetaDataCodec::EncodeDelFileValue(pb_inode));
    } else {
      txn->Put(key, MetaDataCodec::EncodeInodeValue(pb_inode));
    }

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

  } while (++retry < FLAGS_txn_max_retry_times);

  if (status.ok()) {
    inode_cache_.DeleteInode(dentry.Ino());
    inode->UpdateNlink(pb_inode.version(), pb_inode.nlink(), pb_inode.ctime());
  }

  DINGO_LOG(INFO) << fmt::format("[fs.{}] unlink {}/{} finish, elapsed_time({}us).", fs_id_, parent_ino, name,
                                 (Helper::TimestampNs() - now_time) / 1000);

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

  Operation inode_operation(Operation::OpType::kCreateInode, ino, MetaDataCodec::EncodeInodeKey(fs_id_, inode->Ino()),
                            &count_down, &trace);
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

  InodeSPtr inode;
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

  InodeSPtr inode;
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

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  // update backend store
  bthread::CountdownEvent count_down(1);
  MixMutation mix_mutation = {.fs_id = fs_id_};

  std::string key = MetaDataCodec::EncodeInodeKey(fs_id_, inode->Ino());
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

  InodeSPtr inode;
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

  InodeSPtr inode;
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

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  uint64_t now_time = Helper::TimestampNs();

  // update backend store
  bthread::CountdownEvent count_down(1);
  MixMutation mix_mutation = {.fs_id = fs_id_};

  std::string key = MetaDataCodec::EncodeInodeKey(fs_id_, inode->Ino());
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

void FileSystem::SendRefreshInode(uint64_t mds_id, uint32_t fs_id, const std::vector<uint64_t>& inoes) {
  MDSMeta mds_meta;
  if (!mds_meta_map_->GetMDSMeta(mds_id, mds_meta)) {
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
  std::string old_parent_key = MetaDataCodec::EncodeInodeKey(fs_id_, old_parent_ino);
  auto status = txn->Get(old_parent_key, value);
  if (!status.ok()) {
    return Status(status.error_code(),
                  fmt::format("not found old parent inode({}), {}", old_parent_ino, status.error_str()));
  }

  pb::mdsv2::Inode pb_old_parent_inode = MetaDataCodec::DecodeInodeValue(value);

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
  std::string new_parent_key = MetaDataCodec::EncodeInodeKey(fs_id_, new_parent_ino);
  if (!is_same_parent) {
    value.clear();
    status = txn->Get(new_parent_key, value);
    if (!status.ok()) {
      return Status(status.error_code(),
                    fmt::format("not found new parent inode({}), {}", new_parent_ino, status.error_str()));
    }
    pb_new_parent_inode = MetaDataCodec::DecodeInodeValue(value);

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
    std::string new_inode_key = MetaDataCodec::EncodeInodeKey(fs_id_, pb_exist_new_dentry.ino());
    value.clear();
    status = txn->Get(new_inode_key, value);
    if (!status.ok() && status.error_code() != pb::error::ENOT_FOUND) {
      return Status(status.error_code(), fmt::format("get new inode fail, {}", status.error_str()));
    }

    bool is_exist_new_inode = !value.empty();
    if (is_exist_new_inode) {
      if (pb_exist_new_dentry.type() == pb::mdsv2::DIRECTORY) {
        pb::mdsv2::Inode pb_new_inode = MetaDataCodec::DecodeInodeValue(value);
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
        pb::mdsv2::Inode pb_new_inode = MetaDataCodec::DecodeInodeValue(value);
        pb_new_inode.set_version(pb_new_inode.version() + 1);
        pb_new_inode.set_nlink(pb_new_inode.nlink() - 1);
        pb_new_inode.set_ctime(std::max(pb_new_inode.ctime(), now_ns));
        pb_new_inode.set_mtime(std::max(pb_new_inode.mtime(), now_ns));

        status = txn->Put(new_inode_key, MetaDataCodec::EncodeInodeValue(pb_new_inode));
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

    status = txn->Put(old_parent_key, MetaDataCodec::EncodeInodeValue(pb_old_parent_inode));
    if (!status.ok()) {
      return Status(status.error_code(), fmt::format("put old parent inode fail, {}", status.error_str()));
    }

  } else {
    // update old parent inode nlink/ctime/mtime
    pb_old_parent_inode.set_version(pb_old_parent_inode.version() + 1);
    pb_old_parent_inode.set_ctime(now_ns);
    pb_old_parent_inode.set_mtime(now_ns);
    pb_old_parent_inode.set_nlink(pb_old_parent_inode.nlink() - 1);

    status = txn->Put(old_parent_key, MetaDataCodec::EncodeInodeValue(pb_old_parent_inode));
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

    status = txn->Put(new_parent_key, MetaDataCodec::EncodeInodeValue(pb_new_parent_inode));
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
  Status status;
  int retry = 0;
  do {
    status = Rename(ctx, old_parent_ino, old_name, new_parent_ino, new_name, old_parent_version, new_parent_version);
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      return status;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  return status;
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

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  auto& trace = ctx.GetTrace();
  auto& trace_txn = trace.GetTxn();

  uint64_t now_ns = Helper::TimestampNs();
  std::string key = MetaDataCodec::EncodeChunkKey(fs_id_, ino, chunk_index);
  std::string value;
  pb::mdsv2::Chunk chunk;
  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();
    status = txn->Get(key, value);
    if (!status.ok()) {
      if (status.error_code() != pb::error::ENOT_FOUND) {
        return status;
      }
      // create new chunk
      chunk.set_index(chunk_index);
      chunk.set_size(fs_info_->GetChunkSize());

    } else {
      chunk = MetaDataCodec::DecodeChunkValue(value);
    }

    // append new slices
    chunk.mutable_slices()->MergeFrom(slice_list.slices());

    // compact chunk
    if (chunk.slices_size() > FLAGS_compact_slice_threshold_num) {
      DoCompactChunk(inode->Ino(), inode->Length(), chunk, txn.get());
    }

    chunk.set_version(chunk.version() + 1);
    txn->Put(key, MetaDataCodec::EncodeChunkValue(chunk));
    status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (!status.ok()) {
      return status;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.retry = retry;

  if (status.ok()) {
    // update chunk cache
    chunk_cache_->PutIf(ino, chunk_index, chunk);
  }

  DINGO_LOG(INFO) << fmt::format("[fs.{}] writeslice {}/{} finish, elapsed_time({}us) status({}).", fs_id_, ino,
                                 chunk_index, (Helper::TimestampNs() - now_ns) / 1000, status.error_str());

  return Status::OK();
}

Status FileSystem::ReadSlice(Context& ctx, uint64_t ino, uint64_t chunk_index, pb::mdsv2::SliceList& slice_list) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] readslice ino({}), chunk_index({}).", fs_id_, ino, chunk_index);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();
  auto& trace_txn = trace.GetTxn();

  uint64_t now_ns = Helper::TimestampNs();

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  // try to get from cache
  auto chunk = chunk_cache_->Get(ino, chunk_index);
  if (chunk != nullptr) {
    *slice_list.mutable_slices() = chunk->slices();
    return Status::OK();
  }

  // take from store
  auto txn = kv_storage_->NewTxn();
  std::string value;
  status = txn->Get(MetaDataCodec::EncodeChunkKey(fs_id_, ino, chunk_index), value);
  if (status.ok()) {
    pb::mdsv2::Chunk chunk = MetaDataCodec::DecodeChunkValue(value);
    *slice_list.mutable_slices() = chunk.slices();
  }

  trace_txn = txn->GetTrace();

  DINGO_LOG(INFO) << fmt::format("[fs.{}] readslice {}/{} finish, elapsed_time({}us) status({}).", fs_id_, ino,
                                 chunk_index, (Helper::TimestampNs() - now_ns) / 1000, status.error_str());

  return status;
}

std::vector<pb::mdsv2::TrashSlice> FileSystem::GenTrashSlices(uint64_t ino, uint64_t file_length,
                                                              const pb::mdsv2::Chunk& chunk) const {
  struct OffsetRange {
    uint64_t start;
    uint64_t end;
    std::vector<pb::mdsv2::Slice> slices;
  };

  struct Block {
    uint64_t slice_id;
    uint64_t offset;
    uint64_t size;
  };

  const auto& fs_info = FsInfo();

  const uint32_t fs_id = fs_info.fs_id();
  const uint64_t chunk_size = fs_info.chunk_size();
  const uint64_t block_size = fs_info.block_size();
  const uint64_t chunk_offset = chunk.index() * chunk_size;

  auto chunk_copy = chunk;

  std::vector<pb::mdsv2::TrashSlice> trash_slices;

  // 1. complete out of file length slices
  // chunk offset:               |______|
  // file length: |____________|
  // 2. partial out of file length slices
  // chunk offset:          |______|
  // file length: |____________|
  if (chunk_offset + chunk_size > file_length) {
    for (const auto& slice : chunk.slices()) {
      if (slice.offset() >= file_length) {
        pb::mdsv2::TrashSlice trash_slice;
        trash_slice.set_fs_id(fs_id);
        trash_slice.set_ino(ino);
        trash_slice.set_chunk_index(chunk.index());
        trash_slice.set_slice_id(slice.id());
        trash_slice.set_is_partial(false);
        auto* range = trash_slice.add_ranges();
        range->set_offset(slice.offset());
        range->set_len(slice.len());

        trash_slices.push_back(std::move(trash_slice));
      }
    }

    return trash_slices;
  }

  // 3. complete overlapped slices
  //     |______4________|      slices
  // |__1__|___2___|____3_____| slices
  // |________________________| chunk
  // slice-2 is complete overlapped by slice-4
  // sort by offset
  std::sort(chunk_copy.mutable_slices()->begin(), chunk_copy.mutable_slices()->end(),
            [](const pb::mdsv2::Slice& a, const pb::mdsv2::Slice& b) { return a.offset() < b.offset(); });

  // get offset ranges
  std::vector<uint64_t> offsets;
  for (const auto& slice : chunk_copy.slices()) {
    offsets.push_back(slice.offset());
    offsets.push_back(slice.offset() + slice.len());
  }

  std::sort(offsets.begin(), offsets.end());

  std::vector<OffsetRange> offset_ranges;
  for (size_t i = 0; i < offsets.size() - 1; ++i) {
    offset_ranges.push_back({.start = offsets[i], .end = offsets[i + 1]});
  }

  for (auto& offset_range : offset_ranges) {
    for (const auto& slice : chunk_copy.slices()) {
      uint64_t slice_start = slice.offset();
      uint64_t slice_end = slice.offset() + slice.len();
      if ((slice_start >= offset_range.start && slice_start < offset_range.end) ||
          (slice_end >= offset_range.start && slice_end < offset_range.end)) {
        offset_range.slices.push_back(slice);
      }
    }
  }

  // get reserve slice ids
  std::set<uint64_t> reserve_slice_ids;
  for (auto& offset_range : offset_ranges) {
    // sort by id, from newest to oldest
    std::sort(offset_range.slices.begin(), offset_range.slices.end(),
              [](const pb::mdsv2::Slice& a, const pb::mdsv2::Slice& b) { return a.id() > b.id(); });
    reserve_slice_ids.insert(offset_range.slices.front().id());
  }

  // get delete slices
  for (const auto& slice : chunk_copy.slices()) {
    if (reserve_slice_ids.count(slice.id()) == 0) {
      pb::mdsv2::TrashSlice trash_slice;
      trash_slice.set_fs_id(fs_id);
      trash_slice.set_ino(ino);
      trash_slice.set_chunk_index(chunk.index());
      trash_slice.set_slice_id(slice.id());
      trash_slice.set_is_partial(false);
      auto* range = trash_slice.add_ranges();
      range->set_offset(slice.offset());
      range->set_len(slice.len());

      trash_slices.push_back(std::move(trash_slice));
    }
  }

  // 4. partial overlapped slices
  //     |______4________|      slices
  // |__1__|___2___|____3_____| slices
  // |________________________| chunk
  // slice-2 and slice-3 are partial overlapped by slice-4
  // or
  //     |______4________|      slices
  // |___________1____________| slices
  // |________________________| chunk
  // slice-1 is partial overlapped by slice-4
  // or
  //     |___2___|   |___3___|   slices
  // |___________1____________| slices
  // |________________________| chunk
  // slice-1 is partial overlapped by slice-2 and slice-3
  std::map<uint64_t, pb::mdsv2::TrashSlice> temp_trash_slice_map;
  for (auto& offset_range : offset_ranges) {
    auto& slices = offset_range.slices;
    auto reserve_slice = slices.front();
    for (int i = 1; i < slices.size(); ++i) {
      auto& slice = slices[i];
      if (reserve_slice_ids.count(slice.id()) == 0) {
        continue;
      }

      auto start_offset = std::max(offset_range.start, slice.offset());
      auto end_offset = std::min(offset_range.end, slice.offset() + slice.len());

      for (uint64_t block_offset = slice.offset(); block_offset < end_offset; block_offset += block_size) {
        if (block_offset >= start_offset && block_offset + block_size < end_offset) {
          auto it = temp_trash_slice_map.find(slice.id());
          if (it == temp_trash_slice_map.end()) {
            pb::mdsv2::TrashSlice trash_slice;
            trash_slice.set_fs_id(fs_id);
            trash_slice.set_ino(ino);
            trash_slice.set_chunk_index(chunk.index());
            trash_slice.set_slice_id(slice.id());
            trash_slice.set_is_partial(true);
            auto* range = trash_slice.add_ranges();
            range->set_offset(slice.offset());
            range->set_len(slice.len());

            temp_trash_slice_map[slice.id()] = trash_slice;
          } else {
            auto* range = it->second.add_ranges();
            range->set_offset(block_offset);
            range->set_len(block_size);
          }
        }
      }
    }
  }

  for (auto& [_, trash_slice] : temp_trash_slice_map) {
    trash_slices.push_back(std::move(trash_slice));
  }

  return trash_slices;
}

static void UpdateChunk(pb::mdsv2::Chunk& chunk, const std::vector<pb::mdsv2::TrashSlice>& trash_slices) {
  auto gen_slice_map_fn = [](const std::vector<pb::mdsv2::TrashSlice>& trash_slices) {
    std::map<uint64_t, pb::mdsv2::TrashSlice> slice_map;
    for (const auto& slice : trash_slices) {
      slice_map[slice.slice_id()] = slice;
    }
    return slice_map;
  };

  auto trash_slice_map = gen_slice_map_fn(trash_slices);
  for (auto slice_it = chunk.mutable_slices()->begin(); slice_it != chunk.mutable_slices()->end();) {
    auto it = trash_slice_map.find(slice_it->id());
    if (it != trash_slice_map.end() && !it->second.is_partial()) {
      slice_it = chunk.mutable_slices()->erase(slice_it);
    } else {
      ++slice_it;
    }
  }
}

std::vector<pb::mdsv2::TrashSlice> FileSystem::DoCompactChunk(uint64_t ino, uint64_t file_length,
                                                              pb::mdsv2::Chunk& chunk) {
  auto trash_slices = GenTrashSlices(ino, file_length, chunk);

  UpdateChunk(chunk, trash_slices);

  return std::move(trash_slices);
}

std::vector<pb::mdsv2::TrashSlice> FileSystem::DoCompactChunk(uint64_t ino, uint64_t file_length,
                                                              pb::mdsv2::Chunk& chunk, Txn* txn) {
  auto trash_slices = DoCompactChunk(ino, file_length, chunk);

  pb::mdsv2::TrashSliceList trash_slice_list;
  for (auto& slice : trash_slices) {
    trash_slice_list.add_slices()->Swap(&slice);
  }

  uint64_t now_ns = Helper::TimestampNs();
  txn->Put(MetaDataCodec::EncodeTrashChunkKey(fs_id_, ino, chunk.index(), now_ns),
           MetaDataCodec::EncodeTrashChunkValue(trash_slice_list));

  return std::move(trash_slices);
}

Status FileSystem::CompactChunk(Context& ctx, uint64_t ino, uint64_t chunk_index,
                                std::vector<pb::mdsv2::TrashSlice>& trash_slices) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] compactchunk ino({}), chunk_index({}).", fs_id_, ino, chunk_index);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  auto trace = ctx.GetTrace();
  auto& trace_txn = trace.GetTxn();

  uint64_t now_ns = Helper::TimestampNs();

  std::string key = MetaDataCodec::EncodeChunkKey(fs_id_, ino, chunk_index);

  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

    std::string value;
    status = txn->Get(key, value);
    if (!status.ok()) {
      return status;
    }

    auto chunk = MetaDataCodec::DecodeChunkValue(value);

    trash_slices = DoCompactChunk(ino, inode->Length(), chunk, txn.get());

    chunk.set_version(chunk.version() + 1);
    txn->Put(key, MetaDataCodec::EncodeChunkValue(chunk));

    status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.retry = retry;

  DINGO_LOG(INFO) << fmt::format("[fs.{}] compactchunk {}/{} finish, elapsed_time({}us) status({}).", fs_id_, ino,
                                 chunk_index, (Helper::TimestampNs() - now_ns) / 1000, status.error_str());

  return status;
}

Status FileSystem::CompactFile(Context& ctx, uint64_t ino, std::vector<pb::mdsv2::TrashSlice>& trash_slices) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] compactfile ino({}).", fs_id_, ino);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  auto trace = ctx.GetTrace();
  auto& trace_txn = trace.GetTxn();

  uint64_t now_ns = Helper::TimestampNs();

  Range range;
  MetaDataCodec::GetChunkRange(fs_id_, ino, range.start_key, range.end_key);

  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

    std::vector<KeyValue> kvs;
    status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      return status;
    }

    CHECK(kvs.size() <= FLAGS_fs_scan_batch_size) << fmt::format("file({}) has too many chunks.", ino);

    for (const auto& kv : kvs) {
      auto chunk = MetaDataCodec::DecodeChunkValue(kv.value);

      trash_slices = DoCompactChunk(ino, inode->Length(), chunk, txn.get());

      chunk.set_version(chunk.version() + 1);
      txn->Put(MetaDataCodec::EncodeChunkKey(fs_id_, ino, chunk.index()), MetaDataCodec::EncodeChunkValue(chunk));
    }

    status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.retry = retry;

  DINGO_LOG(INFO) << fmt::format("[fs.{}] compactfile {} finish, elapsed_time({}us) status({}).", fs_id_, ino,
                                 (Helper::TimestampNs() - now_ns) / 1000, status.error_str());

  return status;
}

Status FileSystem::CompactAll(Context& ctx, uint64_t& checked_count, uint64_t& compacted_count) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] compactall.", fs_id_);

  checked_count = 0;
  compacted_count = 0;

  uint64_t now_ns = Helper::TimestampNs();

  Range range;
  MetaDataCodec::GetFileInodeTableRange(fs_id_, range.start_key, range.end_key);

  auto txn = kv_storage_->NewTxn();

  Status status;
  std::vector<KeyValue> kvs;
  do {
    kvs.clear();
    status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      break;
    }

    for (auto& kv : kvs) {
      ++checked_count;
      auto pb_inode = MetaDataCodec::DecodeInodeValue(kv.value);

      std::vector<pb::mdsv2::TrashSlice> trash_slices;
      status = CompactFile(ctx, pb_inode.ino(), trash_slices);
      if (!status.ok()) {
        break;
      }
      if (!trash_slices.empty()) {
        ++compacted_count;
      }
    }

  } while (status.ok() && kvs.size() >= FLAGS_fs_scan_batch_size);

  DINGO_LOG(INFO) << fmt::format("[fs.{}] compactall finish, elapsed_time({}us) status({}).", fs_id_,
                                 (Helper::TimestampNs() - now_ns) / 1000, status.error_str());

  return status;
}

Status FileSystem::CleanTrashFileData(Context& ctx, uint64_t ino) {
  // auto trace = ctx.GetTrace();
  // auto& trace_txn = trace.GetTxn();

  // Range range;
  // MetaDataCodec::GetTrashChunkRange(fs_id_, ino, range.start_key, range.end_key);

  // auto txn = kv_storage_->NewTxn();
  // std::vector<KeyValue> kvs;
  // do {
  //   kvs.clear();
  //   auto status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
  //   if (!status.ok()) {
  //     return status;
  //   }

  //   for (const auto& kv : kvs) {
  //     pb::mdsv2::TrashSlice trash_slice = MetaDataCodec::DecodeTrashChunkValue(kv.value);

  //     auto status = data_accessor_->Delete("");
  //     if (!status.ok()) {
  //       DINGO_LOG(ERROR) << fmt::format("[fs.{}] delete trash slice({}) fail, {}", fs_id_,
  //                                       trash_slice.ShortDebugString(), status.ToString());
  //     }
  //   }

  // } while (kvs.size() >= FLAGS_fs_scan_batch_size);

  return Status::OK();
}

Status FileSystem::GetDentry(Context& ctx, uint64_t parent, const std::string& name, Dentry& dentry) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] getdentry name({}/{}).", fs_id_, parent, name);

  bool bypass_cache = ctx.IsBypassCache();
  auto& trace = ctx.GetTrace();

  if (!bypass_cache) {
    auto partition = GetPartitionFromCache(parent);
    if (partition != nullptr) {
      trace.SetHitPartition();
      if (partition->GetChild(name, dentry)) {
        trace.SetHitDentry();
        return Status::OK();
      }
    }
  }

  return GetDentryFromStore(parent, name, dentry);
}

Status FileSystem::ListDentry(Context& ctx, uint64_t parent, const std::string& last_name, uint32_t limit,
                              bool is_only_dir, std::vector<Dentry>& dentries) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] listdentry name({}/{}) limit({}).", fs_id_, parent, last_name, limit);

  bool bypass_cache = ctx.IsBypassCache();
  auto& trace = ctx.GetTrace();

  if (!bypass_cache) {
    auto partition = GetPartitionFromCache(parent);
    if (partition != nullptr) {
      trace.SetHitPartition();
      dentries = partition->GetChildren(last_name, limit, is_only_dir);
      return Status::OK();
    }
  }

  return ListDentryFromStore(parent, last_name, limit, is_only_dir, dentries);
}

Status FileSystem::GetInode(Context& ctx, uint64_t ino, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] getinode ino({}).", fs_id_, ino);

  bool bypass_cache = ctx.IsBypassCache();
  auto& trace = ctx.GetTrace();

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  entry_out.inode = inode->CopyTo();

  return Status::OK();
}

Status FileSystem::BatchGetInode(Context& ctx, const std::vector<uint64_t>& inoes, std::vector<EntryOut>& out_entries) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] batchgetinode inoes({}).", fs_id_, Helper::VectorToString(inoes));

  bool bypass_cache = ctx.IsBypassCache();
  auto& trace = ctx.GetTrace();

  out_entries.reserve(inoes.size());
  if (!bypass_cache) {
    for (auto ino : inoes) {
      InodeSPtr inode = GetInodeFromCache(ino);
      if (inode == nullptr) {
        DINGO_LOG(WARNING) << fmt::format("[fs.{}] not found inode({}).", fs_id_, ino);
        continue;
      }

      EntryOut entry_out;
      entry_out.inode = inode->CopyTo();
      out_entries.push_back(entry_out);
    }

  } else {
    std::vector<InodeSPtr> inodes;
    auto status = BatchGetInodeFromStore(inoes, inodes);
    if (!status.ok()) {
      return status;
    }

    for (auto& inode : inodes) {
      EntryOut entry_out;
      entry_out.inode = inode->CopyTo();
      out_entries.push_back(entry_out);
    }
  }

  return Status::OK();
}

Status FileSystem::BatchGetXAttr(Context& ctx, const std::vector<uint64_t>& inoes,
                                 std::vector<pb::mdsv2::XAttr>& out_xattrs) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] batchgetxattr inoes({}).", fs_id_, Helper::VectorToString(inoes));

  bool bypass_cache = ctx.IsBypassCache();
  auto& trace = ctx.GetTrace();

  auto add_xattr_func = [&out_xattrs](const InodeSPtr& inode) {
    pb::mdsv2::XAttr xattr;
    for (auto& [k, v] : inode->GetXAttrMap()) {
      xattr.mutable_xattrs()->insert({k, v});
    }
    out_xattrs.push_back(xattr);
  };

  out_xattrs.reserve(inoes.size());
  if (!bypass_cache) {
    for (auto ino : inoes) {
      InodeSPtr inode = GetInodeFromCache(ino);
      if (inode == nullptr) {
        DINGO_LOG(WARNING) << fmt::format("[fs.{}] not found inode({}).", fs_id_, ino);
        continue;
      }

      add_xattr_func(inode);
    }

  } else {
    std::vector<InodeSPtr> inodes;
    auto status = BatchGetInodeFromStore(inoes, inodes);
    if (!status.ok()) {
      return status;
    }

    for (auto& inode : inodes) {
      add_xattr_func(inode);
    }
  }

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

FileSystemSet::FileSystemSet(CoordinatorClientSPtr coordinator_client, IdGeneratorPtr id_generator,
                             KVStorageSPtr kv_storage, MDSMeta self_mds_meta, MDSMetaMapSPtr mds_meta_map,
                             RenamerPtr renamer, MutationProcessorSPtr mutation_processor)
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
  fs_info.set_uuid(utils::UUIDGenerator::GenerateUUID());

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
                            renamer_, mutation_processor_, mds_meta_map_);
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

Status FileSystemSet::UpdateFsInfo(Context& ctx, const std::string& fs_name, const pb::mdsv2::FsInfo& fs_info) {
  auto trace = ctx.GetTrace();
  auto& trace_txn = trace.GetTxn();

  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);

  Status status;
  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

    std::string value;
    status = txn->Get(fs_key, value);
    if (!status.ok()) {
      return Status(pb::error::EBACKEND_STORE, fmt::format("get fs({}) fail, {}.", fs_name, status.error_str()));
    }

    auto new_fs_info = MetaDataCodec::DecodeFSValue(value);
    new_fs_info.set_capacity(fs_info.capacity());
    new_fs_info.set_block_size(fs_info.block_size());
    new_fs_info.set_owner(fs_info.owner());
    new_fs_info.set_recycle_time_hour(fs_info.recycle_time_hour());

    txn->Put(fs_key, MetaDataCodec::EncodeFSValue(new_fs_info));

    status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.retry = retry;

  return status;
}

Status FileSystemSet::GetFsInfo(Context& ctx, const std::string& fs_name, pb::mdsv2::FsInfo& fs_info) {
  auto& trace_txn = ctx.GetTrace().GetTxn();

  uint64_t now_us = Helper::TimestampUs();
  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}), {}.", fs_name, status.error_str()));
  }

  trace_txn.read_time_us = Helper::TimestampUs() - now_us;

  fs_info = MetaDataCodec::DecodeFSValue(value);

  return Status::OK();
}

Status FileSystemSet::GetAllFsInfo(Context& ctx, std::vector<pb::mdsv2::FsInfo>& fs_infoes) {
  auto& trace_txn = ctx.GetTrace().GetTxn();

  uint64_t now_us = Helper::TimestampUs();

  Range range;
  MetaDataCodec::GetFsTableRange(range.start_key, range.end_key);

  // scan fs table from kv storage
  std::vector<KeyValue> kvs;
  auto status = kv_storage_->Scan(range, kvs);
  if (!status.ok()) {
    return status;
  }

  trace_txn.read_time_us = Helper::TimestampUs() - now_us;

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

bool FileSystemSet::AddFileSystem(FileSystemSPtr fs, bool is_force) {
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

FileSystemSPtr FileSystemSet::GetFileSystem(uint32_t fs_id) {
  utils::ReadLockGuard lk(lock_);

  auto it = fs_map_.find(fs_id);
  return it != fs_map_.end() ? it->second : nullptr;
}

FileSystemSPtr FileSystemSet::GetFileSystem(const std::string& fs_name) {
  utils::ReadLockGuard lk(lock_);

  for (auto& [fs_id, fs] : fs_map_) {
    if (fs->FsName() == fs_name) {
      return fs;
    }
  }

  return nullptr;
}

uint32_t FileSystemSet::GetFsId(const std::string& fs_name) {
  utils::ReadLockGuard lk(lock_);

  for (auto& [fs_id, fs] : fs_map_) {
    if (fs->FsName() == fs_name) {
      return fs_id;
    }
  }

  return 0;
}

std::vector<FileSystemSPtr> FileSystemSet::GetAllFileSystem() {
  utils::ReadLockGuard lk(lock_);

  std::vector<FileSystemSPtr> fses;
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

      file_system = FileSystem::New(self_mds_meta_.ID(), FsInfo::NewUnique(fs_info), std::move(id_generator),
                                    kv_storage_, renamer_, mutation_processor_, mds_meta_map_);
      AddFileSystem(file_system);

    } else {
      file_system->RefreshFsInfo(fs_info);
    }
  }

  return true;
}

}  // namespace mdsv2
}  // namespace dingofs
