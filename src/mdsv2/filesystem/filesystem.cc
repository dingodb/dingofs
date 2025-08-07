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

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
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
#include "mdsv2/common/time.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/common/type.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/file_session.h"
#include "mdsv2/filesystem/fs_info.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/filesystem/notify_buddy.h"
#include "mdsv2/filesystem/store_operation.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/storage/storage.h"
#include "utils/uuid.h"

namespace dingofs {
namespace mdsv2 {

static const std::string kFsTableName = "dingofs";

static const std::string kStatsName = ".stats";
static const std::string kRecyleName = ".recycle";

DEFINE_uint32(filesystem_name_max_size, 1024, "Max size of filesystem name.");
DEFINE_uint32(filesystem_hash_bucket_num, 1024, "Filesystem hash bucket num.");

DEFINE_uint32(compact_chunk_threshold_num, 64, "Compact chunk threshold num.");
DEFINE_uint32(compact_chunk_interval_ms, 30 * 1000, "Compact chunk interval ms.");

static bool IsReserveNode(Ino ino) { return ino == kRootIno; }

static bool IsReserveName(const std::string& name) { return name == kStatsName || name == kRecyleName; }

static bool IsInvalidName(const std::string& name) {
  return name.empty() || name.size() > FLAGS_filesystem_name_max_size;
}

FileSystem::FileSystem(int64_t self_mds_id, FsInfoUPtr fs_info, IdGeneratorUPtr ino_id_generator,
                       IdGeneratorSPtr slice_id_generator, KVStorageSPtr kv_storage,
                       OperationProcessorSPtr operation_processor, MDSMetaMapSPtr mds_meta_map,
                       WorkerSetSPtr quota_worker_set, notify::NotifyBuddySPtr notify_buddy)
    : self_mds_id_(self_mds_id),
      fs_info_(std::move(fs_info)),
      fs_id_(fs_info_->GetFsId()),
      inode_cache_(fs_id_),
      partition_cache_(fs_id_),
      ino_id_generator_(std::move(ino_id_generator)),
      slice_id_generator_(slice_id_generator),
      kv_storage_(kv_storage),
      operation_processor_(operation_processor),
      mds_meta_map_(mds_meta_map),
      parent_memo_(ParentMemo::New(fs_id_)),
      chunk_cache_(fs_id_),
      quota_manager_(quota::QuotaManager::New(fs_id_, parent_memo_, operation_processor, quota_worker_set)),
      notify_buddy_(notify_buddy),
      file_session_manager_(fs_id_, operation_processor) {
  can_serve_ = CanServe(self_mds_id);
};

FileSystem::~FileSystem() {
  // destroy
  quota_manager_->Destroy();

  renamer_.Destroy();
}

FileSystemSPtr FileSystem::GetSelfPtr() { return std::dynamic_pointer_cast<FileSystem>(shared_from_this()); }

bool FileSystem::Init() {
  if (!ino_id_generator_->Init()) {
    DINGO_LOG(ERROR) << fmt::format("[fs.{}] init generator fail.", fs_id_);
    return false;
  }

  if (!quota_manager_->Init()) {
    DINGO_LOG(ERROR) << fmt::format("[fs.{}] init quota manager fail.", fs_id_);
    return false;
  }

  if (!renamer_.Init()) {
    DINGO_LOG(ERROR) << fmt::format("[fs.{}] init renamer fail.", fs_id_);
    return false;
  }

  return true;
}

uint64_t FileSystem::Epoch() const {
  auto partition_policy = fs_info_->GetPartitionPolicy();
  return partition_policy.epoch();
}

pb::mdsv2::PartitionType FileSystem::PartitionType() const { return fs_info_->GetPartitionType(); }

bool FileSystem::IsMonoPartition() const {
  return fs_info_->GetPartitionType() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION;
}
bool FileSystem::IsParentHashPartition() const {
  return fs_info_->GetPartitionType() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION;
}

// odd number is dir inode
Status FileSystem::GenDirIno(Ino& ino) {
  bool ret = ino_id_generator_->GenID(2, ino);
  ino = (ino & 1) ? ino : (ino + 1);  // ensure odd number for dir inode

  return ret ? Status::OK() : Status(pb::error::EGEN_FSID, "generate inode id fail");
}

// even number is file inode
Status FileSystem::GenFileIno(Ino& ino) {
  bool ret = ino_id_generator_->GenID(2, ino);
  ino = (ino & 1) ? (ino + 1) : ino;  // ensure even number for file inode

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

Status FileSystem::GetPartition(Context& ctx, Ino parent, PartitionPtr& out_partition) {
  return GetPartition(ctx, ctx.GetInodeVersion(), parent, out_partition);
}

Status FileSystem::GetPartition(Context& ctx, uint64_t version, Ino parent, PartitionPtr& out_partition) {
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

PartitionPtr FileSystem::GetPartitionFromCache(Ino parent) { return partition_cache_.Get(parent); }

std::map<uint64_t, PartitionPtr> FileSystem::GetAllPartitionsFromCache() { return partition_cache_.GetAll(); }

Status FileSystem::GetPartitionFromStore(Ino parent, const std::string& reason, PartitionPtr& out_partition) {
  // scan dentry from store
  Range range = MetaCodec::GetDentryRange(fs_id_, parent, true);

  std::vector<KeyValue> kvs;
  auto status = kv_storage_->Scan(range, kvs);
  if (!status.ok()) {
    return status;
  }

  if (kvs.empty()) {
    return Status(pb::error::ENOT_FOUND, "not found kv");
  }

  auto& parent_kv = kvs.at(0);
  CHECK(parent_kv.key == range.start) << fmt::format("invalid parent key({}/{}).", Helper::StringToHex(parent_kv.key),
                                                     Helper::StringToHex(range.start));

  // build partition
  auto parent_inode = Inode::New(MetaCodec::DecodeInodeValue(parent_kv.value));
  auto partition = Partition::New(parent_inode);

  // add child dentry
  for (size_t i = 1; i < kvs.size(); ++i) {
    const auto& kv = kvs.at(i);
    auto dentry = MetaCodec::DecodeDentryValue(kv.value);
    partition->PutChild(dentry);
  }

  partition_cache_.Put(parent, partition);
  inode_cache_.PutInode(parent, parent_inode);

  out_partition = partition;

  DINGO_LOG(INFO) << fmt::format("[fs.{}] fetch partition({}), reason({}).", fs_id_, parent, reason);

  return Status::OK();
}

Status FileSystem::GetDentryFromStore(Ino parent, const std::string& name, Dentry& dentry) {
  std::string value;
  auto status = kv_storage_->Get(MetaCodec::EncodeDentryKey(fs_id_, parent, name), value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found dentry({}/{}), {}", parent, name, status.error_str()));
  }

  DINGO_LOG(INFO) << fmt::format("[fs.{}] fetch dentry({}/{}).", fs_id_, parent, name);

  dentry = Dentry(MetaCodec::DecodeDentryValue(value));

  return Status::OK();
}

Status FileSystem::ListDentryFromStore(Ino parent, const std::string& last_name, uint32_t limit, bool is_only_dir,
                                       std::vector<Dentry>& dentries) {
  limit = limit > 0 ? limit : UINT32_MAX;

  Trace trace;
  ScanDentryOperation operation(trace, fs_id_, parent, last_name, [&](DentryType dentry) -> bool {
    if (is_only_dir && dentry.type() != pb::mdsv2::FileType::DIRECTORY) {
      return true;  // skip non-directory entries
    }

    dentries.push_back(Dentry(dentry));
    return dentries.size() < limit;
  });

  return RunOperation(&operation);
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
      status = GetInodeFromStore(dentry.INo(), "Bypass", false, out_inode);
      is_fetch = true;
      break;
    }

    auto inode = dentry.Inode();
    if (inode == nullptr) {
      inode = GetInodeFromCache(dentry.INo());
      if (inode == nullptr) {
        status = GetInodeFromStore(dentry.INo(), "CacheMiss", true, out_inode);
        is_fetch = true;
        break;
      }
    }

    if (inode->Version() < version) {
      status = GetInodeFromStore(dentry.INo(), "OutOfDate", true, out_inode);
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

Status FileSystem::GetInode(Context& ctx, Ino ino, InodeSPtr& out_inode) {
  return GetInode(ctx, ctx.GetInodeVersion(), ino, out_inode);
}

Status FileSystem::GetInode(Context& ctx, uint64_t version, Ino ino, InodeSPtr& out_inode) {
  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();

  if (bypass_cache) {
    return GetInodeFromStore(ino, "Bypass", false, out_inode);
  }

  auto inode = GetInodeFromCache(ino);
  if (inode == nullptr) {
    return GetInodeFromStore(ino, "CacheMiss", true, out_inode);
  }

  if (inode->Version() < version) {
    return GetInodeFromStore(ino, "OutOfDate", true, out_inode);
  }

  out_inode = inode;
  trace.SetHitInode();

  return Status::OK();
}

InodeSPtr FileSystem::GetInodeFromCache(Ino ino) { return inode_cache_.GetInode(ino); }

std::map<uint64_t, InodeSPtr> FileSystem::GetAllInodesFromCache() { return inode_cache_.GetAllInodes(); }

Status FileSystem::GetInodeFromStore(Ino ino, const std::string& reason, bool is_cache, InodeSPtr& out_inode) {
  Trace trace;
  GetInodeAttrOperation operation(trace, fs_id_, ino);

  auto status = RunOperation(&operation);
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      DINGO_LOG(ERROR) << fmt::format("[fs.{}] fetch inode({}) from store fail, reason({}), status({}).", fs_id_, ino,
                                      reason, status.error_str());
    }
    return status;
  }

  auto& result = operation.GetResult();

  out_inode = Inode::New(result.attr);

  if (is_cache) inode_cache_.PutInode(ino, out_inode);

  DINGO_LOG(INFO) << fmt::format("[fs.{}] fetch inode({}), reason({}).", fs_id_, ino, reason);

  return Status::OK();
}

Status FileSystem::BatchGetInodeFromStore(std::vector<uint64_t> inoes, std::vector<InodeSPtr>& out_inodes) {
  std::vector<std::string> keys;
  keys.reserve(inoes.size());
  for (auto ino : inoes) {
    keys.push_back(MetaCodec::EncodeInodeKey(fs_id_, ino));
  }

  std::vector<KeyValue> kvs;
  auto status = kv_storage_->BatchGet(keys, kvs);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND,
                  fmt::format("not found inode({}), {}", Helper::VectorToString(inoes), status.error_str()));
  }

  for (const auto& kv : kvs) {
    uint32_t fs_id = 0;
    Ino ino = 0;
    MetaCodec::DecodeInodeKey(kv.key, fs_id, ino);
    out_inodes.push_back(Inode::New(MetaCodec::DecodeInodeValue(kv.value)));
  }

  return Status::OK();
}

Status FileSystem::GetDelFileFromStore(Ino ino, AttrType& out_attr) {
  std::string key = MetaCodec::EncodeDelFileKey(fs_id_, ino);
  std::string value;
  auto status = kv_storage_->Get(key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found inode({}), {}", ino, status.error_str()));
  }
  out_attr = MetaCodec::DecodeDelFileValue(value);

  return Status::OK();
}

void FileSystem::DeleteInodeFromCache(Ino ino) { inode_cache_.DeleteInode(ino); }

void FileSystem::ClearCache() {
  partition_cache_.Clear();
  inode_cache_.Clear();
}

void FileSystem::BatchDeleteCache(uint32_t bucket_num, const std::set<uint32_t>& bucket_ids) {
  if (bucket_ids.empty()) return;

  auto check_fn = [&](const Ino& ino) -> bool {
    uint32_t bucket_id = ino % bucket_num;

    return (bucket_ids.find(bucket_id) != bucket_ids.end());
  };

  partition_cache_.BatchDeleteInodeIf(check_fn);
  inode_cache_.BatchDeleteInodeIf(check_fn);
}

Status FileSystem::DestoryInode(uint32_t fs_id, Ino ino) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] destory inode {}.", fs_id_, ino);

  std::string inode_key = MetaCodec::EncodeInodeKey(fs_id, ino);
  auto status = kv_storage_->Delete(inode_key);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("delete inode fail, {}", status.error_str()));
  }

  inode_cache_.DeleteInode(ino);

  return Status::OK();
}

static uint64_t ElapsedTimeUs(uint64_t start_time_ns) { return (Helper::TimestampNs() - start_time_ns) / 1000; }

Status FileSystem::RunOperation(Operation* operation) {
  CHECK(operation != nullptr) << "operation is null.";

  if (!operation->IsBatchRun()) {
    return operation_processor_->RunAlone(operation);
  }

  bthread::CountdownEvent count_down(1);

  operation->SetEvent(&count_down);

  if (!operation_processor_->RunBatched(operation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  return operation->GetResult().status;
}

Status FileSystem::CreateRoot() {
  CHECK(fs_id_ > 0) << "fs_id is invalid.";

  uint64_t time_ns = Helper::TimestampNs();

  AttrType attr;
  attr.set_fs_id(fs_id_);
  attr.set_ino(kRootIno);
  attr.set_length(0);
  attr.set_uid(1008);
  attr.set_gid(1008);
  attr.set_mode(S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  attr.set_nlink(kEmptyDirMinLinkNum);
  attr.set_type(pb::mdsv2::FileType::DIRECTORY);
  attr.set_rdev(0);

  attr.set_ctime(time_ns);
  attr.set_mtime(time_ns);
  attr.set_atime(time_ns);

  attr.add_parents(kRootParentIno);

  auto inode = Inode::New(attr);

  Dentry dentry(fs_id_, "/", kRootParentIno, kRootIno, pb::mdsv2::FileType::DIRECTORY, 0, inode);

  // update backend store
  Trace trace;
  CreateRootOperation operation(trace, dentry, attr);

  auto status = RunOperation(&operation);
  auto& result = operation.GetResult();
  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] create root finish, status({}).", fs_id_, ElapsedTimeUs(time_ns),
                                 status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("create root fail, {}", status.error_str()));
  }

  inode_cache_.PutInode(inode->Ino(), inode);
  partition_cache_.Put(dentry.INo(), Partition::New(inode));

  return Status::OK();
}

Status FileSystem::CreateQuota() {
  Trace trace;
  QuotaEntry quota_entry;
  quota_entry.set_max_inodes(INT64_MAX);
  quota_entry.set_max_bytes(INT64_MAX);
  quota_entry.set_used_inodes(1);

  auto status = quota_manager_->SetFsQuota(trace, quota_entry);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[fs.{}] create quota fail, status({}).", fs_id_, status.error_str());
    return Status(pb::error::EBACKEND_STORE, fmt::format("create quota fail, {}", status.error_str()));
  }

  return Status::OK();
}

Status FileSystem::Lookup(Context& ctx, Ino parent, const std::string& name, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] lookup parent({}), name({}).", fs_id_, parent, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  uint64_t time_ns = Helper::TimestampNs();

  PartitionPtr partition;
  auto status = GetPartition(ctx, parent, partition);
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

  parent_memo_->Remeber(inode->Ino(), parent);

  entry_out.attr = inode->Copy();

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] lookup parent({}), name({}) version({}) ptr({}).", fs_id_,
                                 ElapsedTimeUs(time_ns), parent, name, entry_out.attr.version(), (void*)inode.get());

  return Status::OK();
}

uint64_t FileSystem::GetMdsIdByIno(Ino ino) {
  ino = ino != kRootParentIno ? ino : kRootIno;
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
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] mknod parent({}), name({}).", fs_id_, param.parent, param.name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();
  Ino parent = param.parent;

  // check request
  if (param.name.empty()) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "name is empty");
  }

  if (param.parent == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "invalid parent inode id");
  }

  // get dentry set
  PartitionPtr partition;
  auto status = GetPartition(ctx, parent, partition);
  if (!status.ok()) {
    return status;
  }
  auto parent_inode = partition->ParentInode();

  // generate inode id
  Ino ino = 0;
  status = GenFileIno(ino);
  if (!status.ok()) {
    return status;
  }

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  // check quota
  if (!quota_manager_->CheckQuota(param.parent, 0, 1)) {
    return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
  }

  uint64_t time_ns = Helper::TimestampNs();

  // build inode
  Inode::AttrType attr;
  attr.set_fs_id(fs_id_);
  attr.set_ino(ino);
  attr.set_length(0);
  attr.set_ctime(time_ns);
  attr.set_mtime(time_ns);
  attr.set_atime(time_ns);
  attr.set_uid(param.uid);
  attr.set_gid(param.gid);
  attr.set_mode(param.mode);
  attr.set_nlink(1);
  attr.set_type(pb::mdsv2::FileType::FILE);
  attr.set_rdev(param.rdev);
  attr.add_parents(parent);

  auto inode = Inode::New(attr);

  // build dentry
  Dentry dentry(fs_id_, param.name, parent, ino, pb::mdsv2::FileType::FILE, param.flag, inode);

  // update backend store
  MkNodOperation operation(trace, dentry, attr);
  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] mknod {} finish, status({}).", fs_id_, ElapsedTimeUs(time_ns),
                                 param.name, status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;

  // update cache
  inode_cache_.PutInode(ino, inode);
  partition->PutChild(dentry);
  parent_inode->UpdateIf(parent_attr);

  // update quota
  quota_manager_->UpdateFsUsage(0, 1);
  quota_manager_->AysncUpdateDirUsage(param.parent, 0, 1);

  parent_memo_->Remeber(attr.ino(), param.parent);

  entry_out.attr.Swap(&attr);
  entry_out.parent_version = parent_attr.version();

  if (IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(parent_attr));
  }

  return Status::OK();
}

Status FileSystem::Open(Context& ctx, Ino ino, uint32_t flags, std::string& session_id, uint64_t& version) {
  DINGO_LOG(INFO) << fmt::format("[fs.{}] open ino({}).", fs_id_, ino);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  if ((flags & O_TRUNC) && !(flags & O_WRONLY || flags & O_RDWR)) {
    return Status(pb::error::ENO_PERMISSION, "O_TRUNC without O_WRONLY or O_RDWR");
  }

  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();
  const std::string& client_id = ctx.ClientId();

  uint64_t time_ns = Helper::TimestampNs();

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  FileSessionPtr file_session;
  status = file_session_manager_.Create(ino, client_id, file_session);
  if (!status.ok()) {
    return status;
  }

  OpenFileOperation operation(trace, flags, *file_session);

  status = RunOperation(&operation);
  if (!status.ok()) {
    return status;
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;
  int64_t delta_bytes = result.delta_bytes;

  version = attr.version();
  session_id = file_session->session_id();

  // update quota
  if (delta_bytes != 0) {
    quota_manager_->UpdateFsUsage(delta_bytes, 0);
    for (auto parent : attr.parents()) {
      quota_manager_->AysncUpdateDirUsage(parent, delta_bytes, 0);
    }
  }

  // update cache
  inode->UpdateIf(std::move(attr));

  return Status::OK();
}

Status FileSystem::Release(Context& ctx, Ino ino, const std::string& session_id) {
  DINGO_LOG(INFO) << fmt::format("[fs.{}] release ino({}).", fs_id_, ino);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  CloseFileOperation operation(trace, fs_id_, ino, session_id);

  auto status = RunOperation(&operation);
  if (!status.ok()) {
    return status;
  }

  auto& result = operation.GetResult();

  // delete cache
  file_session_manager_.Delete(ino, session_id);

  return Status::OK();
}

// create directory, need below steps:
// 1. create inode
// 2. create dentry and update parent inode(nlink/mtime/ctime)
Status FileSystem::MkDir(Context& ctx, const MkDirParam& param, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] mkdir parent({}), name({}).", fs_id_, param.parent, param.name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();
  Ino parent = param.parent;

  // check request
  if (param.name.empty()) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "name is empty.");
  }

  if (param.parent == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "invalid parent inode id.");
  }

  // get parent dentry
  PartitionPtr partition;
  auto status = GetPartition(ctx, parent, partition);
  if (!status.ok()) {
    return status;
  }
  auto parent_inode = partition->ParentInode();

  // generate inode id
  Ino ino = 0;
  status = GenDirIno(ino);
  if (!status.ok()) {
    return status;
  }

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  // check quota
  if (!quota_manager_->CheckQuota(param.parent, 0, 1)) {
    return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
  }

  // build inode
  uint64_t time_ns = Helper::TimestampNs();

  Inode::AttrType attr;
  attr.set_fs_id(fs_id_);
  attr.set_ino(ino);
  attr.set_length(4096);
  attr.set_ctime(time_ns);
  attr.set_mtime(time_ns);
  attr.set_atime(time_ns);
  attr.set_uid(param.uid);
  attr.set_gid(param.gid);
  attr.set_mode(S_IFDIR | param.mode);
  attr.set_nlink(kEmptyDirMinLinkNum);
  attr.set_type(pb::mdsv2::FileType::DIRECTORY);
  attr.set_rdev(param.rdev);
  attr.add_parents(parent);

  auto inode = Inode::New(attr);

  // build dentry
  Dentry dentry(fs_id_, param.name, parent, ino, pb::mdsv2::FileType::DIRECTORY, param.flag, inode);

  // update backend store
  MkDirOperation operation(trace, dentry, attr);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] mkdir {} finish, status({}).", fs_id_, ElapsedTimeUs(time_ns),
                                 param.name, status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;

  // update cache
  inode_cache_.PutInode(ino, inode);
  partition->PutChild(dentry);
  parent_inode->UpdateIf(parent_attr);
  partition_cache_.Put(ino, Partition::New(inode));

  // update quota
  quota_manager_->UpdateFsUsage(0, 1);
  quota_manager_->AysncUpdateDirUsage(param.parent, 0, 1);

  parent_memo_->Remeber(attr.ino(), param.parent);

  entry_out.attr.Swap(&attr);
  entry_out.parent_version = parent_attr.version();

  if (IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(parent_attr));
  }

  return Status::OK();
}

Status FileSystem::RmDir(Context& ctx, Ino parent, const std::string& name) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] rmdir parent({}), name({}).", fs_id_, parent, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();

  PartitionPtr parent_partition;
  auto status = GetPartition(ctx, parent, parent_partition);
  if (!status.ok()) {
    return status;
  }

  Dentry dentry;
  if (!parent_partition->GetChild(name, dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("child dentry({}) not found.", name));
  }

  PartitionPtr partition = GetPartitionFromCache(dentry.INo());
  if (partition != nullptr) {
    InodeSPtr inode = partition->ParentInode();
    if (inode->Nlink() > kEmptyDirMinLinkNum) {
      return Status(pb::error::ENOT_EMPTY,
                    fmt::format("dir({}/{}) is not empty, nlink({}).", parent, name, inode->Nlink()));
    }
  }

  // check directory is empty
  if (partition->HasChild()) {
    return Status(pb::error::ENOT_EMPTY,
                  fmt::format("dir({}/{}) is not empty, nlink({}).", parent, name, dentry.Inode()->Nlink()));
  }

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  uint64_t time_ns = Helper::TimestampNs();

  // update backend store
  RmDirOperation operation(trace, dentry);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] rmdir {} finish, status({}).", fs_id_, ElapsedTimeUs(time_ns), name,
                                 status.error_str());
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("delete inode/dentry fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;

  // update cache
  parent_partition->DeleteChild(name);
  parent_partition->ParentInode()->UpdateIf(parent_attr);
  partition_cache_.Delete(dentry.INo());

  // update quota
  quota_manager_->UpdateFsUsage(0, -1);
  quota_manager_->AysncUpdateDirUsage(parent, 0, -1);

  parent_memo_->Forget(dentry.INo());

  if (IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(parent_attr));
  }

  return Status::OK();
}

Status FileSystem::ReadDir(Context& ctx, Ino ino, const std::string& last_name, uint limit, bool with_attr,
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
    entry_out.attr.set_ino(dentry.INo());

    if (with_attr) {
      // need inode attr
      InodeSPtr inode;
      status = GetInode(ctx, 0, dentry, partition, inode);
      if (!status.ok()) {
        return status;
      }

      entry_out.attr = inode->Copy();
    }

    entry_outs.push_back(std::move(entry_out));
  }

  return Status::OK();
}

// create hard link for file
// 1. create dentry and update parent inode(nlink/mtime/ctime)
// 2. update inode(mtime/ctime/nlink)
Status FileSystem::Link(Context& ctx, Ino ino, Ino new_parent, const std::string& new_name, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] link ino({}), new_parent({}), new_name({}).", fs_id_, ino, new_parent,
                                  new_name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  PartitionPtr partition;
  auto status = GetPartition(ctx, new_parent, partition);
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

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  // check quota
  if (!quota_manager_->CheckQuota(new_parent, 0, 1)) {
    return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
  }

  uint32_t fs_id = inode->FsId();

  // build dentry
  Dentry dentry(fs_id, new_name, new_parent, ino, pb::mdsv2::FileType::FILE, 0, inode);

  // update backend store
  uint64_t time_ns = Helper::TimestampNs();

  HardLinkOperation operation(trace, dentry);
  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] link {} -> {}/{} finish, status({}).", fs_id_, ElapsedTimeUs(time_ns),
                                 ino, new_parent, new_name, status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;
  auto& child_attr = result.child_attr;

  // update quota
  quota_manager_->AysncUpdateDirUsage(new_parent, child_attr.length(), 1);

  // update cache
  inode->UpdateIf(std::move(child_attr));
  parent_inode->UpdateIf(parent_attr);

  inode_cache_.PutInode(ino, inode);
  partition->PutChild(dentry);

  entry_out.attr = inode->Copy();
  entry_out.parent_version = parent_attr.version();

  if (IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(parent_attr));
  }

  return Status::OK();
}

// delete hard link for file
// 1. delete dentry and update parent inode(nlink/mtime/ctime)
// 3. update inode(nlink/mtime/ctime)
Status FileSystem::UnLink(Context& ctx, Ino parent, const std::string& name) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] unLink parent({}), name({}).", fs_id_, parent, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  PartitionPtr partition;
  auto status = GetPartition(ctx, parent, partition);
  if (!status.ok()) {
    return status;
  }

  Dentry dentry;
  if (!partition->GetChild(name, dentry)) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found dentry({}/{})", parent, name));
  }

  InodeSPtr inode;
  status = GetInode(ctx, dentry, partition, inode);
  if (!status.ok()) {
    return status;
  }

  if (inode->Type() == pb::mdsv2::FileType::DIRECTORY) {
    return Status(pb::error::ENOT_FILE, "directory not allow unlink");
  }

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  uint64_t time_ns = Helper::TimestampNs();

  // update backend store
  UnlinkOperation operation(trace, dentry);

  status = RunOperation(&operation);

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;
  auto& child_attr = result.child_attr;

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] unlink {}/{} finish, nlink({}) status({}).", fs_id_,
                                 ElapsedTimeUs(time_ns), parent, name, child_attr.nlink(), status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", status.error_str()));
  }

  // update quota
  int64_t delta_bytes = child_attr.type() != pb::mdsv2::SYM_LINK ? child_attr.length() : 0;
  if (child_attr.nlink() == 0) {
    quota_manager_->UpdateFsUsage(-delta_bytes, -1);
    chunk_cache_.Delete(child_attr.ino());
  }
  quota_manager_->AysncUpdateDirUsage(parent, -delta_bytes, -1);

  // update cache
  partition->DeleteChild(name);
  partition->ParentInode()->UpdateIf(parent_attr);

  inode->UpdateIf(std::move(child_attr));

  if (IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(parent_attr));
  }

  return Status::OK();
}

// create symbol link
// 1. create inode
// 2. create dentry
// 3. update parent inode mtime/ctime/nlink
Status FileSystem::Symlink(Context& ctx, const std::string& symlink, Ino new_parent, const std::string& new_name,
                           uint32_t uid, uint32_t gid, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] symlink new_parent({}), new_name({}) symlink({}).", fs_id_, new_parent,
                                  new_name, symlink);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  if (new_parent == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid parent param.");
  }
  if (IsInvalidName(new_name)) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "Invalid name param.");
  }

  auto& trace = ctx.GetTrace();

  PartitionPtr partition;
  auto status = GetPartition(ctx, new_parent, partition);
  if (!status.ok()) {
    return status;
  }

  // generate inode id
  Ino ino = 0;
  status = GenFileIno(ino);
  if (!status.ok()) {
    return status;
  }

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  // check quota
  if (!quota_manager_->CheckQuota(new_parent, 0, 1)) {
    return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
  }

  // build inode
  uint64_t time_ns = Helper::TimestampNs();

  Inode::AttrType attr;
  attr.set_fs_id(fs_id_);
  attr.set_ino(ino);
  attr.set_symlink(symlink);
  attr.set_length(symlink.size());
  attr.set_ctime(time_ns);
  attr.set_mtime(time_ns);
  attr.set_atime(time_ns);
  attr.set_uid(uid);
  attr.set_gid(gid);
  attr.set_mode(S_IFLNK | 0777);
  attr.set_nlink(1);
  attr.set_type(pb::mdsv2::FileType::SYM_LINK);
  attr.set_rdev(1);
  attr.add_parents(new_parent);

  auto inode = Inode::New(attr);

  // build dentry
  Dentry dentry(fs_id_, new_name, new_parent, ino, pb::mdsv2::FileType::SYM_LINK, 0, inode);

  // update backend store
  SmyLinkOperation operation(trace, dentry, attr);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] symlink {}/{} finish,  status({}).", fs_id_, ElapsedTimeUs(time_ns),
                                 new_parent, new_name, status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;

  // update cache
  inode_cache_.PutInode(ino, inode);
  partition->PutChild(dentry);
  partition->ParentInode()->UpdateIf(parent_attr);

  // update quota
  quota_manager_->UpdateFsUsage(0, 1);
  quota_manager_->AysncUpdateDirUsage(new_parent, 0, 1);

  entry_out.attr.Swap(&attr);
  entry_out.parent_version = parent_attr.version();

  if (IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(parent_attr));
  }

  return Status::OK();
}

Status FileSystem::ReadLink(Context& ctx, Ino ino, std::string& link) {
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

Status FileSystem::GetAttr(Context& ctx, Ino ino, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] getattr ino({}).", fs_id_, ino);

  if (!ctx.IsBypassCache() && !CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  entry_out.attr = inode->Copy();

  return Status::OK();
}

Status FileSystem::SetAttr(Context& ctx, Ino ino, const SetAttrParam& param, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] setattr ino({}).", fs_id_, ino);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  uint64_t time_ns = Helper::TimestampNs();

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  uint64_t slice_num = 0;
  uint64_t slice_id = 0;
  if (param.to_set & kSetAttrLength) {
    if (param.attr.length() > inode->Length()) {
      // check quota
      if (!quota_manager_->CheckQuota(ino, param.attr.length() - inode->Length(), 0)) {
        return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
      }

      // prealloc slice id
      slice_num = (param.attr.length() - inode->Length()) / fs_info_->GetChunkSize() + 1;
      if (!slice_id_generator_->GenID(slice_num, 0, slice_id)) {
        return Status(pb::error::EINTERNAL, "generate slice id fail");
      }
    }
  }

  // update backend store
  UpdateAttrOperation::ExtraParam extra_param;
  if (slice_id > 0) {
    extra_param.block_size = fs_info_->GetBlockSize();
    extra_param.chunk_size = fs_info_->GetChunkSize();
    extra_param.slice_id = slice_id;
    extra_param.slice_num = slice_num;
  }

  UpdateAttrOperation operation(trace, ino, param.to_set, param.attr, extra_param);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] setattr {} finish, slice({}/{}) status({}).", fs_id_,
                                 ElapsedTimeUs(time_ns), ino, slice_id, slice_num, status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;

  entry_out.attr = attr;

  // update quota
  if (param.to_set & kSetAttrLength) {
    int64_t change_bytes = param.attr.length() > inode->Length()
                               ? param.attr.length() - inode->Length()
                               : -static_cast<int64_t>(inode->Length() - param.attr.length());
    quota_manager_->UpdateFsUsage(change_bytes, 0);

    for (const auto& parent : attr.parents()) {
      quota_manager_->AysncUpdateDirUsage(parent, change_bytes, 0);
    }
  }

  // update cache
  if (IsDir(ino) && IsParentHashPartition()) {
    inode->UpdateIf(attr);
    NotifyBuddyRefreshInode(std::move(attr));
  } else {
    inode->UpdateIf(std::move(attr));
  }

  return Status::OK();
}

Status FileSystem::GetXAttr(Context& ctx, Ino ino, Inode::XAttrMap& xattr) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] getxattr ino({}).", fs_id_, ino);

  if (!ctx.IsBypassCache() && !CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  xattr = inode->XAttrs();

  return Status::OK();
}

Status FileSystem::GetXAttr(Context& ctx, Ino ino, const std::string& name, std::string& value) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] getxattr ino({}), name({}).", fs_id_, ino, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  value = inode->XAttr(name);

  return Status::OK();
}

Status FileSystem::SetXAttr(Context& ctx, Ino ino, const Inode::XAttrMap& xattrs, uint64_t& version) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] setxattr ino({}).", fs_id_, ino);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  uint64_t time_ns = Helper::TimestampNs();

  // update backend store
  UpdateXAttrOperation operation(trace, fs_id_, ino, xattrs);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] setxattr {} finish, status({}).", fs_id_, ElapsedTimeUs(time_ns), ino,
                                 status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;

  version = attr.version();

  // update cache
  if (IsDir(ino) && IsParentHashPartition()) {
    inode->UpdateIf(attr);
    NotifyBuddyRefreshInode(std::move(attr));
  } else {
    inode->UpdateIf(std::move(attr));
  }

  return Status::OK();
}

Status FileSystem::RemoveXAttr(Context& ctx, Ino ino, const std::string& name, uint64_t& version) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] removexattr ino({}), name({}).", fs_id_, ino, name);

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  uint64_t time_ns = Helper::TimestampNs();

  // update backend store
  RemoveXAttrOperation operation(trace, fs_id_, ino, name);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] removexattr {} finish, status({}).", fs_id_, ElapsedTimeUs(time_ns),
                                 ino, status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;

  version = attr.version();

  // update cache
  if (IsDir(ino) && IsParentHashPartition()) {
    inode->UpdateIf(attr);
    NotifyBuddyRefreshInode(std::move(attr));
  } else {
    inode->UpdateIf(std::move(attr));
  }

  return Status::OK();
}

void FileSystem::UpdateParentMemo(const std::vector<Ino>& ancestors) {
  if (IsParentHashPartition()) {
    for (size_t i = 1; i < ancestors.size(); ++i) {
      const Ino& ino = ancestors[i - 1];
      const Ino& parent = ancestors[i];

      // update parent memo
      parent_memo_->Remeber(ino, parent);
    }
  }
}

void FileSystem::NotifyBuddyRefreshFsInfo(std::vector<int64_t> mds_ids, const FsInfoType& fs_info) {
  for (auto mds_id : mds_ids) {
    if (mds_id == 0 || mds_id == self_mds_id_) continue;

    notify_buddy_->AsyncNotify(notify::RefreshFsInfoMessage::Create(mds_id, fs_info.fs_id(), fs_info.fs_name()));
  }
}

void FileSystem::NotifyBuddyRefreshInode(AttrType&& attr) {
  if (notify_buddy_ == nullptr) return;

  Ino parent = kRootParentIno;
  if (attr.ino() != kRootIno) {
    CHECK(attr.parents_size() == 1) << fmt::format("parent size should be 1, but is {} ino({}).", attr.parents_size(),
                                                   attr.ino());
    parent = attr.parents().at(0);
  }
  auto mds_id = GetMdsIdByIno(parent);
  CHECK(mds_id != 0) << fmt::format("mds id should not be 0, ino({}).", parent);
  if (mds_id == self_mds_id_) {
    RefreshInode(attr);

  } else {
    notify_buddy_->AsyncNotify(notify::RefreshInodeMessage::Create(mds_id, fs_id_, std::move(attr)));
  }
}

void FileSystem::NotifyBuddyCleanPartitionCache(Ino ino) {
  if (notify_buddy_ == nullptr) return;

  auto mds_id = GetMdsIdByIno(ino);
  CHECK(mds_id != 0) << fmt::format("mds id should not be 0, ino({}).", ino);
  if (mds_id == self_mds_id_) {
    partition_cache_.Delete(ino);

  } else {
    notify_buddy_->AsyncNotify(notify::CleanPartitionCacheMessage::Create(mds_id, fs_id_, ino));
  }
}

Status FileSystem::Rename(Context& ctx, const RenameParam& param, uint64_t& old_parent_version,
                          uint64_t& new_parent_version) {
  Ino old_parent = param.old_parent;
  const std::string& old_name = param.old_name;
  Ino new_parent = param.new_parent;
  const std::string& new_name = param.new_name;

  DINGO_LOG(INFO) << fmt::format("fs.{}] rename {}/{} to {}/{}.", fs_id_, old_parent, old_name, new_parent, new_name);

  auto& trace = ctx.GetTrace();
  const bool bypass_cache = ctx.IsBypassCache();

  uint64_t time_ns = Helper::TimestampNs();

  // check name is valid
  if (new_name.size() > FLAGS_filesystem_name_max_size) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "new name is too long.");
  }

  if (old_parent == new_parent && old_name == new_name) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "not allow same name");
  }

  // update parent memo
  UpdateParentMemo(param.old_ancestors);
  UpdateParentMemo(param.new_ancestors);

  // check quota
  auto old_quota = quota_manager_->GetNearestDirQuota(old_parent);
  auto new_quota = quota_manager_->GetNearestDirQuota(new_parent);
  bool can_rename = (old_quota == nullptr && new_quota == nullptr) ||
                    (old_quota != nullptr && new_quota != nullptr && old_quota->GetIno() == new_quota->GetIno());
  if (!can_rename) {
    return Status(pb::error::ENOT_SUPPORT, "not support rename between quota directory");
  }

  RenameOperation operation(trace, fs_id_, old_parent, old_name, new_parent, new_name);

  auto status = RunOperation(&operation);

  auto& result = operation.GetResult();
  auto& old_parent_attr = result.old_parent_attr;
  auto& new_parent_attr = result.new_parent_attr;
  auto& old_dentry = result.old_dentry;
  auto& prev_new_dentry = result.prev_new_dentry;
  auto& prev_new_attr = result.prev_new_attr;
  auto& new_dentry = result.new_dentry;
  auto& old_attr = result.old_attr;
  bool is_same_parent = result.is_same_parent;
  bool is_exist_new_dentry = result.is_exist_new_dentry;

  DINGO_LOG(INFO) << fmt::format(
      "[fs.{}][{}us] rename {}/{} -> {}/{} finish, state({},{}) version({},{}) "
      "status({}).",
      fs_id_, ElapsedTimeUs(time_ns), old_parent, old_name, new_parent, new_name, is_same_parent, is_exist_new_dentry,
      old_parent_attr.version(), new_parent_attr.version(), status.error_str());

  if (!status.ok()) {
    return status;
  }

  old_parent_version = old_parent_attr.version();
  new_parent_version = new_parent_attr.version();

  if (IsMonoPartition()) {
    // update cache
    PartitionPtr old_parent_partition;
    auto status = GetPartition(ctx, old_parent, old_parent_partition);
    if (status.ok()) {
      // delete old dentry
      old_parent_partition->DeleteChild(old_name);
      // update old parent attr
      old_parent_partition->ParentInode()->UpdateIf(old_parent_attr);
    }

    auto old_inode = GetInodeFromCache(old_attr.ino());
    if (old_inode) old_inode->UpdateIf(old_attr);

    // check new parent dentry/inode
    PartitionPtr new_parent_partition;
    status = GetPartition(ctx, new_parent, new_parent_partition);
    if (status.ok()) {
      // update new parent attr
      new_parent_partition->ParentInode()->UpdateIf(new_parent_attr);

      // delete prev new dentry
      if (is_exist_new_dentry) new_parent_partition->DeleteChild(new_name);

      // add new dentry
      Dentry new_dentry(fs_id_, new_name, new_parent, old_dentry.ino(), old_dentry.type(), 0,
                        GetInodeFromCache(old_dentry.ino()));
      new_parent_partition->PutChild(new_dentry);
    }

    // delete exist new partition
    if (is_exist_new_dentry) {
      if (prev_new_dentry.type() == pb::mdsv2::FileType::DIRECTORY) {
        partition_cache_.Delete(prev_new_dentry.ino());
      } else {
        if (prev_new_attr.nlink() <= 0) {
          DeleteInodeFromCache(prev_new_attr.ino());

        } else {
          auto prev_new_inode = GetInodeFromCache(prev_new_attr.ino());
          if (prev_new_inode) prev_new_inode->UpdateIf(std::move(prev_new_attr));
        }
      }
    }

    // update quota
    if (is_exist_new_dentry) {
      quota_manager_->UpdateFsUsage(0, -1);
      int64_t delta_bytes = old_attr.type() != pb::mdsv2::SYM_LINK ? old_attr.length() : 0;
      quota_manager_->AysncUpdateDirUsage(old_parent, delta_bytes, -1);
    }

  } else {
    // clean partition cache
    NotifyBuddyCleanPartitionCache(old_parent);
    if (!is_same_parent) NotifyBuddyCleanPartitionCache(new_parent);

    // refresh parent of parent inode cache
    NotifyBuddyRefreshInode(std::move(old_parent_attr));
    if (!is_same_parent) NotifyBuddyRefreshInode(std::move(new_parent_attr));
  }

  return Status::OK();
}

Status FileSystem::CommitRename(Context& ctx, const RenameParam& param, Ino& old_parent_version,
                                uint64_t& new_parent_version) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  return renamer_.Execute<RenameParam>(GetSelfPtr(), ctx, param, old_parent_version, new_parent_version);
}

static uint64_t CalculateDeltaLength(uint64_t length, const std::vector<pb::mdsv2::Slice>& slices) {
  uint64_t temp_length = length;
  for (const auto& slice : slices) {
    temp_length = std::max(temp_length, slice.offset() + slice.len());
  }

  return temp_length - length;
}

Status FileSystem::WriteSlice(Context& ctx, Ino parent, Ino ino, uint64_t chunk_index,
                              const std::vector<pb::mdsv2::Slice>& slices) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] writeslice ino({}), chunk_index({}), slice_list.size({}).", fs_id_, ino,
                                  chunk_index, slices.size());

  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  auto& trace = ctx.GetTrace();

  uint64_t time_ns = Helper::TimestampNs();

  // check quota
  uint64_t delta_bytes = CalculateDeltaLength(inode->Length(), slices);
  if (!quota_manager_->CheckQuota(ino, static_cast<int64_t>(delta_bytes), 0)) {
    return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
  }

  // update backend store
  UpsertChunkOperation operation(trace, GetFsInfo(), ino, chunk_index, slices);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] writeslice {}/{} finish, status({}).", fs_id_, ElapsedTimeUs(time_ns),
                                 ino, chunk_index, status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("upsert chunk fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;
  int64_t length_delta = result.length_delta;
  auto& chunk = result.chunk;

  // update cache
  inode->UpdateIf(attr);

  // check whether need to compact chunk
  if (chunk.slices_size() > FLAGS_compact_chunk_threshold_num &&
      chunk.last_compaction_time_ms() + FLAGS_compact_chunk_interval_ms < Helper::TimestampMs()) {
    auto fs_info = fs_info_->Get();
    if (CompactChunkOperation::MaybeCompact(fs_info, ino, attr.length(), chunk)) {
      DINGO_LOG(INFO) << fmt::format("[fs.{}] trigger compact chunk({}) for ino({}).", fs_id_, chunk_index, ino);
      operation_processor_->AsyncRun(CompactChunkOperation::New(fs_info, ino, chunk_index, attr.length()));
    }
  }

  // update chunk cache
  chunk_cache_.PutIf(ino, std::move(chunk));

  // update quota
  quota_manager_->UpdateFsUsage(length_delta, 0);
  for (const auto& parent : attr.parents()) {
    quota_manager_->AysncUpdateDirUsage(parent, length_delta, 0);
  }

  return Status::OK();
}

Status FileSystem::ReadSlice(Context& ctx, Ino ino, uint64_t chunk_index, std::vector<pb::mdsv2::Slice>& slices) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] readslice ino({}), chunk_index({}).", fs_id_, ino, chunk_index);

  if (!ctx.IsBypassCache() && !CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  uint64_t time_ns = Helper::TimestampNs();

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  // get chunk from cache
  auto chunk = chunk_cache_.Get(ino, chunk_index);
  if (chunk != nullptr) {
    slices = Helper::PbRepeatedToVector(chunk->slices());
    return Status::OK();
  }

  // get chunk from backend store
  GetChunkOperation operation(trace, fs_id_, ino, chunk_index);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] readslice {}/{} finish, status({}).", fs_id_, ElapsedTimeUs(time_ns),
                                 ino, chunk_index, status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("get chunk fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();

  slices = Helper::PbRepeatedToVector(result.chunk.slices());

  // update chunk cache
  chunk_cache_.PutIf(ino, std::move(result.chunk));

  return Status::OK();
}

Status FileSystem::Fallocate(Context& ctx, Ino ino, int32_t mode, uint64_t offset, uint64_t len, EntryOut& entry_out) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  uint64_t time_ns = Helper::TimestampNs();

  uint64_t slice_num = 0;
  uint64_t slice_id = 0;
  if (mode == 0 || (mode & FALLOC_FL_ZERO_RANGE)) {
    uint64_t new_length = offset + len;
    if (new_length > inode->Length()) {
      // check quota
      if (!quota_manager_->CheckQuota(ino, new_length - inode->Length(), 0)) {
        return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
      }

      // prealloc slice id
      slice_num = (new_length - inode->Length()) / fs_info_->GetChunkSize() + 1;
      if (!slice_id_generator_->GenID(slice_num, 0, slice_id)) {
        return Status(pb::error::EINTERNAL, "generate slice id fail");
      }
    }
  }

  FallocateOperation::Param param;
  param.fs_id = fs_id_;
  param.ino = ino;
  param.mode = mode;
  param.offset = offset;
  param.len = len;
  param.block_size = fs_info_->GetBlockSize();
  param.chunk_size = fs_info_->GetChunkSize();
  param.slice_id = slice_id;
  param.slice_num = slice_num;

  FallocateOperation operation(trace, param);

  status = RunOperation(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[fs.{}][{}us] fallocate ino({}), mode({}), offset({}), len({}) fail, "
        "status({}).",
        fs_id_, ElapsedTimeUs(time_ns), ino, mode, offset, len, status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;
  auto& effected_chunks = result.effected_chunks;

  inode->UpdateIf(attr);

  entry_out.attr = std::move(attr);

  // update chunk cache
  for (auto& chunk : effected_chunks) {
    chunk_cache_.PutIf(ino, std::move(chunk));
  }

  return Status::OK();
}

Status FileSystem::CompactChunk(Context& ctx, Ino ino, uint64_t chunk_index,
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

  auto& trace = ctx.GetTrace();
  uint64_t time_ns = Helper::TimestampNs();

  CompactChunkOperation operation(trace, GetFsInfo(), ino, chunk_index, inode->Length(), true);

  status = RunOperation(&operation);

  auto& result = operation.GetResult();
  auto& effected_chunk = result.effected_chunk;
  trash_slices = Helper::PbRepeatedToVector(result.trash_slice_list.mutable_slices());

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] compactchunk {}/{} finish, trash_slices({}) status({}).", fs_id_,
                                 ElapsedTimeUs(time_ns), ino, chunk_index, trash_slices.size(), status.error_str());
  if (!status.ok()) {
    return status;
  }

  // update chunk cache
  chunk_cache_.PutIf(ino, std::move(effected_chunk));

  return Status::OK();
}

Status FileSystem::GetDentry(Context& ctx, Ino parent, const std::string& name, Dentry& dentry) {
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

Status FileSystem::ListDentry(Context& ctx, Ino parent, const std::string& last_name, uint32_t limit, bool is_only_dir,
                              std::vector<Dentry>& dentries) {
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

Status FileSystem::GetInode(Context& ctx, Ino ino, EntryOut& entry_out) {
  DINGO_LOG(DEBUG) << fmt::format("[fs.{}] getinode ino({}).", fs_id_, ino);

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  entry_out.attr = inode->Copy();

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
      entry_out.attr = inode->Copy();
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
      entry_out.attr = inode->Copy();
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
    for (auto& [k, v] : inode->XAttrs()) {
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

void FileSystem::RefreshInode(AttrType& attr) {
  auto inode = inode_cache_.GetInode(attr.ino());
  if (inode != nullptr) {
    inode->UpdateIf(attr);
  }
}

Status FileSystem::RefreshFsInfo() { return RefreshFsInfo(fs_info_->GetName()); }

Status FileSystem::RefreshFsInfo(const std::string& name) {
  DINGO_LOG(INFO) << fmt::format("[fs.{}] refresh fs({}) info.", fs_id_, name);

  Trace trace;
  GetFsOperation operation(trace, name);

  auto status = RunOperation(&operation);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("get fs info fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();

  RefreshFsInfo(result.fs_info);

  return Status::OK();
}

static std::set<uint32_t> GetDeletedBucketIds(int64_t mds_id, const pb::mdsv2::HashPartition& old_hash,
                                              const pb::mdsv2::HashPartition& hash) {
  std::set<uint32_t> deleted_bucket_ids;
  auto old_bucketset = old_hash.distributions().find(mds_id);
  auto bucketset = hash.distributions().find(mds_id);
  if (old_bucketset == old_hash.distributions().end() || bucketset == hash.distributions().end()) {
    DINGO_LOG(ERROR) << fmt::format("[fs] mds_id({}) not found in old or new hash partition.", mds_id);
    return deleted_bucket_ids;
  }

  const auto& old_bucket_ids = old_bucketset->second.bucket_ids();
  const auto& new_bucket_ids = bucketset->second.bucket_ids();
  for (const auto& old_bucket_id : old_bucket_ids) {
    if (std::find(new_bucket_ids.begin(), new_bucket_ids.end(), old_bucket_id) == new_bucket_ids.end()) {  // NOLINT
      deleted_bucket_ids.insert(old_bucket_id);
    }
  }

  return deleted_bucket_ids;
}

void FileSystem::RefreshFsInfo(const FsInfoType& fs_info) {
  // clean partition and inode cache
  auto pre_handler = [&](const FsInfoType& old_fs_info, const FsInfoType& new_fs_info) {
    const auto& partition_policy = new_fs_info.partition_policy();
    if (partition_policy.type() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
      if (partition_policy.mono().mds_id() != self_mds_id_) {
        ClearCache();
      }

    } else if (partition_policy.type() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
      auto old_hash = old_fs_info.partition_policy().parent_hash();
      auto new_hash = new_fs_info.partition_policy().parent_hash();
      if (!new_hash.distributions().contains(self_mds_id_)) {
        ClearCache();

      } else {
        BatchDeleteCache(new_hash.bucket_num(), GetDeletedBucketIds(self_mds_id_, old_hash, new_hash));
      }
    }
  };

  Duration duration;

  fs_info_->Update(fs_info, pre_handler);

  can_serve_.store(CanServe(self_mds_id_), std::memory_order_release);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] update fs({}) can_serve({}).", fs_id_, duration.ElapsedUs(),
                                 fs_info.fs_name(), can_serve_ ? "true" : "false");
}

Status FileSystem::JoinMonoFs(Context& ctx, uint64_t mds_id, const std::string& reason) {
  if (PartitionType() != pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    return Status(pb::error::ENOT_SUPPORT, "not support join fs for hash partition");
  }

  int64_t old_mds_id = 0;
  auto handler = [&](PartitionPolicy& partition_policy, FsOpLog& log) -> Status {
    auto* mono = partition_policy.mutable_mono();
    if (mono->mds_id() == mds_id) {
      return Status(pb::error::EEXISTED, "mds already exist");
    }
    old_mds_id = mono->mds_id();

    mono->set_mds_id(mds_id);

    log.set_fs_name(FsName());
    log.set_fs_id(fs_id_);
    log.set_type(pb::mdsv2::FsOpLog::JOIN_FS);
    log.mutable_join_fs()->add_mds_ids(mds_id);
    log.set_comment(reason);

    return Status::OK();
  };

  auto& trace = ctx.GetTrace();
  UpdateFsPartitionOperation operation(trace, FsName(), handler);

  auto status = RunOperation(&operation);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, fmt::format("update fs partition policy fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();

  fs_info_->Update(result.fs_info);

  NotifyBuddyRefreshFsInfo({old_mds_id}, result.fs_info);

  return Status::OK();
}

static std::vector<int64_t> GetMdsIdFromHashPartitioin(const pb::mdsv2::HashPartition& hash) {
  std::vector<int64_t> mds_ids;
  mds_ids.reserve(hash.distributions_size());
  for (const auto& [mds_id, _] : hash.distributions()) {
    mds_ids.push_back(mds_id);
  }

  return mds_ids;
}

// check whether the hash partition is valid
// 1. bucket_ids should be unique
// 2. bucket_ids should be in range [0, bucket_num)
// 3. bucket_ids size should be equal to bucket_num
// 4. bucket_ids should not be empty
// 5. bucket_num should be greater than 0
static bool CheckHashPartition(const pb::mdsv2::HashPartition& hash) {
  if (hash.bucket_num() == 0) {
    DINGO_LOG(ERROR) << "[fs] bucket_num should be greater than 0.";
    return false;
  }

  std::set<uint32_t> bucket_ids;
  for (const auto& [mds_id, bucket_set] : hash.distributions()) {
    if (bucket_set.bucket_ids().empty()) {
      DINGO_LOG(ERROR) << fmt::format("[fs] bucket_ids should not be empty for mds_id({}).", mds_id);
      return false;
    }

    for (const auto& bucket_id : bucket_set.bucket_ids()) {
      if (bucket_id >= hash.bucket_num()) {
        DINGO_LOG(ERROR) << fmt::format("[fs] bucket_id({}) should be in range [0, {}).", bucket_id, hash.bucket_num());
        return false;
      }

      if (bucket_ids.count(bucket_id) > 0) {
        DINGO_LOG(ERROR) << fmt::format("[fs] bucket_id({}) should be unique.", bucket_id);
        return false;
      }

      bucket_ids.insert(bucket_id);
    }
  }

  if (bucket_ids.size() != hash.bucket_num()) {
    DINGO_LOG(ERROR) << fmt::format("[fs] bucket_ids size({}) should be equal to bucket_num({}).", bucket_ids.size(),
                                    hash.bucket_num());
    return false;
  }

  return true;
}

// 按bucket_id数量平均分布
static void DistributeByMean(const std::vector<uint64_t>& mds_ids, pb::mdsv2::HashPartition& hash) {
  uint32_t mean_num = hash.bucket_num() / (hash.distributions_size() + mds_ids.size());

  std::vector<uint64_t> pending_bucket_ids;
  pending_bucket_ids.reserve(mean_num * mds_ids.size());
  for (auto& [_, bucket_set] : *hash.mutable_distributions()) {
    for (uint32_t i = mean_num; i < bucket_set.bucket_ids_size(); ++i) {
      pending_bucket_ids.push_back(bucket_set.bucket_ids(i));
    }
    if (bucket_set.bucket_ids_size() > mean_num) {
      bucket_set.mutable_bucket_ids()->Resize(mean_num, 0);
    }
  }

  uint32_t pending_offset = 0;
  for (size_t i = 0; i < mds_ids.size(); ++i) {
    pb::mdsv2::HashPartition::BucketSet bucket_set;
    while (pending_offset < pending_bucket_ids.size()) {
      bucket_set.add_bucket_ids(pending_bucket_ids[pending_offset++]);
      if ((i + 1) < mds_ids.size() && bucket_set.bucket_ids_size() >= mean_num) break;
    }

    // sort bucket id
    std::sort(bucket_set.mutable_bucket_ids()->begin(), bucket_set.mutable_bucket_ids()->end());

    hash.mutable_distributions()->insert({mds_ids[i], std::move(bucket_set)});
  }
}

Status FileSystem::JoinHashFs(Context& ctx, const std::vector<uint64_t>& mds_ids, const std::string& reason) {
  if (PartitionType() != pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    return Status(pb::error::ENOT_SUPPORT, "not support join fs for mono partition");
  }

  auto has_mds_fn = [&](const pb::mdsv2::HashPartition& hash) -> bool {
    for (const auto& mds_id : mds_ids) {
      if (hash.distributions().find(mds_id) != hash.distributions().end()) {
        return true;
      }
    }

    return false;
  };

  std::vector<int64_t> old_mds_ids;
  auto handler = [&](PartitionPolicy& partition_policy, FsOpLog& log) -> Status {
    auto* hash = partition_policy.mutable_parent_hash();

    if (has_mds_fn(*hash)) {
      return Status(pb::error::EEXISTED, "mds already exists");
    }

    old_mds_ids = GetMdsIdFromHashPartitioin(*hash);

    DistributeByMean(mds_ids, *hash);

    CHECK(CheckHashPartition(*hash)) << "invalid hash partition bucket id size.";

    log.set_fs_name(FsName());
    log.set_fs_id(fs_id_);
    log.set_type(pb::mdsv2::FsOpLog::JOIN_FS);
    Helper::VectorToPbRepeated(mds_ids, log.mutable_join_fs()->mutable_mds_ids());
    log.set_comment(reason);

    return Status::OK();
  };

  auto& trace = ctx.GetTrace();
  UpdateFsPartitionOperation operation(trace, FsName(), handler);

  auto status = RunOperation(&operation);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, fmt::format("update fs partition policy fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();

  fs_info_->Update(result.fs_info);

  NotifyBuddyRefreshFsInfo(old_mds_ids, result.fs_info);

  return Status::OK();
}

// 把准备退出的mds负责的bucket_id平均分配给剩余mds
static void ReDistributeByDeleteMds(const std::vector<uint64_t>& quit_mds_ids, pb::mdsv2::HashPartition& hash) {
  std::vector<uint64_t> pending_bucket_ids;
  for (const auto& mds_id : quit_mds_ids) {
    auto it = hash.distributions().find(mds_id);
    if (it == hash.distributions().end()) {
      continue;  // not found, skip
    }

    const auto& bucket_set = it->second;

    pending_bucket_ids.insert(pending_bucket_ids.end(), bucket_set.bucket_ids().begin(), bucket_set.bucket_ids().end());

    hash.mutable_distributions()->erase(mds_id);
  }

  if (hash.distributions().empty()) return;

  const uint32_t mean_num = pending_bucket_ids.size() / hash.distributions_size();
  uint32_t pending_offset = 0;
  for (auto& [_, bucket_set] : *hash.mutable_distributions()) {
    for (uint32_t i = 0; i < mean_num; ++i) {
      bucket_set.add_bucket_ids(pending_bucket_ids[pending_offset++]);
    }
  }

  for (uint32_t i = pending_offset; i < pending_bucket_ids.size(); ++i) {
    hash.mutable_distributions()->begin()->second.add_bucket_ids(pending_bucket_ids[i]);
  }

  // sort bucket id
  for (auto& [_, bucket_set] : *hash.mutable_distributions()) {
    std::sort(bucket_set.mutable_bucket_ids()->begin(), bucket_set.mutable_bucket_ids()->end());
  }
}

Status FileSystem::QuitFs(Context& ctx, const std::vector<uint64_t>& mds_ids, const std::string& reason) {
  if (PartitionType() != pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    return Status(pb::error::ENOT_SUPPORT, "not support join fs for mono partition");
  }

  auto miss_mds_fn = [&](const pb::mdsv2::HashPartition& hash) -> bool {
    for (const auto& mds_id : mds_ids) {
      if (hash.distributions().find(mds_id) != hash.distributions().end()) {
        return false;
      }
    }

    return true;
  };

  std::vector<int64_t> old_mds_ids;
  auto handler = [&](PartitionPolicy& partition_policy, FsOpLog& log) -> Status {
    auto* hash = partition_policy.mutable_parent_hash();

    if (hash->distributions_size() <= 1) {
      return Status(pb::error::EINTERNAL, "not enough mds");
    }
    if (miss_mds_fn(*hash)) {
      return Status(pb::error::ENOT_FOUND, "not found mds");
    }

    old_mds_ids = GetMdsIdFromHashPartitioin(*hash);

    ReDistributeByDeleteMds(mds_ids, *hash);

    CHECK(CheckHashPartition(*hash)) << "invalid hash partition bucket id size.";

    log.set_fs_name(FsName());
    log.set_fs_id(fs_id_);
    log.set_type(pb::mdsv2::FsOpLog::QUIT_FS);
    Helper::VectorToPbRepeated(mds_ids, log.mutable_quit_fs()->mutable_mds_ids());
    log.set_comment(reason);

    return Status::OK();
  };

  auto& trace = ctx.GetTrace();
  UpdateFsPartitionOperation operation(trace, FsName(), handler);

  auto status = RunOperation(&operation);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, fmt::format("update fs partition policy fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();

  fs_info_->Update(result.fs_info);

  NotifyBuddyRefreshFsInfo(old_mds_ids, result.fs_info);

  return Status::OK();
}

Status FileSystem::QuitAndJoinFs(Context& ctx, const std::vector<uint64_t>& quit_mds_ids,
                                 const std::vector<uint64_t>& join_mds_ids, const std::string& reason) {
  if (PartitionType() != pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    return Status(pb::error::ENOT_SUPPORT, "not support join fs for mono partition");
  }

  auto miss_mds_fn = [&](const pb::mdsv2::HashPartition& hash) -> bool {
    for (const auto& mds_id : quit_mds_ids) {
      if (hash.distributions().find(mds_id) != hash.distributions().end()) {
        return false;
      }
    }

    return true;
  };

  auto has_mds_fn = [&](const pb::mdsv2::HashPartition& hash) -> bool {
    for (const auto& mds_id : join_mds_ids) {
      if (hash.distributions().find(mds_id) != hash.distributions().end()) {
        return true;
      }
    }

    return false;
  };

  std::vector<int64_t> old_mds_ids;
  auto handler = [&](PartitionPolicy& partition_policy, FsOpLog& log) -> Status {
    auto* hash = partition_policy.mutable_parent_hash();

    if (hash->distributions_size() <= 1) {
      return Status(pb::error::EINTERNAL, "not enough mds");
    }
    if (miss_mds_fn(*hash)) {
      return Status(pb::error::ENOT_FOUND, "not found mds");
    }
    if (has_mds_fn(*hash)) {
      return Status(pb::error::EEXISTED, "mds already exists");
    }

    old_mds_ids = GetMdsIdFromHashPartitioin(*hash);

    ReDistributeByDeleteMds(quit_mds_ids, *hash);

    CHECK(CheckHashPartition(*hash)) << "invalid hash partition bucket id size.";

    DistributeByMean(join_mds_ids, *hash);

    CHECK(CheckHashPartition(*hash)) << "invalid hash partition bucket id size.";

    log.set_fs_name(FsName());
    log.set_fs_id(fs_id_);
    log.set_type(pb::mdsv2::FsOpLog::QUIT_AND_JOIN_FS);
    Helper::VectorToPbRepeated(quit_mds_ids, log.mutable_quit_and_join_fs()->mutable_quit_mds_ids());
    Helper::VectorToPbRepeated(join_mds_ids, log.mutable_quit_and_join_fs()->mutable_join_mds_ids());
    log.set_comment(reason);

    return Status::OK();
  };

  auto& trace = ctx.GetTrace();
  UpdateFsPartitionOperation operation(trace, FsName(), handler);

  auto status = RunOperation(&operation);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, fmt::format("update fs partition policy fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();

  fs_info_->Update(result.fs_info);

  NotifyBuddyRefreshFsInfo(old_mds_ids, result.fs_info);

  return Status::OK();
}

Status FileSystem::UpdatePartitionPolicy(const std::map<uint64_t, pb::mdsv2::HashPartition::BucketSet>& distributions) {
  std::string key = MetaCodec::EncodeFsKey(fs_info_->GetName());

  auto txn = kv_storage_->NewTxn();
  std::string value;
  auto status = txn->Get(key, value);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("get fs info fail, {}", status.error_str()));
  }

  FsInfoType fs_info = MetaCodec::DecodeFsValue(value);
  CHECK(fs_info.partition_policy().type() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION)
      << "invalid partition polocy type.";

  fs_info.mutable_partition_policy()->set_epoch(fs_info.partition_policy().epoch() + 1);
  auto* hash = fs_info.mutable_partition_policy()->mutable_parent_hash();
  hash->mutable_distributions()->clear();
  for (const auto& [mds_id, bucket_set] : distributions) {
    hash->mutable_distributions()->insert({mds_id, bucket_set});
  }

  fs_info.set_last_update_time_ns(Helper::TimestampNs());

  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, key, MetaCodec::EncodeFsValue(fs_info));
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

Status FileSystem::GetDelFiles(std::vector<AttrType>& delfiles) {
  Trace trace;
  uint32_t count = 0;
  ScanDelFileOperation operation(trace, fs_id_, [&](const std::string&, const std::string& value) -> bool {
    delfiles.push_back(MetaCodec::DecodeDelFileValue(value));
    ++count;
    return true;
  });

  return RunOperation(&operation);
}

Status FileSystem::GetDelSlices(std::vector<TrashSliceList>& delslices) {
  Trace trace;
  uint32_t count = 0;
  ScanDelSliceOperation operation(trace, fs_id_, [&](const std::string&, const std::string& value) -> bool {
    delslices.push_back(MetaCodec::DecodeDelSliceValue(value));
    ++count;
    return true;
  });

  return RunOperation(&operation);
}

Status FileSystem::GetFsOpLogs(std::vector<FsOpLog>& fs_op_logs) {
  Trace trace;
  uint32_t count = 0;
  ScanFsOpLogOperation operation(trace, fs_id_, [&](const FsOpLog& oplog) -> bool {
    fs_op_logs.push_back(oplog);
    ++count;
    return true;
  });

  return RunOperation(&operation);
}

FileSystemSet::FileSystemSet(CoordinatorClientSPtr coordinator_client, IdGeneratorUPtr fs_id_generator,
                             IdGeneratorSPtr slice_id_generator, KVStorageSPtr kv_storage, MDSMeta self_mds_meta,
                             MDSMetaMapSPtr mds_meta_map, OperationProcessorSPtr operation_processor,
                             WorkerSetSPtr quota_worker_set, notify::NotifyBuddySPtr notify_buddy)
    : coordinator_client_(coordinator_client),
      fs_id_generator_(std::move(fs_id_generator)),
      slice_id_generator_(slice_id_generator),
      kv_storage_(kv_storage),
      self_mds_meta_(self_mds_meta),
      mds_meta_map_(mds_meta_map),
      operation_processor_(operation_processor),
      quota_worker_set_(quota_worker_set),
      notify_buddy_(notify_buddy) {}

FileSystemSet::~FileSystemSet() {}  // NOLINT

bool FileSystemSet::Init() {
  CHECK(coordinator_client_ != nullptr) << "coordinator client is null.";
  CHECK(kv_storage_ != nullptr) << "kv_storage is null.";
  CHECK(mds_meta_map_ != nullptr) << "mds_meta_map is null.";
  CHECK(operation_processor_ != nullptr) << "operation_processor is null.";

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

Status FileSystemSet::GenFsId(uint32_t& fs_id) {
  uint64_t temp_fs_id;
  bool ret = fs_id_generator_->GenID(2, temp_fs_id);
  fs_id = static_cast<uint32_t>(temp_fs_id);
  return ret ? Status::OK() : Status(pb::error::EGEN_FSID, "generate fs id fail");
}

// gerenate parent hash partition
static std::map<uint64_t, pb::mdsv2::HashPartition::BucketSet> GenParentHashDistribution(
    const std::vector<MDSMeta>& mds_metas, uint32_t bucket_num) {
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

FsInfoType FileSystemSet::GenFsInfo(int64_t fs_id, const CreateFsParam& param) {
  FsInfoType fs_info;
  fs_info.set_fs_id(fs_id);
  fs_info.set_fs_name(param.fs_name);
  fs_info.set_fs_type(param.fs_type);
  fs_info.set_status(::dingofs::pb::mdsv2::FsStatus::NEW);
  fs_info.set_block_size(param.block_size);
  fs_info.set_chunk_size(param.chunk_size);
  fs_info.set_enable_sum_in_dir(param.enable_sum_in_dir);
  fs_info.set_owner(param.owner);
  fs_info.set_capacity(param.capacity);
  fs_info.set_recycle_time_hour(param.recycle_time_hour);
  fs_info.mutable_extra()->CopyFrom(param.fs_extra);
  fs_info.set_uuid(utils::UUIDGenerator::GenerateUUID());

  auto mds_metas = mds_meta_map_->GetAllMDSMeta();
  auto* partition_policy = fs_info.mutable_partition_policy();
  partition_policy->set_type(param.partition_type);
  partition_policy->set_epoch(1);
  if (param.partition_type == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    auto* mono = partition_policy->mutable_mono();
    int select_offset = Helper::GenerateRealRandomInteger(0, 1000) % mds_metas.size();
    mono->set_mds_id(mds_metas.at(select_offset).ID());

  } else if (param.partition_type == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    auto* parent_hash = partition_policy->mutable_parent_hash();
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

bool FileSystemSet::IsExistFsTable() {
  auto range = MetaCodec::GetMetaTableRange();
  DINGO_LOG(DEBUG) << fmt::format("[fsset] check fs table, {}.", range.ToString());

  auto status = kv_storage_->IsExistTable(range.start, range.end);
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      DINGO_LOG(ERROR) << "[fsset] check fs table exist fail, error: " << status.error_str();
    }
    return false;
  }

  return true;
}

static Status ValidateCreateFsParam(const FileSystemSet::CreateFsParam& param) {
  if (param.fs_name.empty()) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "fs name is empty");
  }

  if (param.block_size == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "block size is zero");
  }

  if (param.chunk_size == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "chunk size is zero");
  }

  return Status::OK();
}

// todo: create fs/dentry/inode table
Status FileSystemSet::CreateFs(const CreateFsParam& param, FsInfoType& fs_info) {
  auto status = ValidateCreateFsParam(param);
  if (!status.ok()) {
    return status;
  }

  // when create fs fail, clean up
  auto cleanup = [&](int64_t dentry_table_id, const std::string& fs_key, const std::string& quota_key) {
    // clean dentry table
    if (dentry_table_id > 0) {
      auto status = kv_storage_->DropTable(dentry_table_id);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("[fsset] clean dentry table({}) fail, error: {}", dentry_table_id,
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

    // clean quota
    if (!quota_key.empty()) {
      auto status = kv_storage_->Delete(quota_key);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("[fsset] clean quota info fail, error: {}", status.error_str());
      }
    }
  };

  std::string fs_key = MetaCodec::EncodeFsKey(param.fs_name);
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

  // generate fs id
  uint32_t fs_id = 0;
  status = GenFsId(fs_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  // create dentry/inode table
  int64_t dentry_table_id = 0;
  {
    auto range = MetaCodec::GetFsMetaTableRange(fs_id);
    KVStorage::TableOption option = {.start_key = range.start, .end_key = range.end};
    std::string table_name = GenFsMetaTableName(param.fs_name);
    Status status = kv_storage_->CreateTable(table_name, option, dentry_table_id);
    if (!status.ok()) {
      return Status(pb::error::EINTERNAL, fmt::format("create dentry table fail, {}", status.error_str()));
    }
  }

  fs_info = GenFsInfo(fs_id, param);

  // create fs
  Trace trace;
  CreateFsOperation operation(trace, fs_info);
  status = RunOperation(&operation);
  if (!status.ok()) {
    cleanup(dentry_table_id, "", "");
    return Status(pb::error::EBACKEND_STORE, fmt::format("put store fs fail, {}", status.error_str()));
  }

  // create FileSystem instance
  auto ino_id_generator = NewInodeIdGenerator(fs_id, coordinator_client_, kv_storage_);
  CHECK(ino_id_generator != nullptr) << "new id generator fail.";

  auto fs =
      FileSystem::New(self_mds_meta_.ID(), FsInfo::NewUnique(fs_info), std::move(ino_id_generator), slice_id_generator_,
                      kv_storage_, operation_processor_, mds_meta_map_, quota_worker_set_, notify_buddy_);
  if (!fs->Init()) {
    cleanup(dentry_table_id, fs_key, "");
    return Status(pb::error::EINTERNAL, "init FileSystem fail");
  }

  // set quota
  status = fs->CreateQuota();
  if (!status.ok()) {
    cleanup(dentry_table_id, fs_key, "");
    return Status(pb::error::EINTERNAL, fmt::format("create quota fail, {}", status.error_str()));
  }

  // create root inode
  status = fs->CreateRoot();
  if (!status.ok()) {
    cleanup(dentry_table_id, fs_key, MetaCodec::EncodeFsQuotaKey(fs_id));
    return Status(pb::error::EINTERNAL, fmt::format("create root fail, {}", status.error_str()));
  }

  CHECK(AddFileSystem(fs, false)) << fmt::format("add FileSystem({}) fail.", fs->FsId());

  return Status::OK();
}

Status FileSystemSet::MountFs(Context& ctx, const std::string& fs_name, const pb::mdsv2::MountPoint& mountpoint) {
  CHECK(!fs_name.empty()) << "fs name is empty.";

  auto& trace = ctx.GetTrace();

  MountFsOperation operation(trace, fs_name, mountpoint);

  auto status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fsset] mount fs({}) to {} finish, status({}).", fs_name,
                                 mountpoint.ShortDebugString(), status.error_str());

  return status;
}

Status FileSystemSet::UmountFs(Context& ctx, const std::string& fs_name, const std::string& client_id) {
  CHECK(!fs_name.empty()) << "fs name is empty.";

  auto& trace = ctx.GetTrace();

  UmountFsOperation operation(trace, fs_name, client_id);

  auto status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fsset] umount fs({}) to {} finish, status({}).", fs_name, client_id,
                                 status.error_str());

  return status;
}

// check if fs is mounted
Status FileSystemSet::DeleteFs(Context& ctx, const std::string& fs_name, bool is_force) {
  CHECK(!fs_name.empty()) << "fs name is empty.";

  auto& trace = ctx.GetTrace();

  DeleteFsOperation operation(trace, fs_name, is_force);

  auto status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fsset] delete fs({}) finish, status({}).", fs_name, status.error_str());

  auto& result = operation.GetResult();
  auto& fs_info = result.fs_info;

  if (status.ok()) {
    DeleteFileSystem(fs_info.fs_id());
  }

  return status;
}

Status FileSystemSet::UpdateFsInfo(Context& ctx, const std::string& fs_name, const FsInfoType& fs_info) {
  auto trace = ctx.GetTrace();

  UpdateFsOperation operation(trace, fs_name, fs_info);

  auto status = RunOperation(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[fsset] update fs({}) info fail, status({}).", fs_name, status.error_str());
    return status;
  }

  return Status::OK();
}

Status FileSystemSet::GetFsInfo(Context& ctx, const std::string& fs_name, FsInfoType& fs_info) {
  auto& trace = ctx.GetTrace();

  std::string fs_key = MetaCodec::EncodeFsKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}), {}.", fs_name, status.error_str()));
  }

  trace.RecordElapsedTime("store_operate");

  fs_info = MetaCodec::DecodeFsValue(value);

  return Status::OK();
}

Status FileSystemSet::GetAllFsInfo(Context& ctx, std::vector<FsInfoType>& fs_infoes) {
  auto& trace = ctx.GetTrace();

  ScanFsOperation operation(trace);

  auto status = RunOperation(&operation);
  if (!status.ok()) return status;

  auto& all_fs_infoes = operation.GetResult().fs_infoes;
  for (const auto& fs_info : all_fs_infoes) {
    if (!fs_info.is_deleted()) {
      fs_infoes.push_back(fs_info);
    }
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

Status FileSystemSet::AllocSliceId(uint32_t num, uint64_t min_slice_id, uint64_t& slice_id) {
  if (!slice_id_generator_->GenID(num, min_slice_id, slice_id)) {
    return Status(pb::error::EINTERNAL, "generate slice id fail");
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

std::string FileSystemSet::GetFsName(const std::string& client_id) {
  utils::ReadLockGuard lk(lock_);

  for (auto& [fs_id, fs] : fs_map_) {
    auto fs_info = fs->GetFsInfo();
    for (const auto& mountpoint : fs_info.mount_points()) {
      if (mountpoint.client_id() == client_id) {
        return fs->FsName();
      }
    }
  }

  return "";  // not found
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

Status FileSystemSet::CheckMdsNormal(const std::vector<uint64_t>& mds_ids) {
  for (const auto& mds_id : mds_ids) {
    if (!mds_meta_map_->IsNormalMDSMeta(mds_id)) {
      return Status(pb::error::ENOT_FOUND, fmt::format("mds({}) is not normal", mds_id));
    }
  }

  return Status::OK();
}

Status FileSystemSet::JoinFs(Context& ctx, uint32_t fs_id, const std::vector<uint64_t>& mds_ids,
                             const std::string& reason) {
  auto fs = GetFileSystem(fs_id);
  if (fs == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({})", fs_id));
  }

  auto status = CheckMdsNormal(mds_ids);
  if (!status.ok()) return status;

  return JoinFs(ctx, fs->FsName(), mds_ids, reason);
}

Status FileSystemSet::JoinFs(Context& ctx, const std::string& fs_name, const std::vector<uint64_t>& mds_ids,
                             const std::string& reason) {
  auto fs = GetFileSystem(fs_name);
  if (fs == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({})", fs_name));
  }

  auto status = CheckMdsNormal(mds_ids);
  if (!status.ok()) return status;

  if (fs->PartitionType() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    if (mds_ids.size() > 1) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "not support join mono fs with multiple mds");
    }

    return fs->JoinMonoFs(ctx, mds_ids.front(), reason);

  } else if (fs->PartitionType() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    return fs->JoinHashFs(ctx, mds_ids, reason);
  }

  return Status::OK();
}

Status FileSystemSet::QuitFs(Context& ctx, uint32_t fs_id, const std::vector<uint64_t>& mds_ids,
                             const std::string& reason) {
  auto fs = GetFileSystem(fs_id);
  if (fs == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({})", fs_id));
  }

  return QuitFs(ctx, fs->FsName(), mds_ids, reason);
}

Status FileSystemSet::QuitFs(Context& ctx, const std::string& fs_name, const std::vector<uint64_t>& mds_ids,
                             const std::string& reason) {
  auto fs = GetFileSystem(fs_name);
  if (fs == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({})", fs_name));
  }

  if (fs->PartitionType() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "not support mono fs quit fs");

  } else if (fs->PartitionType() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    return fs->QuitFs(ctx, mds_ids, reason);

  } else {
    return Status(pb::error::ENOT_SUPPORT, reason);
  }

  return Status::OK();
}

bool FileSystemSet::LoadFileSystems() {
  Range range = MetaCodec::GetFsRange();

  // scan fs table from kv storage
  std::vector<KeyValue> kvs;
  auto status = kv_storage_->Scan(range, kvs);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[fsset] scan fs table fail, error: {}.", status.error_str());
    return false;
  }

  for (const auto& kv : kvs) {
    auto fs_info = MetaCodec::DecodeFsValue(kv.value);

    auto fs = GetFileSystem(fs_info.fs_id());
    if (fs == nullptr) {
      DINGO_LOG(INFO) << fmt::format("[fsset] add fs name({}) id({}).", fs_info.fs_name(), fs_info.fs_id());

      auto ino_id_generator = NewInodeIdGenerator(fs_info.fs_id(), coordinator_client_, kv_storage_);
      CHECK(ino_id_generator != nullptr) << "new id generator fail.";

      fs = FileSystem::New(self_mds_meta_.ID(), FsInfo::NewUnique(fs_info), std::move(ino_id_generator),
                           slice_id_generator_, kv_storage_, operation_processor_, mds_meta_map_, quota_worker_set_,
                           notify_buddy_);
      if (!fs->Init()) {
        DINGO_LOG(ERROR) << fmt::format("[fsset] init FileSystem({}) fail.", fs_info.fs_id());
        continue;
      }

      CHECK(AddFileSystem(fs)) << fmt::format("add FileSystem({}) fail.", fs->FsId());

    } else {
      fs->RefreshFsInfo(fs_info);
    }
  }

  return true;
}

Status FileSystemSet::RunOperation(Operation* operation) {
  CHECK(operation != nullptr) << "operation is null.";

  if (!operation->IsBatchRun()) {
    return operation_processor_->RunAlone(operation);
  }

  bthread::CountdownEvent count_down(1);

  operation->SetEvent(&count_down);

  if (!operation_processor_->RunBatched(operation)) {
    return Status(pb::error::EINTERNAL, "commit mutation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  return operation->GetResult().status;
}

}  // namespace mdsv2
}  // namespace dingofs
