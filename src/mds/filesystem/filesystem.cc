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

#include "mds/filesystem/filesystem.h"

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

#include "brpc/reloadable_flags.h"
#include "butil/status.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "json/value.h"
#include "mds/common/codec.h"
#include "mds/common/constant.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/common/partition_helper.h"
#include "mds/common/status.h"
#include "mds/common/time.h"
#include "mds/common/tracing.h"
#include "mds/common/type.h"
#include "mds/filesystem/dentry.h"
#include "mds/filesystem/file_session.h"
#include "mds/filesystem/fs_info.h"
#include "mds/filesystem/inode.h"
#include "mds/filesystem/notify_buddy.h"
#include "mds/filesystem/store_operation.h"
#include "mds/mds/mds_helper.h"
#include "mds/mds/mds_meta.h"
#include "mds/storage/storage.h"
#include "utils/uuid.h"

namespace dingofs {
namespace mds {

static const std::string kFsTableName = "dingofs";

static const std::string kStatsName = ".stats";
static const std::string kRecyleName = ".recycle";

DEFINE_uint32(mds_filesystem_name_max_size, 1024, "Max size of filesystem name.");
DEFINE_validator(mds_filesystem_name_max_size, brpc::PassValidate);
DEFINE_uint32(mds_filesystem_hash_bucket_num, 1024, "Filesystem hash bucket num.");
DEFINE_validator(mds_filesystem_hash_bucket_num, brpc::PassValidate);
DEFINE_uint32(mds_filesystem_hash_mds_num_default, 3, "Filesystem hash mds num.");
DEFINE_validator(mds_filesystem_hash_mds_num_default, brpc::PassValidate);
DEFINE_uint32(mds_filesystem_recycle_time_hour, 1, "Filesystem recycle time hour.");
DEFINE_validator(mds_filesystem_recycle_time_hour, brpc::PassValidate);

DEFINE_bool(mds_compact_chunk_enable, true, "Compact chunk enable.");
DEFINE_validator(mds_compact_chunk_enable, brpc::PassValidate);
DEFINE_uint32(mds_compact_chunk_threshold_num, 10, "Compact chunk threshold num.");
DEFINE_validator(mds_compact_chunk_threshold_num, brpc::PassValidate);
DEFINE_uint32(mds_compact_chunk_interval_ms, 3 * 1000, "Compact chunk interval ms.");
DEFINE_validator(mds_compact_chunk_interval_ms, brpc::PassValidate);

DEFINE_uint32(mds_transfer_max_slice_num, 8096, "Max slice num for transfer.");
DEFINE_validator(mds_transfer_max_slice_num, brpc::PassValidate);

static bool IsInvalidName(const std::string& name) {
  return name.empty() || name.size() > FLAGS_mds_filesystem_name_max_size;
}

FileSystem::FileSystem(uint64_t self_mds_id, FsInfoSPtr fs_info, IdGeneratorUPtr ino_id_generator,
                       IdGeneratorSPtr slice_id_generator, KVStorageSPtr kv_storage,
                       OperationProcessorSPtr operation_processor, MDSMetaMapSPtr mds_meta_map,
                       WorkerSetSPtr quota_worker_set, notify::NotifyBuddySPtr notify_buddy)
    : self_mds_id_(self_mds_id),
      fs_info_(fs_info),
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
      quota_manager_(
          quota::QuotaManager::New(fs_info, parent_memo_, operation_processor, quota_worker_set, notify_buddy)),
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

pb::mds::PartitionType FileSystem::PartitionType() const { return fs_info_->GetPartitionType(); }

bool FileSystem::IsMonoPartition() const {
  return fs_info_->GetPartitionType() == pb::mds::PartitionType::MONOLITHIC_PARTITION;
}
bool FileSystem::IsParentHashPartition() const {
  return fs_info_->GetPartitionType() == pb::mds::PartitionType::PARENT_ID_HASH_PARTITION;
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

bool FileSystem::CanServe(uint64_t self_mds_id) {
  const auto& partition_policy = fs_info_->GetPartitionPolicy();
  if (partition_policy.type() == pb::mds::PartitionType::MONOLITHIC_PARTITION) {
    return partition_policy.mono().mds_id() == self_mds_id;

  } else if (partition_policy.type() == pb::mds::PartitionType::PARENT_ID_HASH_PARTITION) {
    return partition_policy.parent_hash().distributions().contains(self_mds_id);
  }

  return false;
}

Status FileSystem::GetPartitionParentInode(Context& ctx, PartitionPtr& partition, InodeSPtr& out_inode) {
  auto inode = partition->ParentInode();
  if (inode != nullptr) {
    out_inode = inode;
    return Status::OK();
  }

  return GetInode(ctx, ctx.GetInodeVersion(), partition->INo(), out_inode);
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

  if (version > partition->Version()) {
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
  Trace trace;
  std::vector<KeyValue> kvs;
  ScanPartitionOperation operation(trace, fs_id_, parent, [&](KeyValue& kv) -> bool {
    kvs.push_back(std::move(kv));
    return true;
  });

  auto status = RunOperation(&operation);
  if (!status.ok()) return status;

  if (kvs.empty()) {
    return Status(pb::error::ENOT_FOUND, "not found kv");
  }

  auto& parent_kv = kvs.at(0);
  CHECK(MetaCodec::IsInodeKey(parent_kv.key))
      << fmt::format("invalid parent key({}).", Helper::StringToHex(parent_kv.key));

  // build partition
  auto parent_inode = Inode::New(MetaCodec::DecodeInodeValue(parent_kv.value));

  auto old_partition = partition_cache_.Get(parent);
  if (old_partition != nullptr && parent_inode->Version() <= old_partition->Version()) {
    out_partition = old_partition;
    DINGO_LOG(INFO) << fmt::format("[fs.{}.{}] exist fresh partition, version({}:{}) reason({}).", fs_id_, parent,
                                   old_partition->Version(), parent_inode->Version(), reason);
    return Status::OK();
  }

  auto partition = Partition::New(parent_inode);

  // add child dentry
  for (size_t i = 1; i < kvs.size(); ++i) {
    const auto& kv = kvs.at(i);
    auto dentry = MetaCodec::DecodeDentryValue(kv.value);
    partition->PutChild(dentry);
  }

  partition_cache_.PutIf(parent, partition);
  UpsertInodeCache(parent, parent_inode);

  out_partition = partition;

  DINGO_LOG(INFO) << fmt::format("[fs.{}.{}] fetch partition, version({}) reason({}).", fs_id_, parent,
                                 parent_inode->Version(), reason);

  return Status::OK();
}

Status FileSystem::GetDentryFromStore(Ino parent, const std::string& name, Dentry& dentry) {
  Trace trace;
  GetDentryOperation operation(trace, fs_id_, parent, name);

  auto status = RunOperation(&operation);
  if (!status.ok()) return status;
  DINGO_LOG(INFO) << fmt::format("[fs.{}] fetch dentry({}/{}).", fs_id_, parent, name);

  auto& result = operation.GetResult();
  dentry = Dentry(result.dentry);

  return Status::OK();
}

Status FileSystem::ListDentryFromStore(Ino parent, const std::string& last_name, uint32_t limit, bool is_only_dir,
                                       std::vector<Dentry>& dentries) {
  limit = limit > 0 ? limit : UINT32_MAX;

  Trace trace;
  ScanDentryOperation operation(trace, fs_id_, parent, last_name, [&](DentryEntry dentry) -> bool {
    if (is_only_dir && dentry.type() != pb::mds::FileType::DIRECTORY) {
      return true;  // skip non-directory entries
    }

    dentries.push_back(Dentry(dentry));
    return dentries.size() < limit;
  });

  return RunOperation(&operation);
}

Status FileSystem::GetInode(Context& ctx, const Dentry& dentry, PartitionPtr partition, InodeSPtr& out_inode) {
  return GetInode(ctx, ctx.GetInodeVersion(), dentry, partition, out_inode);
}

Status FileSystem::GetInode(Context& ctx, uint64_t version, const Dentry& dentry, PartitionPtr partition,
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

  if (is_cache) UpsertInodeCache(ino, out_inode);

  DINGO_LOG(INFO) << fmt::format("[fs.{}.{}] fetch inode, version({}) reason({}).", fs_id_, ino, out_inode->Version(),
                                 reason);

  return Status::OK();
}

Status FileSystem::BatchGetInodeFromStore(std::vector<uint64_t> inoes, std::vector<InodeSPtr>& out_inodes) {
  Trace trace;
  BatchGetInodeAttrOperation operation(trace, fs_id_, inoes);

  auto status = RunOperation(&operation);
  if (!status.ok()) return status;

  auto& result = operation.GetResult();
  for (auto& attr : result.attrs) {
    out_inodes.push_back(Inode::New(attr));
  }

  return Status::OK();
}

Status FileSystem::GetDelFileFromStore(Ino ino, AttrEntry& out_attr) {
  Trace trace;
  GetDelFileOperation operation(trace, fs_id_, ino);

  auto status = RunOperation(&operation);
  if (!status.ok()) return status;

  auto& result = operation.GetResult();
  out_attr = result.attr;

  return Status::OK();
}

InodeSPtr FileSystem::GetInodeFromCache(Ino ino) { return inode_cache_.GetInode(ino); }

std::map<uint64_t, InodeSPtr> FileSystem::GetAllInodesFromCache() { return inode_cache_.GetAllInodes(); }

void FileSystem::UpsertInodeCache(Ino ino, InodeSPtr inode) { inode_cache_.PutIf(ino, inode); }

void FileSystem::UpsertInodeCache(InodeSPtr inode) { inode_cache_.PutIf(inode->Ino(), inode); }

void FileSystem::UpsertInodeCache(AttrEntry& attr) { inode_cache_.PutIf(attr); }

void FileSystem::DeleteInodeFromCache(Ino ino) { inode_cache_.Delete(ino); }

void FileSystem::ClearCache() {
  partition_cache_.Clear();
  inode_cache_.Clear();
  chunk_cache_.Clear();
}

void FileSystem::ClearInodeCache() { inode_cache_.Clear(); }

void FileSystem::ClearPartitionCache() { partition_cache_.Clear(); }

void FileSystem::ClearChunkCache() { chunk_cache_.Clear(); }

void FileSystem::BatchDeleteCache(uint32_t bucket_num, const std::set<uint32_t>& bucket_ids) {
  if (bucket_ids.empty()) return;

  auto check_fn = [&](const Ino& ino) -> bool {
    uint32_t bucket_id = ino % bucket_num;

    return (bucket_ids.find(bucket_id) != bucket_ids.end());
  };

  partition_cache_.BatchDeleteInodeIf(check_fn);
  inode_cache_.BatchDeleteIf(check_fn);
  chunk_cache_.BatchDeleteIf(check_fn);
}

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

  Duration duration;

  AttrEntry attr;
  attr.set_fs_id(fs_id_);
  attr.set_ino(kRootIno);
  attr.set_length(0);
  attr.set_uid(1008);
  attr.set_gid(1008);
  attr.set_mode(S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  attr.set_nlink(kEmptyDirMinLinkNum);
  attr.set_type(pb::mds::FileType::DIRECTORY);
  attr.set_rdev(0);

  attr.set_ctime(duration.StartNs());
  attr.set_mtime(duration.StartNs());
  attr.set_atime(duration.StartNs());

  attr.add_parents(kRootParentIno);

  auto inode = Inode::New(attr);

  Dentry dentry(fs_id_, "/", kRootParentIno, kRootIno, pb::mds::FileType::DIRECTORY, 0, inode);

  // update backend store
  Trace trace;
  CreateRootOperation operation(trace, dentry, attr);

  auto status = RunOperation(&operation);
  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] create root finish, status({}).", fs_id_, duration.ElapsedUs(),
                                 status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("create root fail, {}", status.error_str()));
  }

  UpsertInodeCache(inode);
  partition_cache_.PutIf(dentry.INo(), Partition::New(inode));

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
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  Duration duration;

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
                                 duration.ElapsedUs(), parent, name, entry_out.attr.version(), (void*)inode.get());

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

Status FileSystem::BatchCreate(Context& ctx, Ino parent, const std::vector<MkNodParam>& params, EntryOut& entry_out,
                               std::vector<std::string>& session_ids) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();
  const std::string& client_id = ctx.ClientId();

  // get partition
  PartitionPtr partition;
  auto status = GetPartition(ctx, parent, partition);
  if (!status.ok()) return status;

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  // check quota
  if (!quota_manager_->CheckQuota(trace, parent, 0, params.size())) {
    return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
  }

  Duration duration;

  std::vector<Inode::AttrEntry> attrs;
  attrs.reserve(params.size());
  std::vector<Dentry> dentries;
  dentries.reserve(params.size());
  std::vector<FileSessionSPtr> file_sessions;
  file_sessions.reserve(params.size());
  std::vector<InodeSPtr> inodes;
  inodes.reserve(params.size());

  std::string names;
  for (const auto& param : params) {
    // check request
    if (param.name.empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "name is empty");
    }

    if (param.parent == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "invalid parent inode id");
    }

    Ino ino = 0;
    auto status = GenFileIno(ino);
    if (!status.ok()) return status;

    Inode::AttrEntry attr;
    attr.set_fs_id(fs_id_);
    attr.set_ino(ino);
    attr.set_length(0);
    attr.set_ctime(duration.StartNs());
    attr.set_mtime(duration.StartNs());
    attr.set_atime(duration.StartNs());
    attr.set_uid(param.uid);
    attr.set_gid(param.gid);
    attr.set_mode(param.mode);
    attr.set_nlink(1);
    attr.set_type(pb::mds::FileType::FILE);
    attr.set_rdev(param.rdev);
    attr.add_parents(param.parent);

    attrs.push_back(attr);

    auto inode = Inode::New(attr);
    inodes.push_back(inode);
    dentries.emplace_back(fs_id_, param.name, param.parent, ino, pb::mds::FileType::FILE, param.flag, inode);

    FileSessionSPtr file_session = file_session_manager_.Create(ino, client_id);
    file_sessions.push_back(file_session);

    names += param.name + ",";
  }

  BatchCreateFileOperation operation(trace, dentries, attrs, file_sessions);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] create {} finish, status({}).", fs_id_, duration.ElapsedUs(), names,
                                 status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;

  // update cache
  session_ids.reserve(file_sessions.size());
  for (auto& file_session : file_sessions) {
    session_ids.push_back(file_session->session_id());
    file_session_manager_.Put(file_session);
  }

  for (auto& dentry : dentries) {
    partition->PutChild(dentry, parent_attr.version());
  }
  for (auto& inode : inodes) {
    UpsertInodeCache(inode);
  }
  UpsertInodeCache(parent_attr);

  // update quota
  std::string reason = fmt::format("create.{}.{}", parent, names);
  quota_manager_->UpdateFsUsage(0, params.size(), reason);
  quota_manager_->AsyncUpdateDirUsage(parent, 0, params.size(), reason);

  for (auto& dentry : dentries) {
    parent_memo_->Remeber(dentry.INo(), parent);
  }

  // set output
  entry_out.attrs = std::move(attrs);
  entry_out.parent_version = parent_attr.version();

  // notify buddy mds to refresh inode
  if (IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(parent_attr));
  }

  return Status::OK();
}

// create file, need below steps:
// 1. create inode
// 2. create dentry and update parent inode(nlink/mtime/ctime)
Status FileSystem::MkNod(Context& ctx, const MkNodParam& param, EntryOut& entry_out) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();
  Ino parent = param.parent;

  // check request
  if (param.name.empty()) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "name is empty");
  }

  if (param.parent == 0) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "invalid parent inode id");
  }

  // get partition
  PartitionPtr partition;
  auto status = GetPartition(ctx, parent, partition);
  if (!status.ok()) return status;

  // generate inode id
  Ino ino = 0;
  status = GenFileIno(ino);
  if (!status.ok()) return status;

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  // check quota
  if (!quota_manager_->CheckQuota(trace, param.parent, 0, 1)) {
    return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
  }

  Duration duration;

  // build inode
  Inode::AttrEntry attr;
  attr.set_fs_id(fs_id_);
  attr.set_ino(ino);
  attr.set_length(0);
  attr.set_ctime(duration.StartNs());
  attr.set_mtime(duration.StartNs());
  attr.set_atime(duration.StartNs());
  attr.set_uid(param.uid);
  attr.set_gid(param.gid);
  attr.set_mode(param.mode);
  attr.set_nlink(1);
  attr.set_type(pb::mds::FileType::FILE);
  attr.set_rdev(param.rdev);
  attr.add_parents(parent);

  auto inode = Inode::New(attr);

  // build dentry
  Dentry dentry(fs_id_, param.name, parent, ino, pb::mds::FileType::FILE, param.flag, inode);

  // update backend store
  MkNodOperation operation(trace, dentry, attr);
  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] mknod {} finish, status({}).", fs_id_, duration.ElapsedUs(), param.name,
                                 status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;

  // update cache
  UpsertInodeCache(ino, inode);
  UpsertInodeCache(parent_attr);
  partition->PutChild(dentry, parent_attr.version());

  // update quota
  std::string reason = fmt::format("mknod.{}.{}", parent, param.name);
  quota_manager_->UpdateFsUsage(0, 1, reason);
  quota_manager_->AsyncUpdateDirUsage(param.parent, 0, 1, reason);

  parent_memo_->Remeber(attr.ino(), param.parent);

  entry_out.attr.Swap(&attr);
  entry_out.parent_version = parent_attr.version();

  if (IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(parent_attr));
  }

  return Status::OK();
}

Status FileSystem::GetChunksFromStore(Ino ino, std::vector<ChunkEntry>& chunks, uint32_t max_slice_num) {
  Trace trace;
  ScanChunkOperation operation(trace, fs_id_, ino, max_slice_num);
  auto status = RunOperation(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[fs.{}] fetch chunks fail, status({}).", fs_id_, ino, status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  chunks = std::move(result.chunks);

  return Status::OK();
}

Status FileSystem::Open(Context& ctx, Ino ino, uint32_t flags, bool is_prefetch_chunk, std::string& session_id,
                        EntryOut& entry_out, std::vector<ChunkEntry>& chunks) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  // O_ACCMODE	         0003
  // O_RDONLY	             00
  // O_WRONLY	             01
  // O_RDWR		             02
  // O_CREAT		         0100
  // O_TRUNC		        01000
  // O_APPEND		        02000
  // O_NONBLOCK	        04000
  // O_SYNC	         04010000
  // O_ASYNC	         020000
  if ((flags & O_TRUNC) && !(flags & O_WRONLY || flags & O_RDWR)) {
    return Status(pb::error::ENO_PERMISSION, "O_TRUNC without O_WRONLY or O_RDWR");
  }

  auto& trace = ctx.GetTrace();
  const std::string& client_id = ctx.ClientId();

  Duration duration;

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  FileSessionSPtr file_session = file_session_manager_.Create(ino, client_id);

  OpenFileOperation operation(trace, flags, *file_session);

  status = RunOperation(&operation);
  if (!status.ok()) {
    return status;
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;
  int64_t delta_bytes = result.delta_bytes;

  session_id = file_session->session_id();
  entry_out.attr = attr;

  file_session_manager_.Put(file_session);

  // update quota
  if (delta_bytes != 0) {
    std::string reason = fmt::format("open.{}", ino);
    quota_manager_->UpdateFsUsage(delta_bytes, 0, reason);
    for (auto parent : attr.parents()) {
      quota_manager_->AsyncUpdateDirUsage(parent, delta_bytes, 0, reason);
    }
  }

  auto file_length = attr.length();
  // update cache
  UpsertInodeCache(attr);

  auto get_chunks_from_cache_fn = [&](std::vector<ChunkEntry>& chunks) {
    auto chunk_ptrs = chunk_cache_.Get(ino);
    uint32_t slice_num = 0;
    for (auto& chunk_ptr : chunk_ptrs) {
      chunks.push_back(*chunk_ptr);

      slice_num += chunk_ptr->slices_size();
      if (slice_num >= FLAGS_mds_transfer_max_slice_num) break;
    }
  };

  uint64_t chunk_size = fs_info_->GetChunkSize();
  auto is_completely_fn = [&](const std::vector<ChunkEntry>& chunks) -> bool {
    uint32_t slice_num = 0;
    for (const auto& chunk : chunks) {
      slice_num += chunk.slices_size();
      if (slice_num >= FLAGS_mds_transfer_max_slice_num) return true;
    }

    if (file_length == 0) return true;

    uint64_t chunk_num = file_length % chunk_size == 0 ? file_length / chunk_size : (file_length / chunk_size) + 1;
    return chunks.size() >= chunk_num;
  };

  std::string fetch_from("none");
  if (is_prefetch_chunk && ((flags & O_ACCMODE) == O_RDONLY || flags & O_RDWR)) {
    fetch_from = "cache";
    // priority take from cache
    get_chunks_from_cache_fn(chunks);

    bool is_completely = is_completely_fn(chunks);
    // if not enough then fetch from store
    if (!is_completely) {
      fetch_from = "store";
      auto status = GetChunksFromStore(ino, chunks, FLAGS_mds_transfer_max_slice_num);
      if (status.ok() && !is_completely_fn(chunks)) {
        DINGO_LOG(WARNING) << fmt::format("[fs.{}] chunks is not completely, ino({}) length({}) chunks({}).", fs_id_,
                                          ino, file_length, chunks.size());
      }
    }
  }

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] open {} finish, flags({:o}:{}) fetch_chunk({}) status({}).", fs_id_,
                                 duration.ElapsedUs(), ino, flags, Helper::DescOpenFlags(flags), fetch_from,
                                 status.error_str());

  return Status::OK();
}

Status FileSystem::Release(Context& ctx, Ino ino, const std::string& session_id) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  CloseFileOperation operation(trace, fs_id_, ino, session_id);

  auto status = RunOperation(&operation);
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << fmt::format("[fs.{}] release finish, ino({}) session_id({}) status({}).", fs_id_, ino, session_id,
                                 status.error_str());

  // delete cache
  file_session_manager_.Delete(ino, session_id);

  return Status::OK();
}

// create directory, need below steps:
// 1. create inode
// 2. create dentry and update parent inode(nlink/mtime/ctime)
Status FileSystem::MkDir(Context& ctx, const MkDirParam& param, EntryOut& entry_out) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();
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

  // generate inode id
  Ino ino = 0;
  status = GenDirIno(ino);
  if (!status.ok()) {
    return status;
  }

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  // check quota
  if (!quota_manager_->CheckQuota(trace, param.parent, 0, 1)) {
    return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
  }

  // build inode
  Duration duration;

  Inode::AttrEntry attr;
  attr.set_fs_id(fs_id_);
  attr.set_ino(ino);
  attr.set_length(4096);
  attr.set_ctime(duration.StartNs());
  attr.set_mtime(duration.StartNs());
  attr.set_atime(duration.StartNs());
  attr.set_uid(param.uid);
  attr.set_gid(param.gid);
  attr.set_mode(S_IFDIR | param.mode);
  attr.set_nlink(kEmptyDirMinLinkNum);
  attr.set_type(pb::mds::FileType::DIRECTORY);
  attr.set_rdev(param.rdev);
  attr.add_parents(parent);

  auto inode = Inode::New(attr);

  // build dentry
  Dentry dentry(fs_id_, param.name, parent, ino, pb::mds::FileType::DIRECTORY, param.flag, inode);

  // update backend store
  MkDirOperation operation(trace, dentry, attr);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] mkdir {} finish, status({}).", fs_id_, duration.ElapsedUs(), param.name,
                                 status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;

  // update cache
  UpsertInodeCache(ino, inode);
  UpsertInodeCache(parent_attr);
  partition->PutChild(dentry, parent_attr.version());
  if (IsMonoPartition()) {
    partition_cache_.PutIf(ino, Partition::New(inode));
  }

  // update quota
  std::string reason = fmt::format("mkdir.{}.{}", parent, param.name);
  quota_manager_->UpdateFsUsage(0, 1, reason);
  quota_manager_->AsyncUpdateDirUsage(param.parent, 0, 1, reason);

  parent_memo_->Remeber(attr.ino(), param.parent);

  entry_out.attr.Swap(&attr);
  entry_out.parent_version = parent_attr.version();

  if (IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(parent_attr));
  }

  return Status::OK();
}

Status FileSystem::RmDir(Context& ctx, Ino parent, const std::string& name) {
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
    return Status(pb::error::ENOT_FOUND, fmt::format("child dentry({}) not found.", name));
  }

  if (IsMonoPartition()) {
    InodeSPtr inode;
    GetInode(ctx, dentry.INo(), inode);
    if (inode != nullptr && inode->Nlink() > kEmptyDirMinLinkNum) {
      return Status(pb::error::ENOT_EMPTY,
                    fmt::format("dir({}/{}) is not empty, nlink({}).", parent, name, inode->Nlink()));
    }
  }

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  Duration duration;

  // update backend store
  RmDirOperation operation(trace, dentry);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] rmdir {} finish, status({}).", fs_id_, duration.ElapsedUs(), name,
                                 status.error_str());
  if (!status.ok()) {
    return status;
  }

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;  // parent

  // update cache
  UpsertInodeCache(parent_attr);
  partition->DeleteChild(name, parent_attr.version());

  // update quota
  std::string reason = fmt::format("rmdir.{}.{}", parent, name);
  quota_manager_->UpdateFsUsage(0, -1, reason);
  quota_manager_->AsyncUpdateDirUsage(parent, 0, -1, reason);
  quota_manager_->AsyncDeleteDirQuota(dentry.INo());

  parent_memo_->Forget(dentry.INo());

  if (IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(parent_attr));
    NotifyBuddyCleanPartitionCache(dentry.INo(), UINT64_MAX);
  } else {
    partition_cache_.Delete(dentry.INo());
  }

  return Status::OK();
}

Status FileSystem::ReadDir(Context& ctx, Ino ino, const std::string& last_name, uint32_t limit, bool with_attr,
                           std::vector<EntryOut>& entry_outs) {
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
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  PartitionPtr partition;
  auto status = GetPartition(ctx, new_parent, partition);
  if (!status.ok()) {
    return status;
  }

  // get inode
  InodeSPtr inode;
  status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  // check quota
  if (!quota_manager_->CheckQuota(trace, new_parent, 0, 1)) {
    return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
  }

  uint32_t fs_id = inode->FsId();

  // build dentry
  Dentry dentry(fs_id, new_name, new_parent, ino, pb::mds::FileType::FILE, 0, inode);

  // update backend store
  Duration duration;

  HardLinkOperation operation(trace, dentry);
  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] link {} -> {}/{} finish, status({}).", fs_id_, duration.ElapsedUs(),
                                 ino, new_parent, new_name, status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;
  auto& child_attr = result.child_attr;

  // update quota
  std::string reason = fmt::format("link.{}.{}.{}", ino, new_parent, new_name);
  quota_manager_->AsyncUpdateDirUsage(new_parent, child_attr.length(), 1, reason);

  // update cache
  UpsertInodeCache(child_attr);
  UpsertInodeCache(parent_attr);
  partition->PutChild(dentry, parent_attr.version());

  entry_out.attr = child_attr;
  entry_out.parent_version = parent_attr.version();

  if (IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(parent_attr));
  }

  return Status::OK();
}

// delete hard link for file
// 1. delete dentry and update parent inode(nlink/mtime/ctime)
// 3. update inode(nlink/mtime/ctime)
Status FileSystem::UnLink(Context& ctx, Ino parent, const std::string& name, EntryOut& entry_out) {
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

  if (inode->Type() == pb::mds::FileType::DIRECTORY) {
    return Status(pb::error::ENOT_FILE, "directory not allow unlink");
  }

  // update parent memo
  UpdateParentMemo(ctx.GetAncestors());

  Duration duration;

  // update backend store
  UnlinkOperation operation(trace, dentry);

  status = RunOperation(&operation);

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;
  auto& child_attr = result.child_attr;

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] unlink {}/{} finish, nlink({}) status({}).", fs_id_,
                                 duration.ElapsedUs(), parent, name, child_attr.nlink(), status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", status.error_str()));
  }

  // update quota
  std::string reason = fmt::format("unlink.{}.{}", parent, name);
  int64_t delta_bytes = child_attr.type() != pb::mds::SYM_LINK ? child_attr.length() : 0;
  if (child_attr.nlink() == 0) {
    quota_manager_->UpdateFsUsage(-delta_bytes, -1, reason);
    chunk_cache_.Delete(child_attr.ino());
  }
  quota_manager_->AsyncUpdateDirUsage(parent, -delta_bytes, -1, reason);

  // update cache
  partition->DeleteChild(name, parent_attr.version());
  UpsertInodeCache(parent_attr);
  UpsertInodeCache(child_attr);

  entry_out.attr = child_attr;
  entry_out.parent_version = parent_attr.version();

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
  if (!quota_manager_->CheckQuota(trace, new_parent, 0, 1)) {
    return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
  }

  // build inode
  Duration duration;

  Inode::AttrEntry attr;
  attr.set_fs_id(fs_id_);
  attr.set_ino(ino);
  attr.set_symlink(symlink);
  attr.set_length(symlink.size());
  attr.set_ctime(duration.StartNs());
  attr.set_mtime(duration.StartNs());
  attr.set_atime(duration.StartNs());
  attr.set_uid(uid);
  attr.set_gid(gid);
  attr.set_mode(S_IFLNK | 0777);
  attr.set_nlink(1);
  attr.set_type(pb::mds::FileType::SYM_LINK);
  attr.set_rdev(1);
  attr.add_parents(new_parent);

  auto inode = Inode::New(attr);

  // build dentry
  Dentry dentry(fs_id_, new_name, new_parent, ino, pb::mds::FileType::SYM_LINK, 0, inode);

  // update backend store
  SmyLinkOperation operation(trace, dentry, attr);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] symlink {}/{} finish,  status({}).", fs_id_, duration.ElapsedUs(),
                                 new_parent, new_name, status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode/dentry fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& parent_attr = result.attr;

  // update cache
  UpsertInodeCache(ino, inode);
  UpsertInodeCache(parent_attr);
  partition->PutChild(dentry, parent_attr.version());

  // update quota
  std::string reason = fmt::format("symlink.{}.{}", new_parent, new_name);
  quota_manager_->UpdateFsUsage(0, 1, reason);
  quota_manager_->AsyncUpdateDirUsage(new_parent, 0, 1, reason);

  entry_out.attr.Swap(&attr);
  entry_out.parent_version = parent_attr.version();

  if (IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(parent_attr));
  }

  return Status::OK();
}

Status FileSystem::ReadLink(Context& ctx, Ino ino, std::string& link) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  if (inode->Type() != pb::mds::FileType::SYM_LINK) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "not symlink inode");
  }

  link = inode->Symlink();

  return Status::OK();
}

Status FileSystem::GetAttr(Context& ctx, Ino ino, EntryOut& entry_out) {
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
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  Duration duration;

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  if (param.to_set & kSetAttrLength) {
    if (param.attr.length() > inode->Length()) {
      // check quota
      if (!quota_manager_->CheckQuota(trace, ino, param.attr.length() - inode->Length(), 0)) {
        return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
      }
    }
  }

  // update backend store
  UpdateAttrOperation::ExtraParam extra_param;
  extra_param.block_size = fs_info_->GetBlockSize();
  extra_param.chunk_size = fs_info_->GetChunkSize();
  UpdateAttrOperation operation(trace, ino, param.to_set, param.attr, extra_param);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] setattr {} finish, status({}).", fs_id_, duration.ElapsedUs(), ino,
                                 status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;

  entry_out.attr = attr;

  // update quota
  if (param.to_set & kSetAttrLength) {
    std::string reason = fmt::format("setattr.{}", ino);
    int64_t change_bytes = param.attr.length() > inode->Length()
                               ? param.attr.length() - inode->Length()
                               : -static_cast<int64_t>(inode->Length() - param.attr.length());
    quota_manager_->UpdateFsUsage(change_bytes, 0, reason);

    for (const auto& parent : attr.parents()) {
      quota_manager_->AsyncUpdateDirUsage(parent, change_bytes, 0, reason);
    }
  }

  // update cache
  UpsertInodeCache(attr);
  if (IsDir(ino) && IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(attr));
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

Status FileSystem::SetXAttr(Context& ctx, Ino ino, const Inode::XAttrMap& xattrs, EntryOut& entry_out) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  Duration duration;

  // update backend store
  UpdateXAttrOperation operation(trace, fs_id_, ino, xattrs);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] setxattr {} finish, status({}).", fs_id_, duration.ElapsedUs(), ino,
                                 status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;

  entry_out.attr = attr;

  // update cache
  UpsertInodeCache(attr);
  if (IsDir(ino) && IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(attr));
  }

  return Status::OK();
}

Status FileSystem::RemoveXAttr(Context& ctx, Ino ino, const std::string& name, EntryOut& entry_out) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  Duration duration;

  // update backend store
  RemoveXAttrOperation operation(trace, fs_id_, ino, name);

  status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] removexattr {} finish, status({}).", fs_id_, duration.ElapsedUs(), ino,
                                 status.error_str());

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;

  entry_out.attr = attr;

  // update cache
  UpsertInodeCache(attr);
  if (IsDir(ino) && IsParentHashPartition()) {
    NotifyBuddyRefreshInode(std::move(attr));
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

void FileSystem::NotifyBuddyRefreshFsInfo(std::vector<uint64_t> mds_ids, const FsInfoEntry& fs_info) {
  for (auto mds_id : mds_ids) {
    if (mds_id == 0 || mds_id == self_mds_id_) continue;

    notify_buddy_->AsyncNotify(notify::RefreshFsInfoMessage::Create(mds_id, fs_info.fs_id(), fs_info.fs_name()));
  }
}

void FileSystem::NotifyBuddyRefreshInode(AttrEntry&& attr) {
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

void FileSystem::NotifyBuddyCleanPartitionCache(Ino ino, uint64_t version) {
  if (notify_buddy_ == nullptr) return;

  auto mds_id = GetMdsIdByIno(ino);
  CHECK(mds_id != 0) << fmt::format("mds id should not be 0, ino({}).", ino);
  if (mds_id == self_mds_id_) {
    partition_cache_.DeleteIf(ino, version);

  } else {
    notify_buddy_->AsyncNotify(notify::CleanPartitionCacheMessage::Create(mds_id, fs_id_, ino, version));
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

  Duration duration;

  // check name is valid
  if (new_name.size() > FLAGS_mds_filesystem_name_max_size) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "new name is too long.");
  }

  if (old_parent == new_parent && old_name == new_name) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "not allow same name");
  }

  Dentry dentry;
  auto status = GetDentryFromStore(old_parent, old_name, dentry);
  if (!status.ok()) return status;

  // update parent memo
  UpdateParentMemo(param.old_ancestors);
  UpdateParentMemo(param.new_ancestors);

  // check quota
  bool is_exist_quota = false;
  if (dentry.Type() == pb::mds::FileType::DIRECTORY) {
    auto old_quota = quota_manager_->GetNearestDirQuota(old_parent);
    auto new_quota = quota_manager_->GetNearestDirQuota(new_parent);
    bool can_rename = (old_quota == nullptr && new_quota == nullptr) ||
                      (old_quota != nullptr && new_quota != nullptr && old_quota->INo() == new_quota->INo());
    if (!can_rename) {
      return Status(pb::error::ENOT_SUPPORT, "not support rename between quota directory");
    }
    if (old_quota) is_exist_quota = true;
  }

  RenameOperation operation(trace, fs_id_, old_parent, old_name, new_parent, new_name);

  status = RunOperation(&operation);

  auto& result = operation.GetResult();
  auto& old_parent_attr = result.old_parent_attr;
  auto& new_parent_attr = result.new_parent_attr;
  auto& old_dentry = result.old_dentry;
  auto& prev_new_dentry = result.prev_new_dentry;
  auto& prev_new_attr = result.prev_new_attr;
  // auto& new_dentry = result.new_dentry;
  auto& old_attr = result.old_attr;
  bool is_same_parent = result.is_same_parent;
  bool is_exist_new_dentry = result.is_exist_new_dentry;

  DINGO_LOG(INFO) << fmt::format(
      "[fs.{}][{}us] rename {}/{} -> {}/{} finish, state({},{}) version({},{}) "
      "status({}).",
      fs_id_, duration.ElapsedUs(), old_parent, old_name, new_parent, new_name, is_same_parent, is_exist_new_dentry,
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
      UpsertInodeCache(old_parent_attr);
    }

    UpsertInodeCache(old_attr);

    // check new parent dentry/inode
    PartitionPtr new_parent_partition;
    status = GetPartition(ctx, new_parent, new_parent_partition);
    if (status.ok()) {
      // update new parent attr
      UpsertInodeCache(new_parent_attr);

      // delete prev new dentry
      if (is_exist_new_dentry) new_parent_partition->DeleteChild(new_name);

      // add new dentry
      Dentry new_dentry(fs_id_, new_name, new_parent, old_dentry.ino(), old_dentry.type(), 0,
                        GetInodeFromCache(old_dentry.ino()));
      new_parent_partition->PutChild(new_dentry);
    }

    // delete exist new partition
    if (is_exist_new_dentry) {
      if (prev_new_dentry.type() == pb::mds::FileType::DIRECTORY) {
        partition_cache_.Delete(prev_new_dentry.ino());
      } else {
        if (prev_new_attr.nlink() <= 0) {
          DeleteInodeFromCache(prev_new_attr.ino());

        } else {
          UpsertInodeCache(prev_new_attr);
        }
      }
    }

  } else {
    // clean partition cache
    NotifyBuddyCleanPartitionCache(old_parent, old_parent_attr.version());
    if (!is_same_parent) NotifyBuddyCleanPartitionCache(new_parent, new_parent_attr.version());

    // refresh parent of parent inode cache
    NotifyBuddyRefreshInode(std::move(old_parent_attr));
    if (!is_same_parent) NotifyBuddyRefreshInode(std::move(new_parent_attr));

    // delete exist new partition
    if (is_exist_new_dentry) {
      if (prev_new_dentry.type() == pb::mds::FileType::DIRECTORY) {
        NotifyBuddyCleanPartitionCache(prev_new_dentry.ino(), UINT64_MAX);
      } else {
        if (prev_new_attr.nlink() <= 0) {
          DeleteInodeFromCache(prev_new_attr.ino());

        } else {
          UpsertInodeCache(prev_new_attr);
        }
      }
    }
  }

  // update fs quota
  std::string reason = fmt::format("rename.{}.{}.to.{}.{}", old_parent, old_name, new_parent, new_name);
  if (is_exist_new_dentry) {
    int64_t fs_delta_bytes = 0;
    if (prev_new_attr.type() == pb::mds::FileType::FILE && prev_new_attr.nlink() == 0) {
      fs_delta_bytes -= prev_new_attr.length();
    }
    quota_manager_->UpdateFsUsage(fs_delta_bytes, -1, reason);
  }

  // update dir quota
  if (dentry.Type() == pb::mds::FileType::FILE) {
    if (!is_same_parent) {
      quota_manager_->AsyncUpdateDirUsage(old_parent, -old_attr.length(), -1, reason);
      quota_manager_->AsyncUpdateDirUsage(new_parent, old_attr.length(), 1, reason);
    }

    if (is_exist_new_dentry) {
      quota_manager_->AsyncUpdateDirUsage(new_parent, -prev_new_attr.length(), -1, reason);
    }

  } else if (dentry.Type() == pb::mds::FileType::DIRECTORY) {
    if (is_exist_new_dentry && is_exist_quota) {
      quota_manager_->AsyncUpdateDirUsage(old_parent, 0, -1, reason);
    }
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

static uint64_t CalculateDeltaLength(uint64_t length, const std::vector<DeltaSliceEntry>& delta_slices) {
  uint64_t temp_length = length;
  for (const auto& delta_slice : delta_slices) {
    for (const auto& slice : delta_slice.slices()) {
      temp_length = std::max(temp_length, slice.offset() + slice.len());
    }
  }

  return temp_length - length;
}

Status FileSystem::WriteSlice(Context& ctx, Ino, Ino ino, const std::vector<DeltaSliceEntry>& delta_slices) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  auto& trace = ctx.GetTrace();

  Duration duration;

  // check quota
  uint64_t delta_bytes = CalculateDeltaLength(inode->Length(), delta_slices);
  if (!quota_manager_->CheckQuota(trace, ino, static_cast<int64_t>(delta_bytes), 0)) {
    return Status(pb::error::EQUOTA_EXCEED, "exceed quota limit");
  }

  // update backend store
  UpsertChunkOperation operation(trace, GetFsInfo(), ino, delta_slices);

  status = RunOperation(&operation);

  std::string slice_id_str;
  for (const auto& delta_slice : delta_slices) {
    slice_id_str += std::to_string(delta_slice.chunk_index()) + ",";
    DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] writeslice {}/{} finish, status({}).", fs_id_, duration.ElapsedUs(),
                                   ino, delta_slice.chunk_index(), status.error_str());
  }

  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("upsert chunk fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;
  int64_t length_delta = result.length_delta;
  auto& chunks = result.effected_chunks;

  // update cache
  UpsertInodeCache(attr);

  // check whether need to compact chunk
  for (auto& chunk : chunks) {
    if (FLAGS_mds_compact_chunk_enable &&
        static_cast<uint32_t>(chunk.slices_size()) > FLAGS_mds_compact_chunk_threshold_num &&
        chunk.last_compaction_time_ms() + FLAGS_mds_compact_chunk_interval_ms <
            static_cast<uint64_t>(Helper::TimestampMs())) {
      auto fs_info = fs_info_->Get();
      if (CompactChunkOperation::MaybeCompact(fs_info, ino, attr.length(), chunk)) {
        DINGO_LOG(INFO) << fmt::format("[fs.{}] trigger compact chunk({}) for ino({}).", fs_id_, chunk.index(), ino);

        auto post_handler = [this](OperationSPtr operation) {
          auto origin_operation = std::dynamic_pointer_cast<CompactChunkOperation>(operation);
          auto& result = origin_operation->GetResult();
          // update chunk cache
          chunk_cache_.PutIf(origin_operation->GetIno(), std::move(result.effected_chunk));
        };
        operation_processor_->AsyncRun(CompactChunkOperation::New(fs_info, ino, chunk.index(), attr.length()),
                                       post_handler);
      }
    }
  }

  // update chunk cache
  for (auto& chunk : chunks) {
    chunk_cache_.PutIf(ino, std::move(chunk));
  }

  // update quota
  if (length_delta != 0) {
    std::string reason = fmt::format("writeslice.{}.{}", ino, slice_id_str);
    quota_manager_->UpdateFsUsage(length_delta, 0, reason);
    for (const auto& parent : attr.parents()) {
      quota_manager_->AsyncUpdateDirUsage(parent, length_delta, 0, reason);
    }
  }

  return Status::OK();
}

Status FileSystem::ReadSlice(Context& ctx, Ino ino, const std::vector<uint64_t>& chunk_indexes,
                             std::vector<ChunkEntry>& chunks) {
  if (!ctx.IsBypassCache() && !CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  auto& trace = ctx.GetTrace();

  Duration duration;

  // get chunk from cache
  std::vector<uint32_t> miss_chunk_indexes;
  for (const auto& chunk_index : chunk_indexes) {
    auto chunk = chunk_cache_.Get(ino, chunk_index);
    if (chunk != nullptr) {
      chunks.push_back(*chunk);
    } else {
      miss_chunk_indexes.push_back(chunk_index);
    }
  }
  if (miss_chunk_indexes.empty()) return Status::OK();

  // get chunk from backend store
  GetChunkOperation operation(trace, fs_id_, ino, miss_chunk_indexes);

  auto status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] readslice {}/{} finish, miss({}) status({}).", fs_id_,
                                 duration.ElapsedUs(), ino, Helper::VectorToString(chunk_indexes),
                                 Helper::VectorToString(miss_chunk_indexes), status.error_str());

  if (!status.ok() && status.error_code() != pb::error::ENOT_FOUND) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("get chunk fail, {}", status.error_str()));
  }

  if (status.ok()) {
    auto& result = operation.GetResult();

    for (auto& chunk : result.chunks) {
      chunks.push_back(chunk);
      // update chunk cache
      chunk_cache_.PutIf(ino, std::move(chunk));
    }
  }

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

  Duration duration;

  uint64_t slice_num = 0;
  uint64_t slice_id = 0;
  if (mode == 0 || (mode & FALLOC_FL_ZERO_RANGE)) {
    uint64_t new_length = offset + len;
    if (new_length > inode->Length()) {
      // check quota
      if (!quota_manager_->CheckQuota(trace, ino, new_length - inode->Length(), 0)) {
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
        fs_id_, duration.ElapsedUs(), ino, mode, offset, len, status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;
  auto& effected_chunks = result.effected_chunks;

  UpsertInodeCache(attr);

  entry_out.attr = std::move(attr);

  // update chunk cache
  for (auto& chunk : effected_chunks) {
    chunk_cache_.PutIf(ino, std::move(chunk));
  }

  return Status::OK();
}

Status FileSystem::CompactChunk(Context& ctx, Ino ino, uint64_t chunk_index,
                                std::vector<pb::mds::TrashSlice>& trash_slices) {
  if (!CanServe()) {
    return Status(pb::error::ENOT_SERVE, "can not serve");
  }

  InodeSPtr inode;
  auto status = GetInode(ctx, ino, inode);
  if (!status.ok()) {
    return status;
  }

  auto& trace = ctx.GetTrace();
  Duration duration;

  CompactChunkOperation operation(trace, GetFsInfo(), ino, chunk_index, inode->Length(), true);

  status = RunOperation(&operation);

  auto& result = operation.GetResult();
  auto& effected_chunk = result.effected_chunk;
  trash_slices = Helper::PbRepeatedToVector(result.trash_slice_list.mutable_slices());

  DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] compactchunk {}/{} finish, trash_slices({}) status({}).", fs_id_,
                                 duration.ElapsedUs(), ino, chunk_index, trash_slices.size(), status.error_str());
  if (!status.ok()) {
    return status;
  }

  // update chunk cache
  chunk_cache_.PutIf(ino, std::move(effected_chunk));

  return Status::OK();
}

Status FileSystem::GetDentry(Context& ctx, Ino parent, const std::string& name, Dentry& dentry) {
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
  bool bypass_cache = ctx.IsBypassCache();

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
                                 std::vector<pb::mds::XAttr>& out_xattrs) {
  bool bypass_cache = ctx.IsBypassCache();

  auto add_xattr_func = [&out_xattrs](const InodeSPtr& inode) {
    pb::mds::XAttr xattr;
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
  for (const auto& ino : inoes) {
    partition_cache_.Delete(ino);
    inode_cache_.Delete(ino);
  }

  return Status::OK();
}

void FileSystem::RefreshInode(AttrEntry& attr) { UpsertInodeCache(attr); }

Status FileSystem::RefreshFsInfo(const std::string& reason) { return RefreshFsInfo(fs_info_->GetName(), reason); }

Status FileSystem::RefreshFsInfo(const std::string& name, const std::string& reason) {
  DINGO_LOG(INFO) << fmt::format("[fs.{}] refresh fs({}) info.", fs_id_, name);

  Trace trace;
  GetFsOperation operation(trace, name);

  auto status = RunOperation(&operation);
  if (!status.ok()) {
    return Status(pb::error::EBACKEND_STORE, fmt::format("get fs info fail, {}", status.error_str()));
  }

  auto& result = operation.GetResult();

  RefreshFsInfo(result.fs_info, reason);

  return Status::OK();
}

static std::set<uint32_t> GetDeletedBucketIds(int64_t mds_id, const pb::mds::HashPartition& old_hash,
                                              const pb::mds::HashPartition& hash) {
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

void FileSystem::RefreshFsInfo(const FsInfoEntry& fs_info, const std::string& reason) {
  // clean partition and inode cache
  auto pre_handler = [&](const FsInfoEntry& old_fs_info, const FsInfoEntry& new_fs_info) {
    const auto& partition_policy = new_fs_info.partition_policy();
    if (partition_policy.type() == pb::mds::PartitionType::MONOLITHIC_PARTITION) {
      if (partition_policy.mono().mds_id() != self_mds_id_) {
        ClearCache();
      }

    } else if (partition_policy.type() == pb::mds::PartitionType::PARENT_ID_HASH_PARTITION) {
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

  if (fs_info_->Update(fs_info, pre_handler)) {
    can_serve_.store(CanServe(self_mds_id_), std::memory_order_release);

    DINGO_LOG(INFO) << fmt::format("[fs.{}][{}us] update fs({} v{}) can_serve({}) reason({}).", fs_id_,
                                   duration.ElapsedUs(), fs_info.fs_name(), fs_info.version(),
                                   can_serve_ ? "true" : "false", reason);
  }
}

Status FileSystem::JoinMonoFs(Context& ctx, uint64_t mds_id, const std::string& reason) {
  if (PartitionType() != pb::mds::PartitionType::MONOLITHIC_PARTITION) {
    return Status(pb::error::ENOT_SUPPORT, "not support join fs for hash partition");
  }

  uint64_t old_mds_id = 0;
  auto handler = [&](PartitionPolicy& partition_policy, FsOpLog& log) -> Status {
    auto* mono = partition_policy.mutable_mono();
    if (mono->mds_id() == mds_id) {
      return Status(pb::error::EEXISTED, "mds already exist");
    }
    old_mds_id = mono->mds_id();

    mono->set_mds_id(mds_id);

    log.set_fs_name(FsName());
    log.set_fs_id(fs_id_);
    log.set_type(pb::mds::FsOpLog::JOIN_FS);
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

  RefreshFsInfo(result.fs_info, "join_mono");

  NotifyBuddyRefreshFsInfo({old_mds_id}, result.fs_info);

  return Status::OK();
}

static std::vector<uint64_t> GetMdsIdFromHashPartitioin(const pb::mds::HashPartition& hash) {
  std::vector<uint64_t> mds_ids;
  mds_ids.reserve(hash.distributions_size());
  for (const auto& [mds_id, _] : hash.distributions()) {
    mds_ids.push_back(mds_id);
  }

  return mds_ids;
}

// 按bucket_id数量平均分布
static void DistributeByMean(const std::vector<uint64_t>& mds_ids, pb::mds::HashPartition& hash) {
  uint32_t mean_num = hash.bucket_num() / (hash.distributions_size() + mds_ids.size());

  std::vector<uint64_t> pending_bucket_ids;
  pending_bucket_ids.reserve(mean_num * mds_ids.size());
  for (auto& [_, bucket_set] : *hash.mutable_distributions()) {
    for (int i = mean_num; i < bucket_set.bucket_ids_size(); ++i) {
      pending_bucket_ids.push_back(bucket_set.bucket_ids(i));
    }
    if (static_cast<uint32_t>(bucket_set.bucket_ids_size()) > mean_num) {
      bucket_set.mutable_bucket_ids()->Resize(mean_num, 0);
    }
  }

  uint32_t pending_offset = 0;
  for (size_t i = 0; i < mds_ids.size(); ++i) {
    BucketSetEntry bucket_set;
    while (pending_offset < pending_bucket_ids.size()) {
      bucket_set.add_bucket_ids(pending_bucket_ids[pending_offset++]);
      if ((i + 1) < mds_ids.size() && static_cast<uint32_t>(bucket_set.bucket_ids_size()) >= mean_num) break;
    }

    // sort bucket id
    std::sort(bucket_set.mutable_bucket_ids()->begin(), bucket_set.mutable_bucket_ids()->end());

    hash.mutable_distributions()->insert({mds_ids[i], std::move(bucket_set)});
  }
}

Status FileSystem::JoinHashFs(Context& ctx, const std::vector<uint64_t>& mds_ids, const std::string& reason) {
  if (PartitionType() != pb::mds::PartitionType::PARENT_ID_HASH_PARTITION) {
    return Status(pb::error::ENOT_SUPPORT, "not support join fs for mono partition");
  }

  auto has_mds_fn = [&](const pb::mds::HashPartition& hash) -> bool {
    for (const auto& mds_id : mds_ids) {
      if (hash.distributions().find(mds_id) != hash.distributions().end()) {
        return true;
      }
    }

    return false;
  };

  std::vector<uint64_t> old_mds_ids;
  auto handler = [&](PartitionPolicy& partition_policy, FsOpLog& log) -> Status {
    auto* hash = partition_policy.mutable_parent_hash();

    if (has_mds_fn(*hash)) {
      return Status(pb::error::EEXISTED, "mds already exists");
    }

    old_mds_ids = GetMdsIdFromHashPartitioin(*hash);

    DistributeByMean(mds_ids, *hash);

    CHECK(HashPartitionHelper::CheckHashPartition(*hash)) << "invalid hash partition bucket id size.";

    log.set_fs_name(FsName());
    log.set_fs_id(fs_id_);
    log.set_type(pb::mds::FsOpLog::JOIN_FS);
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

  RefreshFsInfo(result.fs_info, "join_hash");

  NotifyBuddyRefreshFsInfo(old_mds_ids, result.fs_info);

  return Status::OK();
}

// 把准备退出的mds负责的bucket_id平均分配给剩余mds
static void ReDistributeByDeleteMds(const std::vector<uint64_t>& quit_mds_ids, pb::mds::HashPartition& hash) {
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
  if (PartitionType() != pb::mds::PartitionType::PARENT_ID_HASH_PARTITION) {
    return Status(pb::error::ENOT_SUPPORT, "not support join fs for mono partition");
  }

  auto miss_mds_fn = [&](const pb::mds::HashPartition& hash) -> bool {
    for (const auto& mds_id : mds_ids) {
      if (hash.distributions().find(mds_id) != hash.distributions().end()) {
        return false;
      }
    }

    return true;
  };

  std::vector<uint64_t> old_mds_ids;
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

    CHECK(HashPartitionHelper::CheckHashPartition(*hash)) << "invalid hash partition bucket id size.";

    log.set_fs_name(FsName());
    log.set_fs_id(fs_id_);
    log.set_type(pb::mds::FsOpLog::QUIT_FS);
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

  RefreshFsInfo(result.fs_info, "quit_fs");

  NotifyBuddyRefreshFsInfo(old_mds_ids, result.fs_info);

  return Status::OK();
}

Status FileSystem::QuitAndJoinFs(Context& ctx, const std::vector<uint64_t>& quit_mds_ids,
                                 const std::vector<uint64_t>& join_mds_ids, const std::string& reason) {
  if (PartitionType() != pb::mds::PartitionType::PARENT_ID_HASH_PARTITION) {
    return Status(pb::error::ENOT_SUPPORT, "not support join fs for mono partition");
  }

  auto miss_mds_fn = [&](const pb::mds::HashPartition& hash) -> bool {
    for (const auto& mds_id : quit_mds_ids) {
      if (hash.distributions().find(mds_id) != hash.distributions().end()) {
        return false;
      }
    }

    return true;
  };

  auto has_mds_fn = [&](const pb::mds::HashPartition& hash) -> bool {
    for (const auto& mds_id : join_mds_ids) {
      if (hash.distributions().find(mds_id) != hash.distributions().end()) {
        return true;
      }
    }

    return false;
  };

  std::vector<uint64_t> old_mds_ids;
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

    CHECK(HashPartitionHelper::CheckHashPartition(*hash)) << "invalid hash partition bucket id size.";

    DistributeByMean(join_mds_ids, *hash);

    CHECK(HashPartitionHelper::CheckHashPartition(*hash)) << "invalid hash partition bucket id size.";

    log.set_fs_name(FsName());
    log.set_fs_id(fs_id_);
    log.set_type(pb::mds::FsOpLog::QUIT_AND_JOIN_FS);
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

  RefreshFsInfo(result.fs_info, "quit_and_join_fs");

  NotifyBuddyRefreshFsInfo(old_mds_ids, result.fs_info);

  return Status::OK();
}

static void GetQuitAndJoinMdsIds(const std::vector<uint64_t>& old_mds_ids, const std::vector<uint64_t>& new_mds_ids,
                                 std::vector<uint64_t>& quit_mds_ids, std::vector<uint64_t>& join_mds_ids) {
  for (const auto& mds_id : old_mds_ids) {
    if (std::find(new_mds_ids.begin(), new_mds_ids.end(), mds_id) == new_mds_ids.end()) {  // NOLINT
      quit_mds_ids.push_back(mds_id);
    }
  }

  for (const auto& mds_id : new_mds_ids) {
    if (std::find(old_mds_ids.begin(), old_mds_ids.end(), mds_id) == old_mds_ids.end()) {  // NOLINT
      join_mds_ids.push_back(mds_id);
    }
  }
}

Status FileSystem::UpdatePartitionPolicy(const std::map<uint64_t, BucketSetEntry>& distributions,
                                         const std::string& reason) {
  auto handler = [&](PartitionPolicy& partition_policy, FsOpLog& log) -> Status {
    auto* hash = partition_policy.mutable_parent_hash();

    auto old_mds_ids = Helper::GetMdsIds(*hash);
    auto new_mds_ids = Helper::GetMdsIds(distributions);

    hash->mutable_distributions()->clear();
    for (const auto& [mds_id, bucket_set] : distributions) {
      hash->mutable_distributions()->insert({mds_id, bucket_set});
    }

    CHECK(HashPartitionHelper::CheckHashPartition(*hash)) << "invalid hash partition bucket id size.";

    std::vector<uint64_t> quit_mds_ids, join_mds_ids;
    GetQuitAndJoinMdsIds(old_mds_ids, new_mds_ids, quit_mds_ids, join_mds_ids);

    log.set_fs_name(FsName());
    log.set_fs_id(fs_id_);
    log.set_type(pb::mds::FsOpLog::QUIT_AND_JOIN_FS);
    Helper::VectorToPbRepeated(quit_mds_ids, log.mutable_quit_and_join_fs()->mutable_quit_mds_ids());
    Helper::VectorToPbRepeated(join_mds_ids, log.mutable_quit_and_join_fs()->mutable_join_mds_ids());
    log.set_comment(reason);

    return Status::OK();
  };

  Trace trace;
  UpdateFsPartitionOperation operation(trace, FsName(), handler);

  auto status = RunOperation(&operation);
  if (!status.ok()) return status;

  auto& result = operation.GetResult();

  RefreshFsInfo(result.fs_info, reason);

  return Status::OK();
}

void FileSystem::DescribeByJson(Json::Value& value) {
  value["fs_id"] = fs_id_;
  value["fs_name"] = fs_info_->GetName();
  value["uuid"] = fs_info_->GetUUID();
  value["version"] = fs_info_->GetVersion();

  Json::Value partition_cache;
  partition_cache_.DescribeByJson(partition_cache);
  value["partition_cache"] = partition_cache;

  Json::Value inode_cache;
  inode_cache_.DescribeByJson(inode_cache);
  value["inode_cache"] = inode_cache;

  Json::Value chunk_cache;
  chunk_cache_.DescribeByJson(chunk_cache);
  value["chunk_cache"] = chunk_cache;

  Json::Value parent_memo;
  parent_memo_->DescribeByJson(parent_memo);
  value["parent_memo"] = parent_memo;
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

  if (!IsExistMetaTable()) {
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
static std::map<uint64_t, BucketSetEntry> GenParentHashDistribution(const std::vector<MDSMeta>& mds_metas,
                                                                    uint32_t bucket_num) {
  std::map<uint64_t, BucketSetEntry> mds_bucket_map;
  for (const auto& mds_meta : mds_metas) {
    mds_bucket_map[mds_meta.ID()] = BucketSetEntry();
  }

  for (uint32_t i = 0; i < bucket_num; ++i) {
    const auto& mds_meta = mds_metas[i % mds_metas.size()];
    mds_bucket_map[mds_meta.ID()].add_bucket_ids(i);
  }

  return mds_bucket_map;
}

FsInfoEntry FileSystemSet::GenFsInfo(uint32_t fs_id, const CreateFsParam& param) {
  FsInfoEntry fs_info;
  fs_info.set_fs_id(fs_id);
  fs_info.set_fs_name(param.fs_name);
  fs_info.set_fs_type(param.fs_type);
  fs_info.set_root_ino(kRootIno);
  fs_info.set_status(pb::mds::FsStatus::NORMAL);
  fs_info.set_block_size(param.block_size);
  fs_info.set_chunk_size(param.chunk_size);
  fs_info.set_enable_sum_in_dir(param.enable_sum_in_dir);
  fs_info.set_owner(param.owner);
  fs_info.set_capacity(param.capacity);
  fs_info.set_recycle_time_hour(param.recycle_time_hour > 0 ? param.recycle_time_hour
                                                            : FLAGS_mds_filesystem_recycle_time_hour);
  fs_info.mutable_extra()->CopyFrom(param.fs_extra);
  fs_info.set_uuid(utils::UUIDGenerator::GenerateUUID());

  auto mds_metas = mds_meta_map_->GetAllMDSMeta();
  auto* partition_policy = fs_info.mutable_partition_policy();
  partition_policy->set_type(param.partition_type);
  partition_policy->set_epoch(1);
  if (param.partition_type == pb::mds::PartitionType::MONOLITHIC_PARTITION) {
    auto* mono = partition_policy->mutable_mono();
    if (param.candidate_mds_ids.empty()) {
      int select_offset = Helper::GenerateRealRandomInteger(0, 1000) % mds_metas.size();
      mono->set_mds_id(mds_metas.at(select_offset).ID());
    } else {
      mono->set_mds_id(param.candidate_mds_ids.front());
    }

  } else if (param.partition_type == pb::mds::PartitionType::PARENT_ID_HASH_PARTITION) {
    auto* parent_hash = partition_policy->mutable_parent_hash();
    parent_hash->set_bucket_num(FLAGS_mds_filesystem_hash_bucket_num);
    parent_hash->set_expect_mds_num(param.expect_mds_num == 0 ? FLAGS_mds_filesystem_hash_mds_num_default
                                                              : param.expect_mds_num);

    auto candidate_mds_metas = MdsHelper::RandomSelectMds(mds_metas, parent_hash->expect_mds_num());
    CHECK(!candidate_mds_metas.empty()) << "candidate_mds_metas is empty.";
    auto mds_bucket_map = GenParentHashDistribution(candidate_mds_metas, FLAGS_mds_filesystem_hash_bucket_num);
    for (const auto& [mds_id, bucket_set] : mds_bucket_map) {
      parent_hash->mutable_distributions()->insert({mds_id, bucket_set});
    }
  }

  fs_info.set_create_time_s(Helper::Timestamp());
  fs_info.set_last_update_time_ns(Helper::TimestampNs());

  return fs_info;
}

bool FileSystemSet::IsExistMetaTable() {
  auto range = MetaCodec::GetMetaTableRange();
  DINGO_LOG(DEBUG) << fmt::format("[fsset] check meta table, {}.", range.ToString());

  auto status = kv_storage_->IsExistTable(range.start, range.end);
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      DINGO_LOG(ERROR) << fmt::format("[fsset] check meta table exist fail, error({}).", status.error_str());
    }
    return false;
  }

  return true;
}

Status FileSystemSet::CreateFsMetaTable(uint32_t fs_id, const std::string& fs_name, int64_t& table_id) {
  auto range = MetaCodec::GetFsMetaTableRange(fs_id);
  KVStorage::TableOption option = {.start_key = range.start, .end_key = range.end};
  std::string table_name = GenFsMetaTableName(fs_name);
  Status status = kv_storage_->CreateTable(table_name, option, table_id);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, fmt::format("create fsmeta table fail, {}", status.error_str()));
  }

  return Status::OK();
}

Status FileSystemSet::DropFsMetaTable(uint32_t fs_id) {
  auto range = MetaCodec::GetFsMetaTableRange(fs_id);
  DINGO_LOG(INFO) << fmt::format("[fsset.{}] drop fsmeta table, range{}.", fs_id, range.ToString());

  return kv_storage_->DropTable(range);
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
Status FileSystemSet::CreateFs(const CreateFsParam& param, FsInfoEntry& fs_info) {
  auto status = ValidateCreateFsParam(param);
  if (!status.ok()) {
    return status;
  }

  // when create fs fail, clean up
  auto cleanup = [&](uint32_t fs_id, int64_t table_id, const std::string& fs_key, const std::string& quota_key) {
    // clean fsmeta table
    if (table_id > 0) {
      auto status = kv_storage_->DropTable(table_id);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("[fsset.{}] clean fsmeta table fail, table_id({}) status({})", fs_id, table_id,
                                  status.error_str());
      }
    }

    // clean fs info
    if (!fs_key.empty()) {
      auto status = kv_storage_->Delete(fs_key);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("[fsset.{}] clean fs info fail, status({})", fs_id, status.error_str());
      }
    }

    // clean quota
    if (!quota_key.empty()) {
      auto status = kv_storage_->Delete(quota_key);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("[fsset.{}] clean quota info fail, status({})", fs_id, status.error_str());
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
  uint32_t fs_id;
  if (param.fs_id == 0) {
    status = GenFsId(fs_id);
    if (BAIDU_UNLIKELY(!status.ok())) {
      return status;
    }
  } else {
    fs_id = param.fs_id;
  }

  // check fs_id exist
  if (IsExistFileSystem(fs_id)) {
    return Status(pb::error::EEXISTED, fmt::format("fs({}) exist.", param.fs_name));
  }

  // create dentry/inode table
  int64_t table_id = 0;
  status = CreateFsMetaTable(fs_id, param.fs_name, table_id);
  if (!status.ok()) return status;

  fs_info = GenFsInfo(fs_id, param);

  // create fs
  Trace trace;
  CreateFsOperation operation(trace, fs_info);
  status = RunOperation(&operation);
  if (!status.ok()) {
    cleanup(fs_id, table_id, "", "");
    return Status(pb::error::EBACKEND_STORE, fmt::format("put store fs fail, {}", status.error_str()));
  }

  // create FileSystem instance
  auto ino_id_generator = NewInodeIdGenerator(fs_id, coordinator_client_, kv_storage_);
  CHECK(ino_id_generator != nullptr) << "new id generator fail.";

  auto fs = FileSystem::New(self_mds_meta_.ID(), FsInfo::New(fs_info), std::move(ino_id_generator), slice_id_generator_,
                            kv_storage_, operation_processor_, mds_meta_map_, quota_worker_set_, notify_buddy_);
  if (!fs->Init()) {
    cleanup(fs_id, table_id, fs_key, "");
    return Status(pb::error::EINTERNAL, "init FileSystem fail");
  }

  // set quota
  status = fs->CreateQuota();
  if (!status.ok()) {
    cleanup(fs_id, table_id, fs_key, "");
    return Status(pb::error::EINTERNAL, fmt::format("create quota fail, {}", status.error_str()));
  }

  // create root inode
  status = fs->CreateRoot();
  if (!status.ok()) {
    cleanup(fs_id, table_id, fs_key, MetaCodec::EncodeFsQuotaKey(fs_id));
    return Status(pb::error::EINTERNAL, fmt::format("create root fail, {}", status.error_str()));
  }

  CHECK(AddFileSystem(fs, true)) << fmt::format("add filesystem({}) fail, already exist.", fs_id);

  return Status::OK();
}

Status FileSystemSet::MountFs(Context& ctx, const std::string& fs_name, const pb::mds::MountPoint& mountpoint) {
  CHECK(!fs_name.empty()) << "fs name is empty.";

  auto& trace = ctx.GetTrace();

  MountFsOperation operation(trace, fs_name, mountpoint);

  auto status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fsset.{}] mount fs finish, mountpoint({}) status({}).", fs_name,
                                 mountpoint.ShortDebugString(), status.error_str());

  return status;
}

Status FileSystemSet::UmountFs(Context& ctx, const std::string& fs_name, const std::string& client_id) {
  CHECK(!fs_name.empty()) << "fs name is empty.";

  auto& trace = ctx.GetTrace();

  UmountFsOperation operation(trace, fs_name, client_id);

  auto status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fsset.{}] umount fs finish, client({}) status({}).", fs_name, client_id,
                                 status.error_str());
  if (!status.ok() && status.error_code() != pb::error::ENOT_FOUND) {
    return status;
  }

  return Status::OK();
}

// check if fs is mounted
Status FileSystemSet::DeleteFs(Context& ctx, const std::string& fs_name, bool is_force) {
  CHECK(!fs_name.empty()) << "fs name is empty.";

  auto& trace = ctx.GetTrace();

  DeleteFsOperation operation(trace, fs_name, is_force);

  auto status = RunOperation(&operation);

  DINGO_LOG(INFO) << fmt::format("[fsset.{}] delete fs finish, status({}).", fs_name, status.error_str());

  auto& result = operation.GetResult();
  auto& fs_info = result.fs_info;

  if (status.ok()) {
    DeleteFileSystem(fs_info.fs_id());
  }

  return status;
}

Status FileSystemSet::UpdateFsInfo(Context& ctx, const std::string& fs_name, const FsInfoEntry& fs_info) {
  auto trace = ctx.GetTrace();

  UpdateFsOperation operation(trace, fs_name, fs_info);

  auto status = RunOperation(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[fsset.{}] update fs info fail, status({}).", fs_name, status.error_str());
    return status;
  }

  return Status::OK();
}

Status FileSystemSet::GetFsInfo(Context& ctx, const std::string& fs_name, FsInfoEntry& fs_info) {
  auto& trace = ctx.GetTrace();

  GetFsOperation operation(trace, fs_name);

  auto status = RunOperation(&operation);
  if (!status.ok()) return status;

  auto& result = operation.GetResult();

  fs_info = result.fs_info;

  return Status::OK();
}

Status FileSystemSet::GetAllFsInfo(Context& ctx, bool include_deleted, std::vector<FsInfoEntry>& fs_infoes) {
  auto& trace = ctx.GetTrace();

  ScanFsOperation operation(trace);
  operation.SetIsolationLevel(Txn::kReadCommitted);

  auto status = RunOperation(&operation);
  if (!status.ok()) return status;

  auto& all_fs_infoes = operation.GetResult().fs_infoes;
  for (const auto& fs_info : all_fs_infoes) {
    if (include_deleted || !fs_info.is_deleted()) {
      fs_infoes.push_back(fs_info);
    }
  }

  return Status::OK();
}

Status FileSystemSet::GetDeletedFsInfo(Context& ctx, std::vector<FsInfoEntry>& fs_infoes) {
  std::vector<FsInfoEntry> all_fs_infoes;
  auto status = GetAllFsInfo(ctx, true, all_fs_infoes);
  if (!status.ok()) return status;

  for (auto& fs_info : all_fs_infoes) {
    if (fs_info.is_deleted()) {
      fs_infoes.push_back(fs_info);
    }
  }

  return Status::OK();
}

Status FileSystemSet::RefreshFsInfo(const std::string& fs_name, const std::string& reason) {
  auto fs = GetFileSystem(fs_name);
  if (fs == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}).", fs_name));
  }

  return fs->RefreshFsInfo(reason);
}

Status FileSystemSet::RefreshFsInfo(uint32_t fs_id, const std::string& reason) {
  auto fs = GetFileSystem(fs_id);
  if (fs == nullptr) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}).", fs_id));
  }

  return fs->RefreshFsInfo(reason);
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

bool FileSystemSet::IsExistFileSystem(uint32_t fs_id) {
  utils::ReadLockGuard lk(lock_);

  return fs_map_.find(fs_id) != fs_map_.end();
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

std::string FileSystemSet::GetFsName(uint32_t fs_id) {
  utils::ReadLockGuard lk(lock_);

  auto it = fs_map_.find(fs_id);
  return it != fs_map_.end() ? it->second->FsName() : "";
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

std::vector<std::string> FileSystemSet::GetAllClientId() {
  utils::ReadLockGuard lk(lock_);

  std::vector<std::string> client_ids;
  for (const auto& [fs_id, fs] : fs_map_) {
    auto fs_info = fs->GetFsInfo();
    for (const auto& mountpoint : fs_info.mount_points()) {
      client_ids.push_back(mountpoint.client_id());
    }
  }

  return client_ids;
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

  if (fs->PartitionType() == pb::mds::PartitionType::MONOLITHIC_PARTITION) {
    if (mds_ids.size() > 1) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "not support join mono fs with multiple mds");
    }

    return fs->JoinMonoFs(ctx, mds_ids.front(), reason);

  } else if (fs->PartitionType() == pb::mds::PartitionType::PARENT_ID_HASH_PARTITION) {
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

  if (fs->PartitionType() == pb::mds::PartitionType::MONOLITHIC_PARTITION) {
    return Status(pb::error::EILLEGAL_PARAMTETER, "not support mono fs quit fs");

  } else if (fs->PartitionType() == pb::mds::PartitionType::PARENT_ID_HASH_PARTITION) {
    return fs->QuitFs(ctx, mds_ids, reason);

  } else {
    return Status(pb::error::ENOT_SUPPORT, reason);
  }

  return Status::OK();
}

Status FileSystemSet::GetFileSessions(uint32_t fs_id, std::vector<FileSessionEntry>& file_sessions) {
  Trace trace;
  ScanFileSessionOperation operation(trace, fs_id, [&](const FileSessionEntry& file_session) -> bool {
    file_sessions.push_back(file_session);
    return true;
  });
  operation.SetIsolationLevel(Txn::kReadCommitted);

  return RunOperation(&operation);
}

Status FileSystemSet::GetDelFiles(uint32_t fs_id, std::vector<AttrEntry>& delfiles) {
  Trace trace;
  uint32_t count = 0;
  ScanDelFileOperation operation(trace, fs_id, [&](const std::string&, const std::string& value) -> bool {
    delfiles.push_back(MetaCodec::DecodeDelFileValue(value));
    ++count;
    return true;
  });
  operation.SetIsolationLevel(Txn::kReadCommitted);

  return RunOperation(&operation);
}

Status FileSystemSet::GetDelSlices(uint32_t fs_id, std::vector<TrashSliceList>& delslices) {
  Trace trace;
  uint32_t count = 0;
  ScanDelSliceOperation operation(trace, fs_id, [&](const std::string&, const std::string& value) -> bool {
    delslices.push_back(MetaCodec::DecodeDelSliceValue(value));
    ++count;
    return true;
  });
  operation.SetIsolationLevel(Txn::kReadCommitted);

  return RunOperation(&operation);
}

Status FileSystemSet::GetFsOpLogs(uint32_t fs_id, std::vector<FsOpLog>& fs_op_logs) {
  Trace trace;
  uint32_t count = 0;
  ScanFsOpLogOperation operation(trace, fs_id, [&](const FsOpLog& oplog) -> bool {
    fs_op_logs.push_back(oplog);
    ++count;
    return true;
  });
  operation.SetIsolationLevel(Txn::kReadCommitted);

  return RunOperation(&operation);
}

bool FileSystemSet::LoadFileSystems() {
  Context ctx;
  std::vector<FsInfoEntry> fs_infoes;
  auto status = GetAllFsInfo(ctx, true, fs_infoes);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[fsset] get all fs info fail, error({}).", status.error_str());
    return false;
  }

  for (const auto& fs_info : fs_infoes) {
    if (fs_info.is_deleted()) {
      if (IsExistFileSystem(fs_info.fs_id())) {
        DINGO_LOG(INFO) << fmt::format("[fsset.{}.{}] fs is deleted, clean up.", fs_info.fs_name(), fs_info.fs_id());

        DeleteFileSystem(fs_info.fs_id());
      }
      continue;
    }

    auto fs = GetFileSystem(fs_info.fs_id());
    if (fs != nullptr) {
      if (fs->UUID() == fs_info.uuid()) {
        // existing fs, just refresh info
        fs->RefreshFsInfo(fs_info, "load_fs");
        continue;

      } else {
        // delete old fs, maybe recreated
        DeleteFileSystem(fs_info.fs_id());
        DINGO_LOG(INFO) << fmt::format("[fsset.{}] fs uuid not match, maybe fs deleted and recreated, uuid({}->{})",
                                       fs_info.fs_name(), fs_info.fs_id(), fs->UUID(), fs_info.uuid());
      }
    }

    // add new fs
    DINGO_LOG(INFO) << fmt::format("[fsset.{}.{}] add new fs.", fs_info.fs_name(), fs_info.fs_id());

    auto ino_id_generator = NewInodeIdGenerator(fs_info.fs_id(), coordinator_client_, kv_storage_);
    CHECK(ino_id_generator != nullptr) << "new id generator fail.";

    fs = FileSystem::New(self_mds_meta_.ID(), FsInfo::New(fs_info), std::move(ino_id_generator), slice_id_generator_,
                         kv_storage_, operation_processor_, mds_meta_map_, quota_worker_set_, notify_buddy_);
    if (!fs->Init()) {
      DINGO_LOG(ERROR) << fmt::format("[fsset.{}.{}] init filesystem fail.", fs_info.fs_name(), fs_info.fs_id());
      continue;
    }

    if (!AddFileSystem(fs)) {
      DINGO_LOG(WARNING) << fmt::format("[fsset.{}.{}] add filesystem fail, already exist.", fs_info.fs_name(),
                                        fs->FsId());
    }
  }

  return true;
}

Status FileSystemSet::DestroyFsResource(uint32_t fs_id) {
  // fsmeta table
  auto status = DropFsMetaTable(fs_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[fsset.{}] drop fsmeta table fail, status({}).", fs_id, status.error_str());
    return status;
  }

  // inode id generator
  DestroyInodeIdGenerator(fs_id, coordinator_client_, kv_storage_);

  return Status::OK();
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

void FileSystemSet::DescribeByJson(Json::Value& value) {
  auto fses = GetAllFileSystem();

  value["count"] = fses.size();

  Json::Value fsset_value(Json::arrayValue);
  for (const auto& fs : fses) {
    Json::Value fs_value;
    fs->DescribeByJson(fs_value);
    fsset_value.append(fs_value);
  }
  value["filesystems"] = fsset_value;
}

}  // namespace mds
}  // namespace dingofs
