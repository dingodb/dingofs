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

#include "mds/filesystem/store_operation.h"

#include <fcntl.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "brpc/reloadable_flags.h"
#include "bthread/bthread.h"
#include "bthread/types.h"
#include "common/const.h"
#include "common/meta.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/codec.h"
#include "mds/common/helper.h"
#include "mds/common/status.h"
#include "mds/common/trash.h"
#include "mds/common/type.h"
#include "mds/storage/storage.h"
#include "utils/time.h"
#include "utils/uuid.h"

namespace dingofs {
namespace mds {

DEFINE_uint32(mds_store_operation_batch_size, 64, "process operation batch size.");
DEFINE_validator(mds_store_operation_batch_size, brpc::PassValidate);

DEFINE_uint32(mds_txn_max_retry_times, 5, "txn max retry times.");
DEFINE_validator(mds_txn_max_retry_times, brpc::PassValidate);

DEFINE_uint32(mds_txn_timeout_ms, 8000, "txn timeout ms.");
DEFINE_validator(mds_txn_timeout_ms, brpc::PassValidate);

DEFINE_bool(mds_store_operation_wait_multi_time, false, "wait multi time before retry.");
DEFINE_validator(mds_store_operation_wait_multi_time, brpc::PassValidate);

DEFINE_uint32(mds_store_operation_merge_delay_us, 50, "merge operation delay us.");
DEFINE_validator(mds_store_operation_merge_delay_us, brpc::PassValidate);

DEFINE_bool(mds_tiny_file_data_enable, false, "enable tiny file data feature.");
DEFINE_validator(mds_tiny_file_data_enable, brpc::PassValidate);

DECLARE_uint32(mds_filesession_live_time_s);

static const uint32_t kOpNameBufInitSize = 128;
static const uint32_t kCleanCompactedSliceIntervalS = 180;  // 3 minutes

static constexpr uint32_t kTryMaxCount = 10;

static uint32_t CalWaitTimeUs(int retry) {
  // exponential backoff
  return Helper::GenerateRealRandomInteger(1000, 5000) * (1 << retry);
}

static bool IsRetry(uint32_t& retry) {
  if (++retry <= FLAGS_mds_txn_max_retry_times) {
    bthread_usleep(CalWaitTimeUs(retry));
    return true;
  }
  return false;
}

static std::string FindValue(const absl::flat_hash_map<std::string_view, std::string_view>& index,
                             const std::string& key) {
  auto it = index.find(key);
  return it != index.end() ? std::string(it->second) : std::string();
}

static std::string FindValue(const std::vector<KeyValue>& kvs, const std::string& key) {
  for (const auto& kv : kvs) {
    if (kv.key == key) {
      return kv.value;
    }
  }

  return "";
}

static void AddParentIno(AttrEntry& attr, Ino parent) {
  attr.add_parents(parent);
  // auto it = std::find(attr.parents().begin(), attr.parents().end(), parent);
  // if (it == attr.parents().end()) {
  //   attr.add_parents(parent);
  // }
}

static void DelParentIno(AttrEntry& attr, Ino parent) {
  auto it = std::find(attr.parents().begin(), attr.parents().end(), parent);
  if (it != attr.parents().end()) {
    attr.mutable_parents()->erase(it);
  }
}
static void DelSliceRefIno(SliceRefEntry& slice_ref, Ino ino) {
  auto it = std::find(slice_ref.inos().begin(), slice_ref.inos().end(), ino);
  if (it != slice_ref.inos().end()) {
    slice_ref.mutable_inos()->erase(it);
  }
}

static void SetError(BatchOperation& batch_operation, const Status& status) {
  for (auto* operation : batch_operation.setattr_operations) {
    if (operation->GetStatus().ok()) operation->SetStatus(status);
  }

  for (auto* operation : batch_operation.create_operations) {
    if (operation->GetStatus().ok()) operation->SetStatus(status);
  }
}

static void SetResultAttr(BatchOperation& batch_operation, Operation::BatchSharedParam& shared_param) {
  for (auto* operation : batch_operation.setattr_operations) {
    if (operation->GetStatus().ok()) operation->SetResultAttr(shared_param);
  }

  for (auto* operation : batch_operation.create_operations) {
    if (operation->GetStatus().ok()) operation->SetResultAttr(shared_param);
  }
}

static void Notify(BatchOperation& batch_operation) {
  for (auto* operation : batch_operation.setattr_operations) {
    operation->NotifyEvent();
  }

  for (auto* operation : batch_operation.create_operations) {
    operation->NotifyEvent();
  }
}

static void SetTrace(BatchOperation& batch_operation, const Trace::Txn& txn_trace) {
  for (auto* operation : batch_operation.setattr_operations) {
    operation->GetTrace().AddTxn(txn_trace);
  }

  for (auto* operation : batch_operation.create_operations) {
    operation->GetTrace().AddTxn(txn_trace);
  }
}

static void SetElapsedTime(BatchOperation& batch_operation, const std::string& name) {
  for (auto* operation : batch_operation.setattr_operations) {
    operation->GetTrace().RecordElapsedTime(name);
  }

  for (auto* operation : batch_operation.create_operations) {
    operation->GetTrace().RecordElapsedTime(name);
  }
}

static bool IsExistMountPoint(const FsInfoEntry& fs_info, const pb::mds::MountPoint& mountpoint) {
  for (const auto& mp : fs_info.mount_points()) {
    if (mp.client_id() == mountpoint.client_id()) {
      return true;
    }
  }

  return false;
}

const char* Operation::OpName() const {
  switch (GetOpType()) {
    case OpType::kCreateFs:
      return "CreateFs";

    case OpType::kGetFs:
      return "GetFs";

    case OpType::kMountFs:
      return "MountFs";

    case OpType::kUmountFs:
      return "UmountFs";

    case OpType::kDeleteFs:
      return "DeleteFs";

    case OpType::kCleanFs:
      return "CleanFs";

    case OpType::kUpdateFs:
      return "UpdateFs";

    case OpType::kUpdateFsPartition:
      return "UpdateFsPartition";

    case OpType::kUpdateFsState:
      return "UpdateFsState";

    case OpType::kUpdateFsRecycleProgress:
      return "UpdateFsRecycleProgress";

    case OpType::kCreateRoot:
      return "CreateRoot";

    case OpType::kMkDir:
      return "MkDir";

    case OpType::kMkNod:
      return "MkNod";

    case OpType::kBatchMkNod:
      return "BatchMkNod";

    case OpType::kBatchCreateFile:
      return "BatchCreateFile";

    case OpType::kHardLink:
      return "HardLink";

    case OpType::kSymLink:
      return "SymLink";

    case OpType::kUpdateAttr:
      return "UpdateAttr";

    case OpType::kUpdateXAttr:
      return "UpdateXAttr";

    case OpType::kRemoveXAttr:
      return "RemoveXAttr";

    case OpType::kUpdateShardBoundaries:
      return "UpdateShardBoundaries";

    case OpType::kFallocate:
      return "Fallocate";

    case OpType::kOpenFile:
      return "OpenFile";

    case OpType::kCloseFile:
      return "CloseFile";

    case OpType::kFlushFile:
      return "FlushFile";

    case OpType::kRmDir:
      return "RmDir";

    case OpType::kUnlink:
      return "Unlink";

    case OpType::kBatchUnlink:
      return "BatchUnlink";

    case OpType::kRename:
      return "Rename";

    case OpType::kBatchTrashUnlink:
      return "BatchTrashUnlink";

    case OpType::kCleanTrashBucket:
      return "CleanTrashBucket";

    case OpType::kCompactChunk:
      return "CompactChunk";

    case OpType::kUpsertChunk:
      return "UpsertChunk";

    case OpType::kGetChunk:
      return "GetChunk";

    case OpType::kScanChunk:
      return "ScanChunk";

    case OpType::kCleanChunk:
      return "CleanChunk";

    case OpType::kGetSliceRef:
      return "GetSliceRef";

    case OpType::kDecSliceRef:
      return "DecSliceRef";

    case OpType::kScanSliceRef:
      return "ScanSliceRef";

    case OpType::kCopyFileRange:
      return "CopyFileRange";

    case OpType::kSetFsQuota:
      return "SetFsQuota";

    case OpType::kGetFsQuota:
      return "GetFsQuota";

    case OpType::kFlushFsUsage:
      return "FlushFsUsage";

    case OpType::kDeleteFsQuota:
      return "DeleteFsQuota";

    case OpType::kSetDirQuota:
      return "SetDirQuota";

    case OpType::kDeleteDirQuota:
      return "DeleteDirQuota";

    case OpType::kLoadDirQuotas:
      return "LoadDirQuotas";

    case OpType::kFlushDirUsages:
      return "FlushDirUsages";

    case OpType::kUpsertMds:
      return "UpsertMds";

    case OpType::kDeleteMds:
      return "DeleteMds";

    case OpType::kScanMds:
      return "ScanMds";

    case OpType::kUpsertClient:
      return "UpsertClient";

    case OpType::kDeleteClient:
      return "DeleteClient";

    case OpType::kScanClient:
      return "ScanClient";

    case OpType::kGetFileSession:
      return "GetFileSession";

    case OpType::kScanFileSession:
      return "ScanFileSession";

    case OpType::kKeepAliveFileSession:
      return "KeepAliveFileSession";

    case OpType::kDeleteFileSession:
      return "DeleteFileSession";

    case OpType::kCleanDelSlice:
      return "CleanDelSlice";

    case OpType::kCleanDelFile:
      return "CleanDelFile";

    case OpType::kScanLock:
      return "ScanLock";

    case OpType::kScanFs:
      return "ScanFs";

    case OpType::kScanDentry:
      return "ScanDentry";

    case OpType::kScanTrashDentry:
      return "ScanTrashDentry";

    case OpType::kScanDirShard:
      return "ScanDirShard";

    case OpType::kScanDelFile:
      return "ScanDelFile";

    case OpType::kScanDirStat:
      return "ScanDirStat";

    case OpType::kScanDelSlice:
      return "ScanDelSlice";

    case OpType::kScanMetaTable:
      return "ScanMetaTable";

    case OpType::kScanFsMetaTable:
      return "ScanFsMetaTable";

    case OpType::kScanFsOpLog:
      return "ScanFsOpLog";

    case OpType::kSaveFsStats:
      return "SaveFsStats";

    case OpType::kScanFsStats:
      return "ScanFsStats";

    case OpType::kGetAndCompactFsStats:
      return "GetAndCompactFsStats";

    case OpType::kGetInodeAttr:
      return "GetInodeAttr";

    case OpType::kBatchGetInodeAttr:
      return "BatchGetInodeAttr";

    case OpType::kGetDentry:
      return "GetDentry";

    case OpType::kImportKV:
      return "ImportKV";

    case OpType::kUpsertCacheMember:
      return "UpsertCacheMember";

    case OpType::kDeleteCacheMember:
      return "DeleteCacheMember";

    case OpType::kScanCacheMember:
      return "ScanCacheMember";

    case OpType::kGetCacheMember:
      return "GetCacheMember";

    case OpType::kDeleteDirStat:
      return "DeleteDirStat";

    case OpType::kBatchSetDirStat:
      return "BatchSetDirStat";

    case OpType::kGetDirStat:
      return "GetDirStat";

    case OpType::kFlushDirStats:
      return "FlushDirStats";

    default:
      return "UnknownOperation";
  }

  return nullptr;
}

Status CreateFsOperation::Run(TxnUPtr& txn) {
  fs_info_.set_version(1);
  txn->Put(MetaCodec::EncodeFsKey(fs_info_.fs_name()), MetaCodec::EncodeFsValue(fs_info_));

  // add fs op log
  FsOpLog fs_config_log;

  fs_config_log.set_fs_name(fs_info_.fs_name());
  fs_config_log.set_fs_id(fs_info_.fs_id());
  fs_config_log.set_type(pb::mds::FsOpLog_Type_CREATE_FS);
  fs_config_log.set_comment("CreateFs");
  fs_config_log.set_time_ms(GetTime() / 1e6);

  txn->Put(MetaCodec::EncodeFsOpLogKey(fs_info_.fs_id(), GetTime()), MetaCodec::EncodeFsOpLogValue(fs_config_log));

  return Status::OK();
}

Status GetFsOperation::Run(TxnUPtr& txn) {
  std::string value;
  Status status = txn->Get(MetaCodec::EncodeFsKey(fs_name_), value);
  if (!status.ok()) return status;

  result_.fs_info = MetaCodec::DecodeFsValue(value);

  return Status::OK();
}

Status MountFsOperation::Run(TxnUPtr& txn) {
  std::string value;
  std::string key = MetaCodec::EncodeFsKey(fs_name_);
  Status status = txn->Get(key, value);
  if (!status.ok()) return status;

  auto fs_info = MetaCodec::DecodeFsValue(value);

  if (IsExistMountPoint(fs_info, mount_point_)) {
    return Status(pb::error::EEXISTED, "mountPoint already exist.");
  }

  fs_info.add_mount_points()->CopyFrom(mount_point_);
  fs_info.set_last_update_time_ns(GetTime());
  fs_info.set_version(fs_info.version() + 1);

  txn->Put(key, MetaCodec::EncodeFsValue(fs_info));

  // add client heartbeat, prevent leave over mountpoint
  ClientEntry client;
  client.set_id(mount_point_.client_id());
  client.set_hostname(mount_point_.hostname());
  client.set_ip(mount_point_.ip());
  client.set_port(mount_point_.port());
  client.set_mountpoint(mount_point_.path());
  client.set_fs_name(fs_info.fs_name());
  client.set_create_time_ms(utils::TimestampMs());
  client.set_last_online_time_ms(utils::TimestampMs());

  txn->Put(MetaCodec::EncodeHeartbeatKey(client.id()), MetaCodec::EncodeHeartbeatValue(client));

  return Status::OK();
}

static bool RemoveMountPoint(FsInfoEntry& fs_info, const std::string& client_id) {
  for (int i = 0; i < fs_info.mount_points_size(); i++) {
    if (fs_info.mount_points(i).client_id() == client_id) {
      fs_info.mutable_mount_points()->SwapElements(i, fs_info.mount_points_size() - 1);
      fs_info.mutable_mount_points()->RemoveLast();
      return true;
    }
  }

  return false;
}

Status UmountFsOperation::Run(TxnUPtr& txn) {
  std::string value;
  std::string key = MetaCodec::EncodeFsKey(fs_name_);
  Status status = txn->Get(key, value);
  if (!status.ok()) return status;

  auto fs_info = MetaCodec::DecodeFsValue(value);

  if (!RemoveMountPoint(fs_info, client_id_)) {
    return Status::OK();
  }

  fs_info.set_last_update_time_ns(GetTime());
  fs_info.set_version(fs_info.version() + 1);

  txn->Put(key, MetaCodec::EncodeFsValue(fs_info));

  return Status::OK();
}

Status DeleteFsOperation::Run(TxnUPtr& txn) {
  std::string value;
  auto status = txn->Get(MetaCodec::EncodeFsKey(fs_name_), value);
  if (!status.ok()) {
    if (status.error_code() == pb::error::ENOT_FOUND) {
      return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}), {}.", fs_name_, status.error_str()));
    }
    return status;
  }

  auto fs_info = MetaCodec::DecodeFsValue(value);
  if (!is_force_ && fs_info.mount_points_size() > 0) {
    return Status(pb::error::EEXISTED, "fs exist mount point.");
  }

  // rename fs name, add suffix DELETED+<fs_id> to avoid conflict with new fs with same name
  fs_info.set_fs_name(fmt::format("{}_DELETED_{}", fs_name_, fs_info.fs_id()));
  fs_info.set_status(pb::mds::FsStatus::DELETED);
  fs_info.set_is_deleted(true);
  fs_info.set_delete_time_s(utils::Timestamp());

  txn->Put(MetaCodec::EncodeFsKey(fs_info.fs_name()), MetaCodec::EncodeFsValue(fs_info));
  txn->Delete(MetaCodec::EncodeFsKey(fs_name_));

  // add fs op log
  FsOpLog fs_config_log;

  fs_config_log.set_fs_name(fs_info.fs_name());
  fs_config_log.set_fs_id(fs_info.fs_id());
  fs_config_log.set_type(pb::mds::FsOpLog_Type_DELETE_FS);
  fs_config_log.set_comment("DeleteFs");
  fs_config_log.set_time_ms(GetTime() / 1e6);

  txn->Put(MetaCodec::EncodeFsOpLogKey(fs_info.fs_id(), GetTime()), MetaCodec::EncodeFsOpLogValue(fs_config_log));

  result_.fs_info = fs_info;

  return Status::OK();
}

Status CleanFsOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ > 0) << "fs_id is zero.";
  CHECK(!fs_name_.empty()) << "fs_name is empty.";

  // clean fs op log
  std::vector<std::string> keys;
  Range range = MetaCodec::GetFsConfigLogRange(fs_id_);
  auto status = txn->Scan(range, [&](const std::string& key, const std::string&) -> bool {
    keys.push_back(key);
    return true;
  });
  if (!status.ok()) return status;

  for (const auto& key : keys) txn->Delete(key);

  txn->Delete(MetaCodec::EncodeFsKey(fs_name_));

  return Status::OK();
}

Status UpdateFsOperation::Run(TxnUPtr& txn) {
  std::string fs_key = MetaCodec::EncodeFsKey(fs_name_);

  std::string value;
  auto status = txn->Get(fs_key, value);
  if (!status.ok()) return status;

  auto new_fs_info = MetaCodec::DecodeFsValue(value);
  if (fs_info_.capacity() > 0) new_fs_info.set_capacity(fs_info_.capacity());
  if (fs_info_.block_size() > 0) new_fs_info.set_block_size(fs_info_.block_size());
  if (!fs_info_.owner().empty()) new_fs_info.set_owner(fs_info_.owner());
  if (fs_info_.recycle_time_hour() > 0) new_fs_info.set_recycle_time_hour(fs_info_.recycle_time_hour());
  new_fs_info.set_trash_days(fs_info_.trash_days());
  // immediate_trash_quota is create-time only: deliberately not merged here.
  // enable_uid_gid_map is a runtime-mutable toggle: callers are expected to
  // GetFsInfo, flip the flag, and UpdateFsInfo with the full record (the
  // same read-modify-write convention used for trash_days above).
  new_fs_info.set_enable_uid_gid_map(fs_info_.enable_uid_gid_map());
  // enable_dir_stats is likewise a runtime-mutable toggle (hot-switchable):
  // same GetFsInfo -> flip -> UpdateFsInfo read-modify-write convention.
  new_fs_info.set_enable_dir_stats(fs_info_.enable_dir_stats());
  if (fs_info_.has_extra() && fs_info_.extra().has_s3_info()) {
    const auto& s3_info = fs_info_.extra().s3_info();
    auto* mut_s3_info = new_fs_info.mutable_extra()->mutable_s3_info();
    if (!s3_info.ak().empty()) mut_s3_info->set_ak(s3_info.ak());
    if (!s3_info.sk().empty()) mut_s3_info->set_sk(s3_info.sk());
    if (!s3_info.endpoint().empty()) mut_s3_info->set_endpoint(s3_info.endpoint());
    if (!s3_info.bucketname().empty()) mut_s3_info->set_bucketname(s3_info.bucketname());

  } else if (fs_info_.has_extra() && fs_info_.extra().has_rados_info()) {
    const auto& rados_info = fs_info_.extra().rados_info();
    auto* mut_rados_info = new_fs_info.mutable_extra()->mutable_rados_info();

    if (!rados_info.mon_host().empty()) mut_rados_info->set_mon_host(rados_info.mon_host());
    if (!rados_info.user_name().empty()) mut_rados_info->set_user_name(rados_info.user_name());
    if (!rados_info.key().empty()) mut_rados_info->set_key(rados_info.key());
    if (!rados_info.cluster_name().empty()) mut_rados_info->set_cluster_name(rados_info.cluster_name());
    if (!rados_info.pool_name().empty()) mut_rados_info->set_pool_name(rados_info.pool_name());
  }

  new_fs_info.set_version(new_fs_info.version() + 1);

  txn->Put(fs_key, MetaCodec::EncodeFsValue(new_fs_info));

  return Status::OK();
}

Status UpdateFsPartitionOperation::Run(TxnUPtr& txn) {
  std::string fs_key = MetaCodec::EncodeFsKey(fs_name_);

  std::string value;
  auto status = txn->Get(fs_key, value);
  if (!status.ok()) return status;

  auto fs_info = MetaCodec::DecodeFsValue(value);
  auto* partition_policy = fs_info.mutable_partition_policy();

  FsOpLog fs_config_log;
  status = handler_(*partition_policy, fs_config_log);
  if (!status.ok()) {
    return status;
  }

  partition_policy->set_epoch(partition_policy->epoch() + 1);

  fs_info.set_version(fs_info.version() + 1);

  txn->Put(fs_key, MetaCodec::EncodeFsValue(fs_info));
  result_.fs_info = fs_info;

  // log
  fs_config_log.set_time_ms(GetTime() / 1e6);
  txn->Put(MetaCodec::EncodeFsOpLogKey(fs_info.fs_id(), GetTime()), MetaCodec::EncodeFsOpLogValue(fs_config_log));

  return Status::OK();
}

Status UpdateFsStateOperation::Run(TxnUPtr& txn) {
  std::string fs_key = MetaCodec::EncodeFsKey(fs_name_);

  std::string value;
  auto status = txn->Get(fs_key, value);
  if (!status.ok()) return status;

  auto fs_info = MetaCodec::DecodeFsValue(value);

  auto old_fs_status = fs_info.status();
  fs_info.set_status(status_);
  fs_info.set_version(fs_info.version() + 1);

  txn->Put(fs_key, MetaCodec::EncodeFsValue(fs_info));

  // add fs op log
  FsOpLog fs_config_log;

  fs_config_log.set_fs_name(fs_info.fs_name());
  fs_config_log.set_fs_id(fs_info.fs_id());
  fs_config_log.set_type(pb::mds::FsOpLog_Type_UPDATE_STATE_FS);
  fs_config_log.set_comment("UpdateFsState");
  fs_config_log.set_time_ms(GetTime() / 1e6);
  fs_config_log.mutable_update_state_fs()->set_old_status(old_fs_status);
  fs_config_log.mutable_update_state_fs()->set_new_status(status_);

  txn->Put(MetaCodec::EncodeFsOpLogKey(fs_info.fs_id(), GetTime()), MetaCodec::EncodeFsOpLogValue(fs_config_log));

  return Status::OK();
}

Status UpdateFsRecycleProgressOperation::Run(TxnUPtr& txn) {
  std::string fs_key = MetaCodec::EncodeFsKey(fs_name_);

  std::string value;
  auto status = txn->Get(fs_key, value);
  if (!status.ok()) return status;

  auto fs_info = MetaCodec::DecodeFsValue(value);

  auto* recycle_progress = fs_info.mutable_recycle_progress();
  recycle_progress->set_last_ino(ino_);
  recycle_progress->set_last_time_ms(GetTime() / 1e6);

  txn->Put(fs_key, MetaCodec::EncodeFsValue(fs_info));

  return Status::OK();
}

Status CreateRootOperation::Run(TxnUPtr& txn) {
  const uint32_t fs_id = attr_.fs_id();

  txn->Put(MetaCodec::EncodeInodeKey(fs_id, attr_.ino()), MetaCodec::EncodeInodeValue(attr_));

  txn->Put(MetaCodec::EncodeDentryKey(fs_id, dentry_.ParentIno(), dentry_.Name()),
           MetaCodec::EncodeDentryValue(dentry_.Copy()));

  return Status::OK();
}

Status MkDirOperation::RunInBatch(TxnUPtr& txn, BatchSharedParam& shared_param) {
  auto& parent_attr = shared_param.attr;
  const uint32_t fs_id = GetFsId();
  const Ino parent = GetIno();
  CHECK(parent_attr.ino() == parent) << fmt::format("parent not equal in shared param, {} {}", parent_attr.ino(),
                                                    parent);

  // create dentry
  txn->Put(MetaCodec::EncodeDentryKey(fs_id, parent, dentry_.Name()), MetaCodec::EncodeDentryValue(dentry_.Copy()));

  // create inode
  txn->Put(MetaCodec::EncodeInodeKey(fs_id, dentry_.INo()), MetaCodec::EncodeInodeValue(attr_));

  // seed an empty dir-stat record so the dir is tracked from birth (no first-flush
  // recompute); the lazy missing_inos->CalcDirStat path still covers pre-existing dirs.
  DirStatEntry dir_stat;
  txn->Put(MetaCodec::EncodeDirStatKey(fs_id, dentry_.INo()), MetaCodec::EncodeDirStatValue(dir_stat));

  // update parent attr
  parent_attr.set_nlink(parent_attr.nlink() + 1);
  parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
  parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));

  return Status::OK();
}

Status BatchMkDirOperation::RunInBatch(TxnUPtr& txn, BatchSharedParam& shared_param) {
  auto& parent_attr = shared_param.attr;
  const uint32_t fs_id = GetFsId();
  const Ino parent = GetIno();
  CHECK(parent_attr.ino() == parent) << fmt::format("parent not equal in shared param, {} {}", parent_attr.ino(),
                                                    parent);

  CHECK(!dentries_.empty()) << "dentries is empty.";
  CHECK(dentries_.size() == attrs_.size())
      << fmt::format("dentry and attr size not equal, {} {}.", dentries_.size(), attrs_.size());

  // create dentry
  for (const auto& dentry : dentries_) {
    txn->Put(MetaCodec::EncodeDentryKey(fs_id, parent, dentry.Name()), MetaCodec::EncodeDentryValue(dentry.Copy()));
  }

  // create inode + seed empty dir-stat in the same txn so each dir is tracked from birth
  for (const auto& attr : attrs_) {
    txn->Put(MetaCodec::EncodeInodeKey(fs_id, attr.ino()), MetaCodec::EncodeInodeValue(attr));

    DirStatEntry dir_stat;
    txn->Put(MetaCodec::EncodeDirStatKey(fs_id, attr.ino()), MetaCodec::EncodeDirStatValue(dir_stat));
  }

  // update parent attr
  parent_attr.set_nlink(parent_attr.nlink() + dentries_.size());
  parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
  parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));

  return Status::OK();
}

Status MkNodOperation::RunInBatch(TxnUPtr& txn, BatchSharedParam& shared_param) {
  auto& parent_attr = shared_param.attr;
  const uint32_t fs_id = GetFsId();
  const Ino parent = GetIno();

  // create dentry
  txn->Put(MetaCodec::EncodeDentryKey(fs_id, parent, dentry_.Name()), MetaCodec::EncodeDentryValue(dentry_.Copy()));

  // create inode
  txn->Put(MetaCodec::EncodeInodeKey(fs_id, dentry_.INo()), MetaCodec::EncodeInodeValue(attr_));

  // update parent attr
  if (shared_param.UseMutation()) {
    // indirectly update parent attr update op, can reduce conflict with other operations on same parent
    AttrMutationEntry& parent_attr_mutation = shared_param.attr_mutation;

    CHECK(parent_attr_mutation.ino() == parent)
        << fmt::format("parent not equal in shared param, {} {}", parent_attr_mutation.ino(), parent);

    parent_attr_mutation.set_mtime(std::max(parent_attr_mutation.mtime(), GetTime()));
    parent_attr_mutation.set_ctime(std::max(parent_attr_mutation.ctime(), GetTime()));

  } else {
    // directly update parent attr, may cause more conflicts
    parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
    parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));
  }

  return Status::OK();
}

Status BatchMkNodOperation::RunInBatch(TxnUPtr& txn, BatchSharedParam& shared_param) {
  auto& parent_attr = shared_param.attr;
  const uint32_t fs_id = GetFsId();
  const Ino parent = GetIno();

  CHECK(!dentries_.empty()) << "dentries is empty.";
  CHECK(dentries_.size() == attrs_.size())
      << fmt::format("dentry and attr size not equal, {} {}.", dentries_.size(), attrs_.size());

  // create dentry
  for (const auto& dentry : dentries_) {
    txn->Put(MetaCodec::EncodeDentryKey(fs_id, parent, dentry.Name()), MetaCodec::EncodeDentryValue(dentry.Copy()));
  }

  // create inode
  for (const auto& attr : attrs_) {
    txn->Put(MetaCodec::EncodeInodeKey(fs_id, attr.ino()), MetaCodec::EncodeInodeValue(attr));
  }

  // update parent attr
  if (shared_param.UseMutation()) {
    // indirectly update parent attr update op, can reduce conflict with other operations on same parent

    AttrMutationEntry& parent_attr_mutation = shared_param.attr_mutation;

    CHECK(parent_attr_mutation.ino() == parent)
        << fmt::format("parent not equal in shared param, {} {}", parent_attr_mutation.ino(), parent);

    parent_attr_mutation.set_mtime(std::max(parent_attr_mutation.mtime(), GetTime()));
    parent_attr_mutation.set_ctime(std::max(parent_attr_mutation.ctime(), GetTime()));

  } else {
    // directly update parent attr, may cause more conflicts
    parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
    parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));
  }

  return Status::OK();
}

Status BatchCreateFileOperation::RunInBatch(TxnUPtr& txn, BatchSharedParam& shared_param) {
  auto& parent_attr = shared_param.attr;
  const uint32_t fs_id = GetFsId();
  const Ino parent = GetIno();

  CHECK(dentries_.size() == attrs_.size())
      << fmt::format("dentry and attr size not equal, {} {}.", dentries_.size(), attrs_.size());
  CHECK(dentries_.size() == file_sessions_.size())
      << fmt::format("dentry and file_session size not equal, {} {}.", dentries_.size(), file_sessions_.size());

  for (size_t i = 0; i < dentries_.size(); ++i) {
    const auto& dentry = dentries_[i];
    const auto& attr = attrs_[i];
    const auto& file_session = file_sessions_[i];

    // create dentry
    txn->Put(MetaCodec::EncodeDentryKey(fs_id, parent, dentry.Name()), MetaCodec::EncodeDentryValue(dentry.Copy()));

    // create inode
    txn->Put(MetaCodec::EncodeInodeKey(fs_id, dentry.INo()), MetaCodec::EncodeInodeValue(attr));

    // add file session
    txn->Put(MetaCodec::EncodeFileSessionKey(file_session->fs_id(), file_session->ino(), file_session->session_id()),
             MetaCodec::EncodeFileSessionValue(*file_session));
  }

  // update parent attr
  if (shared_param.UseMutation()) {
    // indirectly update parent attr update op, can reduce conflict with other operations on same parent

    AttrMutationEntry& parent_attr_mutation = shared_param.attr_mutation;
    CHECK(parent_attr_mutation.ino() == parent)
        << fmt::format("parent not equal in shared param, {} {}", parent_attr_mutation.ino(), parent);

    parent_attr_mutation.set_mtime(std::max(parent_attr_mutation.mtime(), GetTime()));
    parent_attr_mutation.set_ctime(std::max(parent_attr_mutation.ctime(), GetTime()));

  } else {
    // directly update parent attr, may cause more conflicts
    parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
    parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));
  }

  return Status::OK();
}

void HardLinkOperation::PrefetchKey(std::vector<std::string>& keys) {
  keys.push_back(MetaCodec::EncodeInodeKey(GetFsId(), dentry_.INo()));
}

Status HardLinkOperation::RunInBatch(TxnUPtr& txn, BatchSharedParam& shared_param) {
  auto& parent_attr = shared_param.attr;
  auto& prefetch_index = shared_param.prefetch_index;
  const uint32_t fs_id = GetFsId();
  const Ino parent = GetIno();

  // get child attr
  std::string child_key = MetaCodec::EncodeInodeKey(fs_id, dentry_.INo());
  std::string value = FindValue(prefetch_index, child_key);
  if (value.empty()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("get child inode({}) fail", dentry_.INo()));
  }
  AttrEntry attr = MetaCodec::DecodeInodeValue(value);

  // update inode nlink
  attr.set_nlink(attr.nlink() + 1);
  attr.set_ctime(std::max(attr.ctime(), GetTime()));
  AddParentIno(attr, parent);
  attr.set_version(attr.version() + 1);
  txn->Put(child_key, MetaCodec::EncodeInodeValue(attr));

  // create dentry
  txn->Put(MetaCodec::EncodeDentryKey(fs_id, parent, dentry_.Name()), MetaCodec::EncodeDentryValue(dentry_.Copy()));

  // update parent attr
  if (shared_param.UseMutation()) {
    // indirectly update parent attr update op, can reduce conflict with other operations on same parent

    AttrMutationEntry& parent_attr_mutation = shared_param.attr_mutation;
    CHECK(parent_attr_mutation.ino() == parent)
        << fmt::format("parent not equal in shared param, {} {}", parent_attr_mutation.ino(), parent);

    parent_attr_mutation.set_mtime(std::max(parent_attr_mutation.mtime(), GetTime()));
    parent_attr_mutation.set_ctime(std::max(parent_attr_mutation.ctime(), GetTime()));

  } else {
    // directly update parent attr, may cause more conflicts
    parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
    parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));
  }

  result_.child_attr = attr;

  return Status::OK();
}

Status SymLinkOperation::RunInBatch(TxnUPtr& txn, BatchSharedParam& shared_param) {
  auto& parent_attr = shared_param.attr;
  const uint32_t fs_id = GetFsId();
  const Ino parent = GetIno();

  // create dentry
  txn->Put(MetaCodec::EncodeDentryKey(fs_id, parent, dentry_.Name()), MetaCodec::EncodeDentryValue(dentry_.Copy()));

  // create inode
  txn->Put(MetaCodec::EncodeInodeKey(fs_id, dentry_.INo()), MetaCodec::EncodeInodeValue(attr_));

  // update parent attr
  if (shared_param.UseMutation()) {
    // indirectly update parent attr update op, can reduce conflict with other operations on same parent

    AttrMutationEntry& parent_attr_mutation = shared_param.attr_mutation;
    CHECK(parent_attr_mutation.ino() == parent)
        << fmt::format("parent not equal in shared param, {} {}", parent_attr_mutation.ino(), parent);

    parent_attr_mutation.set_mtime(std::max(parent_attr_mutation.mtime(), GetTime()));
    parent_attr_mutation.set_ctime(std::max(parent_attr_mutation.ctime(), GetTime()));

  } else {
    // directly update parent attr, may cause more conflicts
    parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
    parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));
  }

  return Status::OK();
}

static Status GetChunk(TxnUPtr& txn, uint32_t fs_id, Ino ino, uint64_t chunk_index, ChunkEntry& chunk) {
  std::string value;
  auto status = txn->Get(MetaCodec::EncodeChunkKey(fs_id, ino, chunk_index), value);
  if (!status.ok()) return status;

  chunk = MetaCodec::DecodeChunkValue(value);

  return Status::OK();
}

static Status ScanChunk(TxnUPtr& txn, uint32_t fs_id, Ino ino, std::map<uint64_t, ChunkEntry>& chunks) {
  Range range = MetaCodec::GetChunkRange(fs_id, ino);

  auto status = txn->Scan(range, [&](const std::string& key, const std::string& value) -> bool {
    if (!MetaCodec::IsChunkKey(key)) return true;

    auto chunk = MetaCodec::DecodeChunkValue(value);
    chunks.insert({chunk.index(), std::move(chunk)});

    return true;
  });

  return status;
}

// Append zero slices (id=0) over [offset, end_offset) across every chunk the
// range touches, shadowing existing data so the range reads back as zeros.
// Slices are append-ordered (last overlapping wins), so this logically clears
// the range without removing old slices. Touched chunks are written to txn and
// collected into effected_chunks. Shared by truncate-shrink and fallocate
// PUNCH_HOLE/ZERO_RANGE.
static Status AppendZeroSlices(TxnUPtr& txn, uint32_t fs_id, Ino ino, uint64_t offset, uint64_t end_offset,
                               uint64_t chunk_size, uint64_t block_size, std::vector<ChunkEntry>& effected_chunks) {
  // Reset so a txn retry (which re-runs this op) doesn't accumulate duplicates.
  effected_chunks.clear();

  std::map<uint64_t, ChunkEntry> chunks;
  auto status = ScanChunk(txn, fs_id, ino, chunks);
  if (!status.ok()) return status;

  while (offset < end_offset) {
    const uint64_t chunk_pos = offset % chunk_size;
    const uint64_t chunk_index = offset / chunk_size;
    const uint64_t delta_chunk_size = std::min(end_offset - offset, chunk_size - chunk_pos);

    SliceEntry slice;
    slice.set_id(0);
    slice.set_size(0);
    slice.set_pos(chunk_pos);
    slice.set_off(0);
    slice.set_len(delta_chunk_size);

    auto it = chunks.find(chunk_index);
    if (it == chunks.end()) {
      ChunkEntry chunk;
      chunk.set_index(chunk_index);
      chunk.set_chunk_size(chunk_size);
      chunk.set_block_size(block_size);
      chunk.set_version(1);
      chunk.add_slices()->Swap(&slice);

      LOG_DEBUG << fmt::format("[operation.{}.{}] update chunk, range[{},{}) version({}), value({}).", fs_id, ino,
                               offset, end_offset, chunk.version(), chunk.ShortDebugString());

      txn->Put(MetaCodec::EncodeChunkKey(fs_id, ino, chunk_index), MetaCodec::EncodeChunkValue(chunk));
      effected_chunks.push_back(std::move(chunk));

    } else {
      auto& chunk = it->second;
      chunk.add_slices()->Swap(&slice);
      // bump version so ChunkCache::PutIf accepts the updated slice list.
      chunk.set_version(chunk.version() + 1);
      LOG_DEBUG << fmt::format("[operation.{}.{}] update chunk, range[{},{}) version({}), value({}).", fs_id, ino,
                               offset, end_offset, chunk.version(), chunk.ShortDebugString());

      txn->Put(MetaCodec::EncodeChunkKey(fs_id, ino, chunk_index), MetaCodec::EncodeChunkValue(chunk));
      effected_chunks.push_back(chunk);
    }

    offset += delta_chunk_size;
  }

  return Status::OK();
}

Status UpdateAttrOperation::RunInBatch(TxnUPtr& txn, BatchSharedParam& shared_param) {
  auto& attr = shared_param.attr;

  if (to_set_ & kSetAttrMode) {
    attr.set_mode(attr_.mode());
  }

  if (to_set_ & kSetAttrUid) {
    attr.set_uid(attr_.uid());
  }

  if (to_set_ & kSetAttrGid) {
    attr.set_gid(attr_.gid());
  }

  if (to_set_ & kSetAttrSize) {
    const uint64_t old_length = attr.length();
    const uint64_t new_length = attr_.length();
    result_.delta_bytes = static_cast<int64_t>(new_length) - static_cast<int64_t>(old_length);

    attr.set_length(new_length);

    // Truncate-down must neutralize data beyond the new size; otherwise a later
    // extend (truncate-up / fallocate) re-exposes the dropped bytes, because
    // slices are append-ordered and the stale slice still sits in the chunk.
    // Mirror fallocate ZERO_RANGE: zero [new_length, old_length).
    if (new_length < old_length) {
      auto status = AppendZeroSlices(txn, attr.fs_id(), attr.ino(), new_length, old_length, extra_param_.chunk_size,
                                     extra_param_.block_size, result_.effected_chunks);
      if (!status.ok()) return status;
    }
  }

  if (to_set_ & kSetAttrAtime) {
    attr.set_atime(attr_.atime());
  }

  if (to_set_ & kSetAttrMtime) {
    attr.set_mtime(attr_.mtime());
  }

  if (to_set_ & kSetAttrCtime) {
    attr.set_ctime(attr_.ctime());
  }

  if (to_set_ & kSetAttrNlink) {
    attr.set_nlink(attr_.nlink());
  }

  if (to_set_ & kSetAttrFlags) {
    attr.set_flags(attr_.flags());
  }

  return Status::OK();
}

Status UpdateXAttrOperation::RunInBatch(TxnUPtr&, BatchSharedParam& shared_param) {
  auto& attr = shared_param.attr;

  for (const auto& [key, value] : xattrs_) {
    (*attr.mutable_xattrs())[key] = value;
  }

  // update attr
  attr.set_ctime(std::max(attr.ctime(), GetTime()));

  return Status::OK();
}

Status RemoveXAttrOperation::RunInBatch(TxnUPtr&, BatchSharedParam& shared_param) {
  auto& attr = shared_param.attr;

  if (attr.xattrs().find(name_) == attr.xattrs().end()) {
    return Status(pb::error::ENO_DATA, fmt::format("no xattr {}", name_));
  }

  attr.mutable_xattrs()->erase(name_);

  // update attr
  attr.set_ctime(std::max(attr.ctime(), GetTime()));

  return Status::OK();
}

Status UpdateShardBoundariesOperation::RunInBatch(TxnUPtr&, BatchSharedParam& shared_param) {
  auto& attr = shared_param.attr;

  attr.mutable_shard_boundaries()->Clear();

  for (const auto& boundary : shard_boundaries_) {
    *attr.add_shard_boundaries() = boundary;
  }

  // update attr
  attr.set_ctime(std::max(attr.ctime(), GetTime()));

  return Status::OK();
}

Status UpsertChunkOperation::Run(TxnUPtr& txn) {
  const uint32_t fs_id = fs_info_.fs_id();
  const uint64_t now_ms = utils::TimestampMs();

  std::set<std::string> keys;
  for (const auto& delta_slices : delta_slices_) {
    keys.insert(MetaCodec::EncodeChunkKey(fs_id, ino_, delta_slices.chunk_index()));
  }

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(std::vector<std::string>(keys.begin(), keys.end()), kvs);
  if (!status.ok()) return status;

  int64_t length = 0;
  result_.effected_chunks.clear();
  for (const auto& delta_slices : delta_slices_) {
    ChunkEntry chunk;
    const uint64_t chunk_index = delta_slices.chunk_index();

    const std::string key = MetaCodec::EncodeChunkKey(fs_id, ino_, chunk_index);
    auto value = FindValue(kvs, key);
    if (!value.empty()) chunk = MetaCodec::DecodeChunkValue(value);

    LOG(INFO) << fmt::format(
        "[operation.{}.{}] upsert chunk, chunk_index({}), keys({}) kvs({}) value({}) old_chunk({}).", fs_id, ino_,
        chunk_index, keys.size(), kvs.size(), value.size(), chunk.ShortDebugString());

    bool has_update = false;
    // not exist chunk, create a new one
    if (chunk.version() == 0) {
      has_update = true;
      chunk.set_index(chunk_index);
      chunk.set_chunk_size(fs_info_.chunk_size());
      chunk.set_block_size(fs_info_.block_size());
      *chunk.mutable_slices() = delta_slices.slices();

    } else {
      // exist chunk, update slice
      // check if slice already exist
      auto is_exist_fn = [&](const SliceEntry& slice) -> bool {
        for (const auto& exist_slice : chunk.slices()) {
          if (exist_slice.id() == slice.id()) {
            return true;
          }
        }
        for (const auto& compacted_slice : chunk.compacted_slices()) {
          for (const uint64_t& compacted_slice_id : compacted_slice.slice_ids()) {
            if (compacted_slice_id == slice.id()) {
              return true;
            }
          }
        }
        return false;
      };

      const uint64_t chunk_pos = chunk.index() * chunk.chunk_size();
      for (const auto& slice : delta_slices.slices()) {
        if (is_exist_fn(slice)) continue;

        has_update = true;
        *chunk.add_slices() = slice;
        length = std::max(length, static_cast<int64_t>(chunk_pos + slice.pos() + slice.len()));
      }
    }

    if (has_update) {
      // clean compacted slices if expired
      auto it = chunk.mutable_compacted_slices()->begin();
      while (it != chunk.mutable_compacted_slices()->end()) {
        if (it->time_ms() + (kCleanCompactedSliceIntervalS * 1000) < now_ms) {
          it = chunk.mutable_compacted_slices()->erase(it);
        } else {
          ++it;
        }
      }

      chunk.set_version(chunk.version() + 1);

      LOG_DEBUG << fmt::format("[operation.{}.{}] update chunk, version({}), value({}).", fs_id, ino_, chunk.version(),
                               chunk.ShortDebugString());
      txn->Put(key, MetaCodec::EncodeChunkValue(chunk));
    }

    result_.effected_chunks.push_back(std::move(chunk));
  }

  result_.length = length;

  return Status::OK();
}

Status GetChunkOperation::Run(TxnUPtr& txn) {
  std::vector<std::string> keys;
  keys.reserve(chunk_indexes_.size());
  for (const auto& chunk_index : chunk_indexes_) {
    keys.push_back(MetaCodec::EncodeChunkKey(fs_id_, ino_, chunk_index));
  }

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(keys, kvs);
  if (!status.ok()) return status;

  result_.chunks.clear();
  for (const auto& kv : kvs) {
    result_.chunks.push_back(MetaCodec::DecodeChunkValue(kv.value));
  }

  return Status::OK();
}

Status ScanChunkOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetChunkRange(fs_id_, ino_);

  result_.chunks.clear();
  uint32_t slice_num = 0;
  return txn->Scan(range, [&](const std::string& key, const std::string& value) -> bool {
    if (!MetaCodec::IsChunkKey(key)) return true;

    auto chunk = MetaCodec::DecodeChunkValue(value);
    slice_num += chunk.slices_size();
    result_.chunks.push_back(std::move(chunk));

    if (max_slice_num_ != 0 && slice_num >= max_slice_num_) return false;

    return true;
  });
}

Status CleanChunkOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ > 0) << " fs_id is 0.";
  CHECK(ino_ > 0) << " ino is 0.";

  for (auto& chunk_index : chunk_indexs_) {
    txn->Delete(MetaCodec::EncodeChunkKey(fs_id_, ino_, chunk_index));
  }

  return Status::OK();
}

Status GetSliceRefOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ > 0) << " fs_id is 0.";
  CHECK(slice_id_ > 0) << " slice_id is 0.";

  std::string value;
  auto status = txn->Get(MetaCodec::EncodeSliceRefKey(slice_id_), value);
  if (!status.ok()) return status;

  result_.slice_ref = MetaCodec::DecodeSliceRefValue(value);

  return Status::OK();
}

Status DecSliceRefOperation::Run(TxnUPtr& txn) {
  CHECK(slice_id_ > 0) << " slice_id is 0.";

  std::string value;
  const std::string key = MetaCodec::EncodeSliceRefKey(slice_id_);
  auto status = txn->Get(key, value);
  if (!status.ok()) return status;

  SliceRefEntry slice_ref = MetaCodec::DecodeSliceRefValue(value);

  slice_ref.set_ref_count(slice_ref.ref_count() - 1);
  DelSliceRefIno(slice_ref, ino_);
  if (slice_ref.ref_count() > 0) {
    txn->Put(key, MetaCodec::EncodeSliceRefValue(slice_ref));

  } else {
    txn->Delete(key);
  }

  result_.slice_ref = std::move(slice_ref);

  return Status::OK();
}

Status ScanSliceRefOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetSliceRefRange();

  result_.slice_refs.clear();
  return txn->Scan(range, [&](const std::string& key, const std::string& value) -> bool {
    if (!MetaCodec::IsSliceRefKey(key)) return true;

    result_.slice_refs.push_back(MetaCodec::DecodeSliceRefValue(value));

    return true;
  });
}

Status FallocateOperation::PreAlloc(TxnUPtr& txn, AttrEntry& attr, uint64_t offset, uint32_t len) {
  uint64_t length = attr.length();
  const uint64_t new_length = offset + len;

  if (length >= new_length) return Status::OK();

  const uint32_t fs_id = attr.fs_id();
  const Ino ino = attr.ino();
  const uint64_t chunk_size = param_.chunk_size;
  const uint64_t block_size = param_.block_size;
  const uint32_t slice_num = param_.slice_num;

  // GetChunk returns ENOT_FOUND when no chunk exists at the starting index —
  // expected for an empty (0-byte) file or a chunk-aligned current length.
  // Track existence explicitly so the loop always takes the "create new chunk"
  // branch when there is nothing to append to (which sets chunk_size and
  // block_size properly instead of writing a zero-init ChunkEntry back).
  ChunkEntry max_chunk;
  bool max_chunk_exists = false;
  {
    auto status = GetChunk(txn, fs_id, ino, length / chunk_size, max_chunk);
    if (status.ok()) {
      max_chunk_exists = true;
    } else if (status.error_code() != pb::error::ENOT_FOUND) {
      return status;
    }
  }

  std::vector<ChunkEntry> effected_chunks;
  uint32_t count = 0;
  while (length < new_length) {
    uint64_t chunk_pos = length % chunk_size;
    uint64_t chunk_index = length / chunk_size;
    uint64_t delta_size = new_length - length;
    uint64_t delta_chunk_size = (chunk_pos + delta_size > chunk_size) ? (chunk_size - chunk_pos) : delta_size;

    SliceEntry slice;
    slice.set_id(0);
    slice.set_size(0);
    slice.set_pos(chunk_pos);
    slice.set_off(0);
    slice.set_len(delta_chunk_size);

    if (max_chunk_exists) {
      CHECK(chunk_index >= max_chunk.index()) << fmt::format(
          "chunk_index({}) should be greater than or equal to max_chunk.index({}).", chunk_index, max_chunk.index());
    }

    if (!max_chunk_exists || chunk_index > max_chunk.index()) {
      ChunkEntry chunk;
      chunk.set_index(chunk_index);
      chunk.set_chunk_size(chunk_size);
      chunk.set_block_size(block_size);
      chunk.set_version(1);
      chunk.add_slices()->Swap(&slice);

      LOG_DEBUG << fmt::format("[operation.{}.{}] update chunk, version({}), value({}).", fs_id, ino, chunk.version(),
                               chunk.ShortDebugString());

      txn->Put(MetaCodec::EncodeChunkKey(fs_id, ino, chunk_index), MetaCodec::EncodeChunkValue(chunk));
      effected_chunks.push_back(std::move(chunk));

    } else {
      max_chunk.add_slices()->Swap(&slice);
      // bump version so ChunkCache::PutIf accepts the updated slice list.
      max_chunk.set_version(max_chunk.version() + 1);
      LOG_DEBUG << fmt::format("[operation.{}.{}] update chunk, version({}), value({}).", fs_id, ino,
                               max_chunk.version(), max_chunk.ShortDebugString());

      txn->Put(MetaCodec::EncodeChunkKey(fs_id, ino, chunk_index), MetaCodec::EncodeChunkValue(max_chunk));
      effected_chunks.push_back(max_chunk);
      // After we append to max_chunk in this iteration, subsequent iterations
      // will be in new chunks (chunk_index > max_chunk.index()).
      max_chunk_exists = false;
    }

    length += delta_chunk_size;
    ++count;
    if (count > slice_num) {
      return Status(pb::error::EINTERNAL, fmt::format("beyond slice num({})", slice_num));
    }
  }

  // PreAlloc extends file size unconditionally (plain mode=0 semantic).
  // Without this, fallocate(2) with mode=0 returns success but the file
  // length never grows past the original — `ls -la` reports the old size.
  attr.set_length(new_length);

  result_.effected_chunks = std::move(effected_chunks);

  return Status::OK();
}

// |---------file length--------|
// ------------------------------------------>
// 1. [offset, len)    |-----|
// 2. [offset, len)    |-------------|
// 3. [offset, len)                |-----|
Status FallocateOperation::SetZero(TxnUPtr& txn, AttrEntry& attr, uint64_t offset, uint64_t len, bool keep_size) {
  uint64_t end_offset = keep_size ? std::min(attr.length(), offset + len) : (offset + len);

  auto status = AppendZeroSlices(txn, attr.fs_id(), attr.ino(), offset, end_offset, param_.chunk_size,
                                 param_.block_size, result_.effected_chunks);
  if (!status.ok()) return status;

  if (!keep_size && end_offset > attr.length()) {
    attr.set_length(end_offset);
  }

  return Status::OK();
}

Status FallocateOperation::RunInBatch(TxnUPtr& txn, BatchSharedParam& shared_param) {
  auto& attr = shared_param.attr;
  const int32_t mode_ = param_.mode;
  const uint64_t offset = param_.offset;
  const uint64_t len = param_.len;

  // Pre-image length, before PreAlloc/SetZero mutate attr. The growth is charged
  // to quota and dir-stat by the caller via result_.delta_bytes.
  const uint64_t file_length = attr.length();

  if (mode_ == 0) {
    // pre allocate
    auto status = PreAlloc(txn, attr, offset, len);
    if (!status.ok()) {
      return Status(pb::error::EINTERNAL,
                    fmt::format("pre allocate file length({}) fail, {}", offset + len, status.error_str()));
    }

  } else if (mode_ & FALLOC_FL_PUNCH_HOLE) {
    auto status = SetZero(txn, attr, offset, len, true);
    if (!status.ok()) {
      return Status(pb::error::EINTERNAL,
                    fmt::format("punch hole range[{},{}) fail, {}", offset, offset + len, status.error_str()));
    }

  } else if (mode_ & FALLOC_FL_ZERO_RANGE) {
    // set range to zero
    auto status = SetZero(txn, attr, offset, len, mode_ & FALLOC_FL_KEEP_SIZE);
    if (!status.ok()) {
      return Status(pb::error::EINTERNAL,
                    fmt::format("set range[{},{}) to zero fail, {}", offset, offset + len, status.error_str()));
    }

  } else if (mode_ & FALLOC_FL_COLLAPSE_RANGE) {
    return Status(pb::error::ENOT_SUPPORT, "not support FALLOC_FL_COLLAPSE_RANGE");
  }

  result_.delta_bytes = static_cast<int64_t>(attr.length()) - static_cast<int64_t>(file_length);

  return Status::OK();
}

void OpenFileOperation::PrefetchKey(std::vector<std::string>& keys) {
  if (prefetch_chunks_.empty() && !prefetch_data_) return;

  for (const auto& chunk_index : prefetch_chunks_) {
    keys.push_back(MetaCodec::EncodeChunkKey(file_session_.fs_id(), file_session_.ino(), chunk_index));
  }

  if (FLAGS_mds_tiny_file_data_enable && prefetch_data_) {
    keys.push_back(MetaCodec::EncodeTinyFileDataKey(file_session_.fs_id(), file_session_.ino()));
  }
}

Status OpenFileOperation::RunInBatch(TxnUPtr& txn, BatchSharedParam& shared_param) {
  auto& attr = shared_param.attr;
  const auto& prefetch_kvs = shared_param.prefetch_kvs;

  if (attr.nlink() == 0) {
    return Status(pb::error::EDELETED, "file is deleted");
  }

  if (flags_ & O_TRUNC) {
    // delete tiny file data
    if (FLAGS_mds_tiny_file_data_enable && attr.maybe_tiny_file()) {
      txn->Delete(MetaCodec::EncodeTinyFileDataKey(file_session_.fs_id(), file_session_.ino()));
    }

    result_.delta_bytes = -static_cast<int64_t>(attr.length());
    attr.set_length(0);

    attr.set_ctime(std::max(attr.ctime(), GetTime()));
    attr.set_mtime(std::max(attr.mtime(), GetTime()));
  }

  attr.set_atime(std::max(attr.atime(), GetTime()));

  // add file session
  txn->Put(MetaCodec::EncodeFileSessionKey(file_session_.fs_id(), file_session_.ino(), file_session_.session_id()),
           MetaCodec::EncodeFileSessionValue(file_session_));

  // prefetch chunks and tiny file data
  for (const auto& kv : prefetch_kvs) {
    if (MetaCodec::IsChunkKey(kv.key)) {
      result_.chunks.push_back(MetaCodec::DecodeChunkValue(kv.value));

    } else if (MetaCodec::IsTinyFileDataKey(kv.key)) {
      if (attr.length() > 0) {
        auto& mut_kv = const_cast<KeyValue&>(kv);
        uint64_t data_version = 0;
        MetaCodec::DecodeTinyFileDataValue(mut_kv.value, data_version);
        mut_kv.value.resize(attr.length());
        result_.data.swap(mut_kv.value);
        result_.data_version = data_version;
      }
    }
  }

  return Status::OK();
}

Status CloseFileOperation::Run(TxnUPtr& txn) {
  txn->Delete(MetaCodec::EncodeFileSessionKey(fs_id_, ino_, session_id_));
  return Status::OK();
}

Status FlushFileOperation::Run(TxnUPtr& txn) {
  CHECK(param_.slice_id != 0) << "slice_id not should be 0.";
  CHECK(param_.chunk_size != 0) << "chunk_size not should be 0.";

  const std::string key = MetaCodec::EncodeInodeKey(fs_id_, ino_);
  std::vector<std::string> keys = {key};
  if (FLAGS_mds_tiny_file_data_enable) {
    keys.push_back(MetaCodec::EncodeTinyFileDataKey(fs_id_, ino_));
  }
  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(keys, kvs);
  if (!status.ok()) return status;

  AttrEntry attr;
  bool is_updated = true;
  uint64_t data_version = 0;
  for (auto& kv : kvs) {
    if (kv.key == key) {
      attr = MetaCodec::DecodeInodeValue(kv.value);

    } else if (MetaCodec::IsTinyFileDataKey(kv.key)) {
      is_updated = (memcmp(param_.data.data(), kv.value.data(), param_.data.size()) != 0);
      MetaCodec::DecodeTinyFileDataValue(kv.value, data_version);

    } else {
      LOG(FATAL) << fmt::format("[operation.{}.{}] invalid key({}).", fs_id_, ino_, Helper::StringToHex(kv.key));
    }
  }

  if (param_.length > attr.length()) {
    // int64_t delta_bytes = static_cast<int64_t>(param_.length) - static_cast<int64_t>(attr.length());
    // if (delta_bytes < 0) {
    //   if (!param_.is_final) {
    //     result_.attr = attr;
    //     return Status::OK();
    //   }
    // }

    result_.delta_bytes = static_cast<int64_t>(param_.length) - static_cast<int64_t>(attr.length());
    attr.set_length(param_.length);
  }
  attr.set_mtime(std::max(attr.mtime(), GetTime()));
  // attr.set_ctime(std::max(attr.ctime(), GetTime()));
  attr.set_version(attr.version() + 1);

  txn->Put(key, MetaCodec::EncodeInodeValue(attr));

  if (attr.maybe_tiny_file() && is_updated) {
    std::string& mut_data = const_cast<std::string&>(param_.data);
    txn->Put(MetaCodec::EncodeTinyFileDataKey(fs_id_, ino_),
             MetaCodec::EncodeTinyFileDataValue(mut_data, ++data_version));
  }

  result_.attr = attr;

  return Status::OK();
}

static Status CheckDirEmpty(TxnUPtr& txn, uint32_t fs_id, uint64_t ino, bool& is_empty, Ino& child_ino) {
  Range range = MetaCodec::GetDentryRange(fs_id, ino, false);

  std::vector<KeyValue> kvs;
  auto status = txn->Scan(range, 1, kvs);
  if (!status.ok()) return status;

  if (kvs.empty()) {
    is_empty = true;
    return Status::OK();
  }

  for (const auto& kv : kvs) {
    if (MetaCodec::IsDentryKey(kv.key)) {
      auto dentry = MetaCodec::DecodeDentryValue(kv.value);
      child_ino = dentry.ino();
      break;
    }
  }

  is_empty = false;

  return Status::OK();
}

static void PutTrashBucket(TxnUPtr& txn, uint32_t fs_id, const TrashMove& trash, bool trash_bucket_exist) {
  if (trash.IsAlreadyExist() || trash_bucket_exist) return;

  AttrEntry trash_bucket_attr = BuildSubTrashBucketAttr(fs_id, trash.bucket_ino);
  txn->Put(MetaCodec::EncodeInodeKey(fs_id, trash.bucket_ino), MetaCodec::EncodeInodeValue(trash_bucket_attr));

  DentryEntry bucket_dentry;
  bucket_dentry.set_fs_id(fs_id);
  bucket_dentry.set_ino(trash.bucket_ino);
  bucket_dentry.set_parent(kTrashInodeId);
  bucket_dentry.set_name(trash.bucket_name);
  bucket_dentry.set_type(pb::mds::FileType::DIRECTORY);
  txn->Put(MetaCodec::EncodeDentryKey(fs_id, kTrashInodeId, trash.bucket_name),
           MetaCodec::EncodeDentryValue(bucket_dentry));
}

Status RmDirOperation::Run(TxnUPtr& txn) {
  const uint32_t fs_id = fs_id_;
  const Ino parent = parent_;
  const Ino child_ino = child_ino_;
  const bool enable_trash = trash_.Enabled();

  // 1. BatchGet parent + dentry + child inode (always); trash mode also pulls
  // bucket lookup material so PutTrashBucket can stay within this txn.
  // child_ino comes from caller (FileSystem::RmDir partition->Get) so child_key
  // can join the same BatchGet -- mirrors UnlinkOperation's prefetch pattern
  // and keeps trash-branch parents rewrite at zero extra RTT.
  const std::string parent_key = MetaCodec::EncodeInodeKey(fs_id, parent);
  const std::string dentry_key = MetaCodec::EncodeDentryKey(fs_id, parent, name_);
  std::vector<std::string> keys{parent_key, dentry_key};
  std::string child_attr_key, bucket_dentry_key;
  if (enable_trash) {
    CHECK(child_ino != 0) << "invalid child ino(0) for trash rmdir";

    child_attr_key = MetaCodec::EncodeInodeKey(fs_id, child_ino);
    keys.push_back(child_attr_key);
    if (!trash_.IsAlreadyExist()) {
      bucket_dentry_key = MetaCodec::EncodeDentryKey(fs_id, kTrashInodeId, trash_.bucket_name);
      keys.push_back(bucket_dentry_key);
    }
  }

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(keys, kvs);
  if (!status.ok()) return status;

  AttrEntry parent_attr;
  DentryEntry dentry;
  AttrEntry child_attr;
  bool trash_bucket_exist = false;
  for (const auto& kv : kvs) {
    if (kv.key == parent_key) {
      parent_attr = MetaCodec::DecodeInodeValue(kv.value);
    } else if (kv.key == dentry_key) {
      dentry = MetaCodec::DecodeDentryValue(kv.value);
    } else if (kv.key == child_attr_key) {
      child_attr = MetaCodec::DecodeInodeValue(kv.value);
    } else if (kv.key == bucket_dentry_key) {
      trash_bucket_exist = true;
    } else {
      LOG(FATAL) << fmt::format("[operation.{}.{}] invalid key({}).", fs_id, parent, Helper::StringToHex(kv.key));
    }
  }
  if (parent_attr.ino() == 0) {
    return Status(pb::error::ENOT_FOUND, fmt::format("parent inode({}) not found", parent));
  }
  if (dentry.ino() == 0) {
    return Status(pb::error::ENOT_FOUND, fmt::format("dentry({}) not found", name_));
  }
  if (enable_trash && child_attr.ino() == 0) {
    return Status(pb::error::ENOT_FOUND, fmt::format("child inode({}) not found", child_ino));
  }

  // 2. Build trash_entry_name now that the child ino is known.
  std::string trash_entry_name;
  if (enable_trash) trash_entry_name = BuildTrashEntryName(parent, child_ino, name_);

  // 3. POSIX rmdir requires the directory to be empty.
  bool is_empty = false;
  Ino first_child_ino = 0;
  status = CheckDirEmpty(txn, fs_id, dentry.ino(), is_empty, first_child_ino);
  if (!status.ok()) return status;
  if (!is_empty) {
    return Status(pb::error::ENOT_EMPTY,
                  fmt::format("directory({}) is not empty, child ino({})", dentry.ino(), first_child_ino));
  }

  // 4. Update parent attr (nlink--, ctime/mtime/version bump) in both modes.
  // Manual cleanup of a grafted subtree under .trash routes here with
  // parent == bucket; bucket nlink is intentionally fixed (see trash.cc).
  if (!IsTrashInode(parent)) parent_attr.set_nlink(parent_attr.nlink() - 1);
  parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));
  parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
  parent_attr.set_version(parent_attr.version() + 1);
  txn->Put(parent_key, MetaCodec::EncodeInodeValue(parent_attr));

  // 5. Delete the dentry under the original parent.
  txn->Delete(dentry_key);

  if (!enable_trash) {
    child_attr_key = MetaCodec::EncodeInodeKey(fs_id, dentry.ino());
    // 6a. Plain rmdir: drop the child inode, its mutation slots and its dir-stat
    // record. This also covers trash GC permanent deletion, which reuses
    // RmDirOperation with trash disabled, so the dir-stat record is not leaked.
    txn->Delete(child_attr_key);
    for (uint32_t i = 0; i < kDirAttrMutationNum; ++i) {
      txn->Delete(MetaCodec::EncodeDirInodeMutationKey(fs_id, dentry.ino(), i));
    }
    txn->Delete(MetaCodec::EncodeDirStatKey(fs_id, dentry.ino()));
  } else {
    CHECK(trash_.bucket_ino != 0) << "invalid trash bucket ino(0)";

    // 6b. Trash rmdir: resolve bucket, swap child parents into trash bucket,
    // and write the trash dentry. Symmetric with UnlinkOperation::RunInBatch
    // so trashed directories carry parents=[trash_ino] in KV, closing the
    // immutability gate for MkDir/MkNod/SetAttr/etc. under a stale handle.
    PutTrashBucket(txn, fs_id, trash_, trash_bucket_exist);

    DelParentIno(child_attr, parent);
    AddParentIno(child_attr, trash_.bucket_ino);
    child_attr.set_ctime(std::max(child_attr.ctime(), GetTime()));
    child_attr.set_version(child_attr.version() + 1);
    txn->Put(child_attr_key, MetaCodec::EncodeInodeValue(child_attr));

    // Bucket inode attr is intentionally not touched (see trash.cc).
    DentryEntry trash_dentry;
    trash_dentry.set_fs_id(fs_id);
    trash_dentry.set_ino(dentry.ino());
    trash_dentry.set_parent(trash_.bucket_ino);
    trash_dentry.set_name(trash_entry_name);
    trash_dentry.set_type(pb::mds::FileType::DIRECTORY);
    txn->Put(MetaCodec::EncodeDentryKey(fs_id, trash_.bucket_ino, trash_entry_name),
             MetaCodec::EncodeDentryValue(trash_dentry));

    result_.child_attr = child_attr;
  }

  result_.parent_attr = parent_attr;
  result_.dentry = dentry;

  return Status::OK();
}

void UnlinkOperation::PrefetchKey(std::vector<std::string>& keys) {
  keys.push_back(MetaCodec::EncodeInodeKey(GetFsId(), dentry_.INo()));
  keys.push_back(MetaCodec::EncodeDentryKey(GetFsId(), dentry_.ParentIno(), dentry_.Name()));
  if (trash_.Enabled() && !trash_.IsAlreadyExist()) {
    keys.push_back(MetaCodec::EncodeDentryKey(GetFsId(), kTrashInodeId, trash_.bucket_name));
  }
}

Status UnlinkOperation::RunInBatch(TxnUPtr& txn, BatchSharedParam& shared_param) {
  auto& parent_attr = shared_param.attr;
  auto& prefetch_index = shared_param.prefetch_index;
  const uint32_t fs_id = dentry_.FsId();
  const Ino parent = dentry_.ParentIno();

  // get child attr
  const std::string attr_key = MetaCodec::EncodeInodeKey(fs_id, dentry_.INo());
  std::string attr_value = FindValue(prefetch_index, attr_key);
  if (attr_value.empty()) return Status(pb::error::ENOT_FOUND, fmt::format("not found attr({})", dentry_.INo()));
  AttrEntry attr = MetaCodec::DecodeInodeValue(attr_value);

  // 1. Find dentry from prefetch index.
  const std::string dentry_key = MetaCodec::EncodeDentryKey(fs_id, parent, dentry_.Name());
  std::string dentry_value = FindValue(prefetch_index, dentry_key);
  if (dentry_value.empty()) return Status(pb::error::ENOT_FOUND, fmt::format("not found dentry({})", dentry_.Name()));

  const bool enable_trash = trash_.Enabled();

  // 5. trash bucket info from prefetch (cold path only — hot path trusts the
  // SubTrashCache invariant and skips the inode existence check).
  bool trash_bucket_exist = false;
  if (enable_trash && !trash_.IsAlreadyExist()) {
    std::string trash_bucket_dentry_value =
        FindValue(prefetch_index, MetaCodec::EncodeDentryKey(fs_id, kTrashInodeId, trash_.bucket_name));
    if (!trash_bucket_dentry_value.empty()) trash_bucket_exist = true;
  }

  // 6. Update child attr. Trash mode replaces the original parent in
  // parents_ with the sub_trash bucket ino (>= kTrashInodeId), preserving
  // nlink. Non-trash decrements nlink and removes the parent pointer.
  attr.set_ctime(std::max(attr.ctime(), GetTime()));
  attr.set_version(attr.version() + 1);
  if (enable_trash) {
    DelParentIno(attr, parent);
    AddParentIno(attr, trash_.bucket_ino);
  } else {
    attr.set_nlink(attr.nlink() - 1);
    DelParentIno(attr, parent);
  }
  txn->Put(attr_key, MetaCodec::EncodeInodeValue(attr));

  if (!enable_trash && attr.nlink() <= 0) {
    txn->Put(MetaCodec::EncodeDelFileKey(fs_id, dentry_.INo()), MetaCodec::EncodeDelFileValue(attr));
  }

  // 7. Delete the dentry under the original parent.
  txn->Delete(dentry_key);

  // 8. Trash bucket: write the new trash dentry. The bucket inode attr is
  // intentionally not touched (see trash.cc).
  if (enable_trash) {
    // put hour trash bucket
    PutTrashBucket(txn, fs_id, trash_, trash_bucket_exist);

    std::string trash_entry_name = BuildTrashEntryName(parent, dentry_.INo(), dentry_.Name());
    DentryEntry trash_dentry;
    trash_dentry.set_fs_id(fs_id);
    trash_dentry.set_ino(dentry_.INo());
    trash_dentry.set_parent(trash_.bucket_ino);
    trash_dentry.set_name(trash_entry_name);
    trash_dentry.set_type(dentry_.Type());
    txn->Put(MetaCodec::EncodeDentryKey(fs_id, trash_.bucket_ino, trash_entry_name),
             MetaCodec::EncodeDentryValue(trash_dentry));
  }

  if (shared_param.UseMutation()) {
    AttrMutationEntry& parent_attr_mutation = shared_param.attr_mutation;
    CHECK(parent_attr_mutation.ino() == parent)
        << fmt::format("parent not equal in shared param, {} {}", parent_attr_mutation.ino(), parent);
    parent_attr_mutation.set_mtime(std::max(parent_attr_mutation.mtime(), GetTime()));
    parent_attr_mutation.set_ctime(std::max(parent_attr_mutation.ctime(), GetTime()));

  } else {
    parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
    parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));
  }

  result_.child_attr = attr;

  return Status::OK();
}

void BatchUnlinkOperation::PrefetchKey(std::vector<std::string>& keys) {
  const uint32_t fs_id = GetFsId();
  const Ino parent = GetIno();

  for (const auto& dentry : dentries_) {
    keys.push_back(MetaCodec::EncodeInodeKey(fs_id, dentry.INo()));
    keys.push_back(MetaCodec::EncodeDentryKey(fs_id, parent, dentry.Name()));
  }
  if (trash_.Enabled() && !trash_.IsAlreadyExist()) {
    keys.push_back(MetaCodec::EncodeDentryKey(fs_id, kTrashInodeId, trash_.bucket_name));
  }
}

Status BatchUnlinkOperation::RunInBatch(TxnUPtr& txn, BatchSharedParam& shared_param) {
  auto& parent_attr = shared_param.attr;
  auto& prefetch_index = shared_param.prefetch_index;
  const uint32_t fs_id = GetFsId();
  const Ino parent = GetIno();
  const bool enable_trash = trash_.Enabled();

  // 1. Decode prefetched dentries (per-name) and child inode attrs.
  std::vector<AttrEntry> attrs;
  std::vector<DentryEntry> dentries;
  attrs.reserve(dentries_.size());
  dentries.reserve(dentries_.size());
  for (const auto& dentry : dentries_) {
    auto attr_value = FindValue(prefetch_index, MetaCodec::EncodeInodeKey(fs_id, dentry.INo()));
    if (attr_value.empty()) {
      return Status(pb::error::ENOT_FOUND, fmt::format("not found attr({})", dentry.INo()));
    }
    attrs.push_back(MetaCodec::DecodeInodeValue(attr_value));

    auto dentry_value = FindValue(prefetch_index, MetaCodec::EncodeDentryKey(fs_id, parent, dentry.Name()));
    if (dentry_value.empty()) {
      return Status(pb::error::ENOT_FOUND, fmt::format("not found dentry({})", dentry.Name()));
    }
    dentries.push_back(MetaCodec::DecodeDentryValue(dentry_value));
  }

  CHECK(attrs.size() == dentries.size()) << fmt::format("attrs size({}) should be equal to dentries size({}).",
                                                        attrs.size(), dentries.size());

  // 2. Resolve sub-trash bucket (once for the whole batch) when in trash mode.
  // Hot path skips inode prefetch and trusts the SubTrashCache invariant; only
  // cold path needs the bucket dentry to discover an existing winner.
  if (enable_trash) {
    bool trash_bucket_exist = false;
    if (!trash_.IsAlreadyExist()) {
      std::string bucket_dentry_value =
          FindValue(prefetch_index, MetaCodec::EncodeDentryKey(fs_id, kTrashInodeId, trash_.bucket_name));
      if (!bucket_dentry_value.empty()) trash_bucket_exist = true;
    }
    PutTrashBucket(txn, fs_id, trash_, trash_bucket_exist);
  }

  // 3. Per-child: trash → swap parent into bucket + write trash dentry;
  //    plain → nlink--, DelFile if last link, delete dentry.
  for (size_t i = 0; i < attrs.size(); ++i) {
    auto& attr = attrs[i];
    const auto& dentry = dentries_[i];
    const Ino child_ino = dentries[i].ino();
    const std::string child_key = MetaCodec::EncodeInodeKey(fs_id, child_ino);
    const std::string dentry_key = MetaCodec::EncodeDentryKey(fs_id, parent, dentry.Name());

    attr.set_ctime(std::max(attr.ctime(), GetTime()));
    attr.set_version(attr.version() + 1);
    if (enable_trash) {
      DelParentIno(attr, parent);
      AddParentIno(attr, trash_.bucket_ino);
    } else {
      attr.set_nlink(attr.nlink() - 1);
      DelParentIno(attr, parent);
    }
    txn->Put(child_key, MetaCodec::EncodeInodeValue(attr));

    if (!enable_trash && attr.nlink() <= 0) {
      txn->Put(MetaCodec::EncodeDelFileKey(fs_id, child_ino), MetaCodec::EncodeDelFileValue(attr));
    }

    txn->Delete(dentry_key);

    if (enable_trash) {
      const std::string trash_entry_name = BuildTrashEntryName(parent, child_ino, dentry.Name());
      DentryEntry trash_dentry;
      trash_dentry.set_fs_id(fs_id);
      trash_dentry.set_ino(child_ino);
      trash_dentry.set_parent(trash_.bucket_ino);
      trash_dentry.set_name(trash_entry_name);
      trash_dentry.set_type(dentries[i].type());
      txn->Put(MetaCodec::EncodeDentryKey(fs_id, trash_.bucket_ino, trash_entry_name),
               MetaCodec::EncodeDentryValue(trash_dentry));
    }
  }

  if (shared_param.UseMutation()) {
    AttrMutationEntry& parent_attr_mutation = shared_param.attr_mutation;
    CHECK(parent_attr_mutation.ino() == parent)
        << fmt::format("parent not equal in shared param, {} {}", parent_attr_mutation.ino(), parent);

    parent_attr_mutation.set_mtime(std::max(parent_attr_mutation.mtime(), GetTime()));
    parent_attr_mutation.set_ctime(std::max(parent_attr_mutation.ctime(), GetTime()));

  } else {
    parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
    parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));
  }

  result_.child_attrs.swap(attrs);

  return Status::OK();
}

Status RestoreFromTrashOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ != 0) << "invalid fs_id";
  CHECK(expected_file_ino_ != 0) << "invalid expected_file_ino";
  CHECK(trash_parent_ != 0) << "invalid trash_parent";
  CHECK(dst_parent_ != 0) << "invalid dst_parent";
  CHECK(!trash_name_.empty()) << "invalid trash_name";

  // Caller already parsed file_ino out of trash_name (see ParseTrashEntryName
  // in FileSystem::RestoreFromTrash), so all four reads are independent and
  // can be served by a single BatchGet. trash_parent_ is always a sub-trash
  // bucket ino.
  const std::string trash_dentry_key = MetaCodec::EncodeDentryKey(fs_id_, trash_parent_, trash_name_);
  const std::string inode_key = MetaCodec::EncodeInodeKey(fs_id_, expected_file_ino_);
  const std::string dst_parent_key = MetaCodec::EncodeInodeKey(fs_id_, dst_parent_);
  const std::string dst_dentry_key = MetaCodec::EncodeDentryKey(fs_id_, dst_parent_, dst_name_);

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet({trash_dentry_key, inode_key, dst_parent_key, dst_dentry_key}, kvs);
  if (!status.ok()) return status;

  std::unordered_map<std::string, std::string> kv_map;
  kv_map.reserve(kvs.size());
  for (auto& kv : kvs) {
    kv_map.emplace(std::move(kv.key), std::move(kv.value));
  }

  auto trash_dentry_it = kv_map.find(trash_dentry_key);
  if (trash_dentry_it == kv_map.end()) {
    return Status(pb::error::ENOT_FOUND, "trash entry not found");
  }
  auto trash_dentry = MetaCodec::DecodeDentryValue(trash_dentry_it->second);
  if (trash_dentry.ino() != expected_file_ino_) {
    return Status(pb::error::ENOT_FOUND, "trash entry inode mismatch");
  }

  auto inode_it = kv_map.find(inode_key);
  if (inode_it == kv_map.end()) {
    return Status(pb::error::ENOT_FOUND, "inode not found");
  }
  auto attr = MetaCodec::DecodeInodeValue(inode_it->second);

  auto dst_parent_it = kv_map.find(dst_parent_key);
  if (dst_parent_it == kv_map.end()) {
    return Status(pb::error::ENOT_FOUND, "destination parent not found");
  }
  auto dst_parent_attr = MetaCodec::DecodeInodeValue(dst_parent_it->second);

  // In tree-rebuild (allow_trash_parent_) mode, refuse to graft directly
  // into an hour bucket: its parent is kTrashInodeId. Grafting is only allowed
  // onto trashed user directories (whose parents are other trashed dirs).
  if (allow_trash_parent_ && IsTrashInode(dst_parent_)) {
    for (Ino parent : dst_parent_attr.parents()) {
      if (parent == kTrashInodeId) {
        return Status(pb::error::ENOT_SUPPORT, "cannot restore under .trash hour bucket");
      }
    }
  }

  // Same-name dentry at destination must not exist.
  if (kv_map.find(dst_dentry_key) != kv_map.end()) {
    return Status(pb::error::EEXISTED, fmt::format("destination {}/{} already exists", dst_parent_, dst_name_));
  }

  txn->Delete(trash_dentry_key);

  // Create destination dentry under dst_parent_.
  DentryEntry dst_dentry;
  dst_dentry.set_fs_id(fs_id_);
  dst_dentry.set_ino(expected_file_ino_);
  dst_dentry.set_parent(dst_parent_);
  dst_dentry.set_name(dst_name_);
  dst_dentry.set_type(trash_dentry.type());
  txn->Put(dst_dentry_key, MetaCodec::EncodeDentryValue(dst_dentry));

  // Update file inode. parents_ at trash time was rewritten to
  // [trash_parent_]; restore swaps it back to dst_parent_ (the caller is
  // responsible for parsing the original parent out of trash_name and using
  // it as dst_parent_ for plain restore).
  DelParentIno(attr, trash_parent_);
  AddParentIno(attr, dst_parent_);
  attr.set_ctime(std::max(attr.ctime(), GetTime()));
  attr.set_version(attr.version() + 1);
  txn->Put(inode_key, MetaCodec::EncodeInodeValue(attr));

  // Update dst_parent: nlink++ if restoring directory, ctime/mtime++.
  if (trash_dentry.type() == pb::mds::FileType::DIRECTORY) {
    dst_parent_attr.set_nlink(dst_parent_attr.nlink() + 1);
  }
  dst_parent_attr.set_ctime(std::max(dst_parent_attr.ctime(), GetTime()));
  dst_parent_attr.set_mtime(std::max(dst_parent_attr.mtime(), GetTime()));
  dst_parent_attr.set_version(dst_parent_attr.version() + 1);
  txn->Put(dst_parent_key, MetaCodec::EncodeInodeValue(dst_parent_attr));

  result_.dst_parent_attr = dst_parent_attr;
  result_.file_attr = attr;
  result_.file_ino = expected_file_ino_;
  result_.file_type = trash_dentry.type();

  return Status::OK();
}

Status BatchTrashUnlinkOperation::Run(TxnUPtr& txn) {
  const uint32_t fs_id = dentries_[0].FsId();
  const Ino parent = dentries_[0].ParentIno();

  // BatchGet child inode + dentry for each entry (parent attr intentionally omitted).
  std::vector<std::string> keys;
  keys.reserve(dentries_.size() * 2);
  for (const auto& dentry : dentries_) {
    keys.push_back(MetaCodec::EncodeInodeKey(fs_id, dentry.INo()));
    keys.push_back(MetaCodec::EncodeDentryKey(fs_id, parent, dentry.Name()));
  }

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(keys, kvs);
  if (!status.ok()) return status;

  // Index results by key so we can tolerate missing entries.
  std::unordered_map<std::string, std::string> kv_map;
  kv_map.reserve(kvs.size());
  for (auto& kv : kvs) {
    kv_map.emplace(std::move(kv.key), std::move(kv.value));
  }

  std::vector<AttrEntry> processed_attrs;
  processed_attrs.reserve(dentries_.size());
  uint32_t skipped = 0;

  for (const auto& dentry : dentries_) {
    const std::string child_key = MetaCodec::EncodeInodeKey(fs_id, dentry.INo());
    const std::string dentry_key = MetaCodec::EncodeDentryKey(fs_id, parent, dentry.Name());

    auto dentry_it = kv_map.find(dentry_key);
    if (dentry_it == kv_map.end()) {
      // Dentry already gone — previous partial cleanup handled it.
      ++skipped;
      continue;
    }

    auto child_it = kv_map.find(child_key);
    if (child_it == kv_map.end()) {
      // Orphan dentry: inode gone, dentry stale. Just drop the dentry.
      txn->Delete(dentry_key);
      ++skipped;
      continue;
    }

    auto attr = MetaCodec::DecodeInodeValue(child_it->second);
    attr.set_nlink(attr.nlink() - 1);
    DelParentIno(attr, parent);
    attr.set_ctime(std::max(attr.ctime(), GetTime()));
    attr.set_version(attr.version() + 1);
    txn->Put(child_key, MetaCodec::EncodeInodeValue(attr));

    if (attr.nlink() <= 0) {
      txn->Put(MetaCodec::EncodeDelFileKey(fs_id, attr.ino()), MetaCodec::EncodeDelFileValue(attr));
    }

    txn->Delete(dentry_key);
    processed_attrs.push_back(std::move(attr));
  }

  result_.child_attrs = std::move(processed_attrs);
  result_.skipped_count = skipped;

  return Status::OK();
}

Status CleanTrashBucketOperation::Run(TxnUPtr& txn) {
  txn->Delete(MetaCodec::EncodeInodeKey(fs_id_, sub_trash_ino_));
  txn->Delete(MetaCodec::EncodeDentryKey(fs_id_, kTrashInodeId, bucket_name_));
  return Status::OK();
}

Status RenameOperation::Run(TxnUPtr& txn) {
  uint64_t time_ns = GetTime();

  LOG(INFO) << fmt::format("[operation.{}] rename old_parent({}), old_name({}), new_parent_ino({}), new_name({}).",
                           fs_id_, old_parent_, old_name_, new_parent_, new_name_);

  bool is_same_parent = (old_parent_ == new_parent_);

  // batch get old parent attr/child dentry and new parent attr/child dentry.
  // When trash is Enabled we speculatively read the sub_trash / bucket-dentry
  // keys in the same round-trip so the trash branch below can stay in one txn.
  std::string old_parent_key = MetaCodec::EncodeInodeKey(fs_id_, old_parent_);
  std::string old_dentry_key = MetaCodec::EncodeDentryKey(fs_id_, old_parent_, old_name_);
  std::string new_parent_key = MetaCodec::EncodeInodeKey(fs_id_, new_parent_);
  std::string new_dentry_key = MetaCodec::EncodeDentryKey(fs_id_, new_parent_, new_name_);

  std::vector<std::string> keys = {old_parent_key, old_dentry_key, new_dentry_key};
  for (uint32_t i = 0; i < kDirAttrMutationNum; ++i) {
    keys.push_back(MetaCodec::EncodeDirInodeMutationKey(fs_id_, old_parent_, i));
  }
  if (!is_same_parent) {
    keys.push_back(new_parent_key);
    for (uint32_t i = 0; i < kDirAttrMutationNum; ++i) {
      keys.push_back(MetaCodec::EncodeDirInodeMutationKey(fs_id_, new_parent_, i));
    }
  }
  std::string bucket_dentry_key;
  if (trash_.Enabled() && !trash_.IsAlreadyExist()) {
    bucket_dentry_key = MetaCodec::EncodeDentryKey(fs_id_, kTrashInodeId, trash_.bucket_name);
    if (bucket_dentry_key != old_dentry_key && bucket_dentry_key != new_dentry_key) {
      keys.push_back(bucket_dentry_key);
    }
  }

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(keys, kvs);
  LOG(INFO) << fmt::format("[operation.{}] kvs size({})", fs_id_, kvs.size());
  if (!status.ok()) return status;

  if (kvs.size() < 2) {
    return Status(pb::error::ENOT_FOUND, "not found old parent inode/old dentry");
  }

  AttrEntry old_parent_attr, new_parent_attr;
  DentryEntry old_dentry, prev_new_dentry;
  bool trash_bucket_exist = false;
  for (const auto& kv : kvs) {
    if (kv.key == old_parent_key) {
      old_parent_attr = MetaCodec::DecodeInodeValue(kv.value);
      if (is_same_parent) new_parent_attr = old_parent_attr;

    } else if (kv.key == old_dentry_key) {
      old_dentry = MetaCodec::DecodeDentryValue(kv.value);

      if (!bucket_dentry_key.empty() && kv.key == bucket_dentry_key) {
        trash_bucket_exist = true;
      }

    } else if (kv.key == new_parent_key) {
      new_parent_attr = MetaCodec::DecodeInodeValue(kv.value);

    } else if (kv.key == new_dentry_key) {
      prev_new_dentry = MetaCodec::DecodeDentryValue(kv.value);
      if (!bucket_dentry_key.empty() && kv.key == bucket_dentry_key) {
        trash_bucket_exist = true;
      }

    } else if (MetaCodec::IsDirInodeMutationKey(kv.key)) {
      auto mutation = MetaCodec::DecodeDirInodeMutationValue(kv.value);
      if (mutation.ino() == old_parent_) {
        result_.old_parent_attr_with_mutation.mutations.push_back(mutation);
      }
      if (mutation.ino() == new_parent_) {
        result_.new_parent_attr_with_mutation.mutations.push_back(mutation);
      }

    } else if (kv.key == bucket_dentry_key) {
      trash_bucket_exist = true;

    } else {
      LOG(FATAL) << fmt::format("[operation.{}.{}] invalid key({}).", fs_id_, old_parent_, Helper::StringToHex(kv.key));
    }
  }
  // kTrashInodeId is a virtual directory: no inode key in KV. Synthesize the
  // attr on demand so admin escape-hatch renames (root moving an hour bucket
  // out of .trash) work, instead of CHECK-failing on the missing record.
  if (old_parent_attr.ino() == 0 && old_parent_ == kTrashInodeId) {
    old_parent_attr = BuildTrashInodeAttr(fs_id_);
  }
  if (new_parent_attr.ino() == 0 && new_parent_ == kTrashInodeId) {
    new_parent_attr = BuildTrashInodeAttr(fs_id_);
  }
  CHECK(old_parent_attr.ino() > 0) << "old parent attr is null.";
  CHECK(new_parent_attr.ino() > 0) << "new parent attr is null.";
  CHECK(old_dentry.ino() > 0) << "old dentry is null.";

  bool is_exist_new_dentry = (prev_new_dentry.ino() != 0);

  // get old inode/prev new inode
  keys.clear(), kvs.clear();
  std::string old_inode_key = MetaCodec::EncodeInodeKey(fs_id_, old_dentry.ino());
  std::string prev_new_inode_key = MetaCodec::EncodeInodeKey(fs_id_, prev_new_dentry.ino());
  keys.push_back(old_inode_key);
  if (is_exist_new_dentry) keys.push_back(prev_new_inode_key);
  status = txn->BatchGet(keys, kvs);
  if (!status.ok()) return status;

  if (kvs.empty()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found old inode({})", old_dentry.ino()));
  }

  AttrEntry old_attr, prev_new_attr;
  for (const auto& kv : kvs) {
    if (kv.key == old_inode_key) {
      old_attr = MetaCodec::DecodeInodeValue(kv.value);

    } else if (kv.key == prev_new_inode_key) {
      prev_new_attr = MetaCodec::DecodeInodeValue(kv.value);
    }
  }

  if (is_exist_new_dentry) {
    CHECK(prev_new_attr.ino() != 0) << fmt::format("prev new inode is null, old({}/{}) new({}/{}) ino({})", old_parent_,
                                                   old_name_, new_parent_, new_name_, prev_new_dentry.ino());

    if (trash_.Enabled()) {
      CHECK(trash_.bucket_ino != 0) << "invalid trash bucket ino(0)";

      // Move overwritten entry to trash instead of deleting.
      if (prev_new_dentry.type() == pb::mds::DIRECTORY) {
        bool is_empty;
        Ino child_ino = 0;
        status = CheckDirEmpty(txn, fs_id_, prev_new_dentry.ino(), is_empty, child_ino);
        if (!status.ok()) return status;
        if (!is_empty) {
          return Status(pb::error::ENOT_EMPTY, fmt::format("new dentry({}/{}) is not empty, child ino({})", new_parent_,
                                                           new_name_, child_ino));
        }
      }

      PutTrashBucket(txn, fs_id_, trash_, trash_bucket_exist);

      // Mark overwritten inode as in trash by replacing new_parent_ in
      // parents_ with the sub_trash bucket. parents_[*] >= kTrashInodeId is
      // the trash-membership marker; the original parent is recovered from
      // trash_entry_name on restore.
      DelParentIno(prev_new_attr, new_parent_);
      AddParentIno(prev_new_attr, trash_.bucket_ino);
      prev_new_attr.set_ctime(std::max(prev_new_attr.ctime(), time_ns));
      prev_new_attr.set_version(prev_new_attr.version() + 1);
      txn->Put(prev_new_inode_key, MetaCodec::EncodeInodeValue(prev_new_attr));

      // Bucket inode attr is intentionally not touched (see trash.cc).
      // Create trash dentry under sub_trash.
      std::string trash_entry_name = BuildTrashEntryName(new_parent_, prev_new_dentry.ino(), new_name_);
      DentryEntry trash_dentry;
      trash_dentry.set_fs_id(fs_id_);
      trash_dentry.set_ino(prev_new_dentry.ino());
      trash_dentry.set_parent(trash_.bucket_ino);
      trash_dentry.set_name(trash_entry_name);
      trash_dentry.set_type(prev_new_dentry.type());
      txn->Put(MetaCodec::EncodeDentryKey(fs_id_, trash_.bucket_ino, trash_entry_name),
               MetaCodec::EncodeDentryValue(trash_dentry));

    } else if (prev_new_dentry.type() == pb::mds::DIRECTORY) {
      // check new dentry is empty
      bool is_empty;
      Ino child_ino = 0;
      status = CheckDirEmpty(txn, fs_id_, prev_new_dentry.ino(), is_empty, child_ino);
      if (!status.ok()) return status;

      if (!is_empty) {
        return Status(pb::error::ENOT_EMPTY,
                      fmt::format("new dentry({}/{}) is not empty, child ino({})", new_parent_, new_name_, child_ino));
      }

      // delete exist new inode
      txn->Delete(prev_new_inode_key);
      for (uint32_t i = 0; i < kDirAttrMutationNum; ++i) {
        txn->Delete(MetaCodec::EncodeDirInodeMutationKey(fs_id_, prev_new_dentry.ino(), i));
      }

    } else {
      // update exist new inode nlink
      prev_new_attr.set_nlink(prev_new_attr.nlink() - 1);
      prev_new_attr.set_ctime(std::max(prev_new_attr.ctime(), time_ns));
      prev_new_attr.set_mtime(std::max(prev_new_attr.mtime(), time_ns));
      prev_new_attr.set_version(prev_new_attr.version() + 1);

      // update exist new inode attr
      txn->Put(prev_new_inode_key, MetaCodec::EncodeInodeValue(prev_new_attr));

      if (prev_new_attr.nlink() <= 0) {
        // save delete file info
        txn->Put(MetaCodec::EncodeDelFileKey(fs_id_, prev_new_attr.ino()),
                 MetaCodec::EncodeDelFileValue(prev_new_attr));
      }
    }
  }

  // delete old dentry
  txn->Delete(old_dentry_key);

  // add new dentry
  DentryEntry new_dentry;
  new_dentry.set_fs_id(fs_id_);
  new_dentry.set_name(new_name_);
  new_dentry.set_ino(old_dentry.ino());
  new_dentry.set_type(old_dentry.type());
  new_dentry.set_parent(new_parent_);

  txn->Put(new_dentry_key, MetaCodec::EncodeDentryValue(new_dentry));

  // update old inode attr
  old_attr.set_ctime(std::max(old_attr.ctime(), time_ns));
  if (!is_same_parent) {
    DelParentIno(old_attr, old_parent_);
    AddParentIno(old_attr, new_parent_);
  }
  old_attr.set_version(old_attr.version() + 1);

  txn->Put(old_inode_key, MetaCodec::EncodeInodeValue(old_attr));

  // update old parent inode attr
  old_parent_attr.set_ctime(std::max(old_parent_attr.ctime(), time_ns));
  old_parent_attr.set_mtime(std::max(old_parent_attr.mtime(), time_ns));
  // Skip nlink-- when src is a hour bucket (see trash.cc).
  if (old_dentry.type() == pb::mds::FileType::DIRECTORY &&
      (!is_same_parent || (is_same_parent && is_exist_new_dentry)) && !IsTrashInode(old_parent_)) {
    old_parent_attr.set_nlink(old_parent_attr.nlink() - 1);
  }
  old_parent_attr.set_version(old_parent_attr.version() + 1);

  // kTrashInodeId is virtual — its attr is synthesized on read, never written.
  if (old_parent_ != kTrashInodeId) {
    status = txn->Put(old_parent_key, MetaCodec::EncodeInodeValue(old_parent_attr));
  }

  if (!is_same_parent) {
    // update new parent inode attr
    new_parent_attr.set_ctime(std::max(new_parent_attr.ctime(), time_ns));
    new_parent_attr.set_mtime(std::max(new_parent_attr.mtime(), time_ns));
    // Skip nlink++ when dst is a hour bucket (see trash.cc).
    if (new_dentry.type() == pb::mds::FileType::DIRECTORY && !is_exist_new_dentry && !IsTrashInode(new_parent_))
      new_parent_attr.set_nlink(new_parent_attr.nlink() + 1);
    new_parent_attr.set_version(new_parent_attr.version() + 1);

    if (new_parent_ != kTrashInodeId) {
      txn->Put(new_parent_key, MetaCodec::EncodeInodeValue(new_parent_attr));
    }
  }

  result_.old_parent_attr_with_mutation.attr = old_parent_attr;
  result_.old_dentry = old_dentry;
  result_.old_attr = old_attr;
  result_.new_parent_attr_with_mutation.attr = new_parent_attr;
  result_.prev_new_dentry = prev_new_dentry;
  result_.prev_new_attr = prev_new_attr;
  result_.new_dentry = new_dentry;

  result_.is_same_parent = is_same_parent;
  result_.is_exist_new_dentry = is_exist_new_dentry;

  return Status::OK();
}

Status CompactChunkOperation::Run(TxnUPtr& txn) {
  const uint32_t fs_id = fs_id_;
  const uint32_t chunk_index = param_.chunk_index;

  CHECK(fs_id > 0) << "fs_id is 0";
  CHECK(ino_ > 0) << "ino is 0.";
  CHECK(!param_.new_slices.empty()) << "new_slices is empty.";
  CHECK(param_.end_pos >= param_.start_pos) << "invalid pos range.";
  CHECK(param_.new_slices.size() < (param_.end_pos - param_.start_pos)) << "new_slices size invalid.";

  std::string chunk_key = MetaCodec::EncodeChunkKey(fs_id, ino_, chunk_index);

  std::string value;
  auto status = txn->Get(chunk_key, value);
  if (!status.ok()) return status;

  ChunkEntry chunk = MetaCodec::DecodeChunkValue(value);
  CHECK(chunk.index() == chunk_index) << "chunk index not match.";

  const auto& slices = chunk.slices();
  if (slices.at(param_.start_pos).id() != param_.start_slice_id) {
    result_.chunk = chunk;
    return Status(pb::error::EILLEGAL_PARAMTETER,
                  fmt::format("not match start slice id({})", slices.at(param_.start_pos).id()));
  }
  if (slices.at(param_.end_pos).id() != param_.end_slice_id) {
    result_.chunk = chunk;
    return Status(pb::error::EILLEGAL_PARAMTETER,
                  fmt::format("not match end slice id({})", slices.at(param_.end_pos).id()));
  }

  // generate trash slice list
  TrashSliceList trash_slice_list;
  for (uint32_t i = param_.start_pos; i <= param_.end_pos; ++i) {
    const auto& slice = slices.at(i);

    // filter by new slices
    bool found = false;
    for (const auto& new_slice : param_.new_slices) {
      if (new_slice.id() == slice.id()) {
        found = true;
        break;
      }
    }
    if (found) continue;

    TrashSliceEntry* trash_slice = trash_slice_list.add_slices();
    trash_slice->set_fs_id(fs_id);
    trash_slice->set_ino(ino_);
    trash_slice->set_chunk_index(chunk.index());
    trash_slice->set_block_size(chunk.block_size());
    trash_slice->set_chunk_size(chunk.chunk_size());
    trash_slice->mutable_slice()->CopyFrom(slice);
  }
  trash_slice_list.set_time_ms(utils::TimestampMs());

  // update chunk slices
  uint32_t old_slice_size = chunk.slices_size();
  uint32_t pos = param_.start_pos;
  for (const auto& slice : param_.new_slices) {
    chunk.mutable_slices()->at(pos++) = slice;
  }

  for (int i = param_.end_pos + 1; i < chunk.slices_size(); ++i) {
    chunk.mutable_slices()->at(pos++) = chunk.mutable_slices()->at(i);
  }

  chunk.mutable_slices()->DeleteSubrange(pos, chunk.slices_size() - pos);
  chunk.set_last_compaction_time_ms(utils::TimestampMs());
  // record compacted slices
  auto* compacted_slices = chunk.add_compacted_slices();
  compacted_slices->set_time_ms(utils::TimestampMs());
  for (const auto& slice : trash_slice_list.slices()) {
    compacted_slices->add_slice_ids(slice.slice().id());
  }

  chunk.set_version(chunk.version() + 1);

  LOG(INFO) << fmt::format(
      "[operation.{}.{}.{}] compact chunk, pos[{},{}] slice_id[{},{}] new_slices({}) old_slices({}) final_slices({}).",
      fs_id, ino_, chunk_index, param_.start_pos, param_.end_pos, param_.start_slice_id, param_.end_slice_id,
      Helper::ToString(param_.new_slices), old_slice_size, chunk.slices_size());

  LOG_DEBUG << fmt::format("[operation.{}.{}] update chunk, version({}), value({}).", fs_id, ino_, chunk.version(),
                           chunk.ShortDebugString());

  txn->Put(chunk_key, MetaCodec::EncodeChunkValue(chunk));

  // save trash slice list
  txn->Put(MetaCodec::EncodeDelSliceKey(fs_id, ino_, chunk.index(), GetTime()),
           MetaCodec::EncodeDelSliceValue(trash_slice_list));

  result_.chunk = chunk;

  return Status::OK();
}

struct ByteRange {
  uint64_t start;
  uint64_t end;

  uint64_t Len() const { return end - start; }
};

struct VisibleSliceRange {
  ByteRange range;
  const SliceEntry* slice{nullptr};  // nullptr means sparse hole
  uint64_t slice_start{0};

  bool IsZero() const { return slice == nullptr || slice->id() == 0; }
};

struct DstSlice {
  uint64_t chunk_index;
  SliceEntry slice;
};

static bool IntersectRange(const ByteRange& lhs, const ByteRange& rhs, ByteRange& out) {
  out.start = std::max(lhs.start, rhs.start);
  out.end = std::min(lhs.end, rhs.end);
  return out.start < out.end;
}

static void AppendRemainderRanges(const ByteRange& range, const ByteRange& covered, std::vector<ByteRange>& out) {
  if (range.start < covered.start) {
    out.push_back({.start = range.start, .end = covered.start});
  }
  if (covered.end < range.end) {
    out.push_back({.start = covered.end, .end = range.end});
  }
}

static std::vector<VisibleSliceRange> CollectVisibleSliceRanges(
    const absl::flat_hash_map<uint64_t, ChunkEntry>& src_chunks, const ByteRange& copy_range, uint32_t chunk_size) {
  std::vector<ByteRange> unmatched_ranges{copy_range};
  std::vector<VisibleSliceRange> visible_ranges;

  // Iterate only over the source chunks intersecting [src_off, src_end).
  // The end index uses ceiling division so the last partially-covered chunk
  // is included.
  uint32_t src_chunk_index_begin = copy_range.start / chunk_size;
  uint32_t src_chunk_index_end = (copy_range.end + chunk_size - 1) / chunk_size;

  for (uint32_t i = src_chunk_index_begin; i < src_chunk_index_end; ++i) {
    auto it = src_chunks.find(i);
    if (it == src_chunks.end()) continue;
    const auto& src_chunk = it->second;

    const uint64_t src_chunk_pos = static_cast<uint64_t>(src_chunk.index()) * chunk_size;
    for (auto slice_it = src_chunk.slices().rbegin(); slice_it != src_chunk.slices().rend(); ++slice_it) {
      const auto& slice = *slice_it;
      if (slice.len() == 0) continue;

      const ByteRange slice_range{.start = src_chunk_pos + slice.pos(),
                                  .end = src_chunk_pos + slice.pos() + slice.len()};

      std::vector<ByteRange> next_unmatched_ranges;
      for (const auto& range : unmatched_ranges) {
        ByteRange visible_range;
        if (!IntersectRange(range, slice_range, visible_range)) {
          next_unmatched_ranges.push_back(range);
          continue;
        }

        visible_ranges.push_back({.range = visible_range, .slice = &slice, .slice_start = slice_range.start});
        AppendRemainderRanges(range, visible_range, next_unmatched_ranges);
      }

      unmatched_ranges = std::move(next_unmatched_ranges);
      if (unmatched_ranges.empty()) break;
    }
  }

  for (const auto& range : unmatched_ranges) {
    visible_ranges.push_back({.range = range});
  }

  std::sort(visible_ranges.begin(), visible_ranges.end(),  // NOLINT
            [](const VisibleSliceRange& a, const VisibleSliceRange& b) { return a.range.start < b.range.start; });

  return visible_ranges;
}

static void AppendMappedSlices(const VisibleSliceRange& visible, uint64_t src_off, uint64_t dst_off,
                               uint32_t chunk_size, std::vector<DstSlice>& out) {
  uint64_t dst_logical = dst_off + (visible.range.start - src_off);
  uint64_t remaining = visible.range.Len();
  uint64_t cursor_off = visible.IsZero() ? 0 : visible.slice->off() + visible.range.start - visible.slice_start;

  while (remaining > 0) {
    const uint64_t dst_chunk_index = dst_logical / chunk_size;
    const uint64_t dst_chunk_pos = dst_logical % chunk_size;
    const uint64_t take = std::min(remaining, chunk_size - dst_chunk_pos);

    DstSlice dst_slice;
    dst_slice.chunk_index = dst_chunk_index;
    SliceEntry slice;
    if (visible.IsZero()) {
      slice.set_id(0);
      slice.set_size(0);
      slice.set_off(0);
    } else {
      slice.set_id(visible.slice->id());
      slice.set_size(visible.slice->size());
      slice.set_off(cursor_off);
    }
    slice.set_pos(dst_chunk_pos);
    slice.set_len(take);

    dst_slice.slice = slice;
    out.push_back(std::move(dst_slice));

    dst_logical += take;
    cursor_off += take;
    remaining -= take;
  }
}

// Clone slice references from a source byte range [src_off, src_off+len) into
// a destination byte range starting at dst_off. Source holes and zero slices are
// cloned as zero slices so they overwrite any existing destination data.
static absl::flat_hash_map<uint64_t, std::vector<SliceEntry>> CloneSlice(
    const absl::flat_hash_map<uint64_t, ChunkEntry>& src_chunks, uint64_t src_off, uint64_t dst_off, uint64_t len,
    uint32_t chunk_size) {
  const ByteRange copy_range{.start = src_off, .end = src_off + len};
  std::vector<DstSlice> dst_mapped;

  auto visible_slice_ranges = CollectVisibleSliceRanges(src_chunks, copy_range, chunk_size);
  for (const auto& visible : visible_slice_ranges) {
    AppendMappedSlices(visible, src_off, dst_off, chunk_size, dst_mapped);
  }

  // Group the produced slices by destination chunk index for the caller.
  absl::flat_hash_map<uint64_t, std::vector<SliceEntry>> dst_copy_slice_map;
  for (auto& item : dst_mapped) {
    dst_copy_slice_map[item.chunk_index].push_back(std::move(item.slice));
  }

  return dst_copy_slice_map;
}

absl::flat_hash_map<uint64_t, std::vector<SliceEntry>> CopyFileRangeOperation::TestCloneSlice(
    absl::flat_hash_map<uint64_t, ChunkEntry> src_chunks, uint64_t src_off, uint64_t dst_off, uint64_t len,
    uint32_t chunk_size) {
  return CloneSlice(src_chunks, src_off, dst_off, len, chunk_size);
}

static Status UpsertSliceRef(TxnUPtr& txn, absl::flat_hash_map<uint64_t, std::vector<SliceEntry>> copy_slice_map,
                             Ino src_ino, Ino dst_ino) {
  // 5.a) collect unique slice ids
  std::map<uint64_t, uint64_t> copy_slice_sizes;
  for (const auto& [_, slices] : copy_slice_map) {
    for (const auto& slice : slices) {
      if (slice.id() == 0) continue;
      copy_slice_sizes.emplace(slice.id(), slice.size());
    }
  }

  // 5.b) batch get slice ref entries
  std::vector<std::string> copy_slice_ref_keys;
  copy_slice_ref_keys.reserve(copy_slice_sizes.size());
  for (const auto& [slice_id, _] : copy_slice_sizes) {
    copy_slice_ref_keys.push_back(MetaCodec::EncodeSliceRefKey(slice_id));
  }

  if (!copy_slice_ref_keys.empty()) {
    std::vector<KeyValue> slice_ref_kvs;
    auto status = txn->BatchGet(copy_slice_ref_keys, slice_ref_kvs);
    if (!status.ok() && status.error_code() != pb::error::ENOT_FOUND) {
      return status;
    }

    // 5.c) update slice ref entries
    absl::flat_hash_map<uint64_t, SliceRefEntry> slice_ref_map;
    for (auto& kv : slice_ref_kvs) {
      SliceRefEntry slice_ref = MetaCodec::DecodeSliceRefValue(kv.value);
      slice_ref_map[slice_ref.id()] = slice_ref;
    }

    for (const auto& [slice_id, slice_size] : copy_slice_sizes) {
      auto it = slice_ref_map.find(slice_id);
      if (it != slice_ref_map.end()) {
        auto& slice_ref = it->second;
        slice_ref.set_ref_count(slice_ref.ref_count() + 1);
        slice_ref.add_inos(dst_ino);

      } else {
        SliceRefEntry slice_ref;
        slice_ref.set_id(slice_id);
        slice_ref.set_size(slice_size);
        slice_ref.set_ref_count(2);
        slice_ref.add_inos(src_ino);
        slice_ref.add_inos(dst_ino);

        slice_ref_map.insert({slice_ref.id(), slice_ref});
      }
    }

    for (const auto& [_, slice_ref] : slice_ref_map) {
      txn->Put(MetaCodec::EncodeSliceRefKey(slice_ref.id()), MetaCodec::EncodeSliceRefValue(slice_ref));
    }
  }

  return Status::OK();
}

Status CopyFileRangeOperation::Run(TxnUPtr& txn) {
  const uint32_t fs_id = fs_info_.fs_id();
  const uint64_t chunk_size = fs_info_.chunk_size();
  const uint64_t block_size = fs_info_.block_size();
  const uint64_t now_ns = GetTime();

  const bool same_file = (param_.src_ino == param_.dst_ino);

  // 1) batch get src/dst inodes + src chunks in [begin, end)
  const std::string src_inode_key = MetaCodec::EncodeInodeKey(fs_id, param_.src_ino);
  const std::string dst_inode_key = MetaCodec::EncodeInodeKey(fs_id, param_.dst_ino);

  std::vector<std::string> phase1_keys{src_inode_key, dst_inode_key};

  uint32_t src_chunk_index_begin = param_.src_off / chunk_size;
  uint32_t src_chunk_index_end = (param_.src_off + param_.len + chunk_size - 1) / chunk_size;
  for (uint32_t i = src_chunk_index_begin; i < src_chunk_index_end; ++i) {
    phase1_keys.push_back(MetaCodec::EncodeChunkKey(fs_id, param_.src_ino, i));
  }

  uint32_t dst_chunk_index_begin = param_.dst_off / chunk_size;
  uint32_t dst_chunk_index_end = (param_.dst_off + param_.len + chunk_size - 1) / chunk_size;
  for (uint32_t i = dst_chunk_index_begin; i < dst_chunk_index_end; ++i) {
    phase1_keys.push_back(MetaCodec::EncodeChunkKey(fs_id, param_.dst_ino, i));
  }

  // Same-file copy duplicates the inode key and any chunk keys shared by the
  // src/dst ranges; drop duplicates so backends never see repeated keys.
  if (same_file) {
    std::sort(phase1_keys.begin(), phase1_keys.end());                                          // NOLINT
    phase1_keys.erase(std::unique(phase1_keys.begin(), phase1_keys.end()), phase1_keys.end());  // NOLINT
  }

  std::vector<KeyValue> phase1_kvs;
  auto status = txn->BatchGet(phase1_keys, phase1_kvs);
  if (!status.ok()) return status;

  AttrEntry src_attr;
  AttrEntry dst_attr;
  absl::flat_hash_map<uint64_t, ChunkEntry> src_chunks;  // index -> chunk
  absl::flat_hash_map<uint64_t, ChunkEntry> dst_chunks;  // index -> chunk
  for (const auto& kv : phase1_kvs) {
    if (kv.key == src_inode_key) {
      src_attr = MetaCodec::DecodeInodeValue(kv.value);

    } else if (kv.key == dst_inode_key) {
      dst_attr = MetaCodec::DecodeInodeValue(kv.value);

    } else if (MetaCodec::IsChunkKey(kv.key)) {
      uint32_t fs_id;
      uint64_t ino;
      uint64_t chunk_index;
      MetaCodec::DecodeChunkKey(kv.key, fs_id, ino, chunk_index);
      auto chunk = MetaCodec::DecodeChunkValue(kv.value);
      if (ino == param_.src_ino) {
        // Same-file copy: the chunk is both a clone source and a destination
        // whose existing slices must be preserved, so route it into both.
        if (same_file) dst_chunks[chunk.index()] = chunk;
        src_chunks[chunk.index()] = std::move(chunk);
      } else if (ino == param_.dst_ino) {
        dst_chunks[chunk.index()] = std::move(chunk);
      }
    }
  }

  // Same-file copy: the dst_inode_key branch above is shadowed by the
  // identical src key, so mirror the single decoded inode as the dst inode.
  if (same_file) dst_attr = src_attr;

  if (src_attr.ino() == 0) return Status(pb::error::ENOT_FOUND, "src inode not found");
  if (dst_attr.ino() == 0) return Status(pb::error::ENOT_FOUND, "dst inode not found");
  if (src_attr.type() != pb::mds::FileType::FILE) return Status(pb::error::ENOT_FILE, "src is not regular file");
  if (dst_attr.type() != pb::mds::FileType::FILE) return Status(pb::error::ENOT_FILE, "dst is not regular file");

  // 2) check src offset and length
  if (param_.src_off >= src_attr.length()) {
    return Status(pb::error::EOUT_OF_RANGE, "src offset beyond EOF");
  }

  // actually copy length
  const uint64_t actual_len = std::min(param_.len, src_attr.length() - param_.src_off);
  CHECK(actual_len != 0) << fmt::format("invalid copy length {}, src_off {}, src_attr.length {}", param_.len,
                                        param_.src_off, src_attr.length());

  // 3) clone slice
  absl::flat_hash_map<uint64_t, std::vector<SliceEntry>> copy_slice_map =
      CloneSlice(src_chunks, param_.src_off, param_.dst_off, actual_len, chunk_size);

  result_.effected_chunks.clear();
  // 4) update dst chunks; if chunk not exist, create new one; Put
  dst_chunk_index_end = (param_.dst_off + actual_len + chunk_size - 1) / chunk_size;
  for (uint32_t i = dst_chunk_index_begin; i < dst_chunk_index_end; ++i) {
    auto it = dst_chunks.find(i);
    ChunkEntry chunk;
    if (it != dst_chunks.end()) {
      chunk = std::move(it->second);
    } else {
      chunk.set_index(i);
      chunk.set_chunk_size(chunk_size);
      chunk.set_block_size(block_size);
    }

    auto it2 = copy_slice_map.find(i);
    if (it2 != copy_slice_map.end()) {
      for (const auto& slice : it2->second) {
        *chunk.add_slices() = slice;
      }
    }

    chunk.set_version(chunk.version() + 1);
    LOG_DEBUG << fmt::format("[operation.{}.{}] update chunk, version({}), value({}).", fs_id, param_.dst_ino,
                             chunk.version(), chunk.ShortDebugString());

    txn->Put(MetaCodec::EncodeChunkKey(fs_id, param_.dst_ino, i), MetaCodec::EncodeChunkValue(chunk));
    result_.effected_chunks.push_back(chunk);
  }

  // 5) upsert slice ref
  status = UpsertSliceRef(txn, copy_slice_map, param_.src_ino, param_.dst_ino);
  if (!status.ok()) return status;

  // 6) update dst inode (size grow, mtime/ctime)
  const uint64_t new_end = param_.dst_off + actual_len;
  const int64_t length_delta = new_end > dst_attr.length() ? static_cast<int64_t>(new_end - dst_attr.length()) : 0;
  if (length_delta > 0) dst_attr.set_length(new_end);
  dst_attr.set_mtime(std::max(dst_attr.mtime(), now_ns));
  dst_attr.set_ctime(std::max(dst_attr.ctime(), now_ns));
  dst_attr.set_shared_slice(true);
  // Same-file copy writes the inode only once below, so fold the src-side
  // atime bump in here to avoid a second Put that would clobber these updates.
  if (same_file) dst_attr.set_atime(std::max(dst_attr.atime(), now_ns));
  dst_attr.set_version(dst_attr.version() + 1);
  txn->Put(dst_inode_key, MetaCodec::EncodeInodeValue(dst_attr));

  // 7) update src inode (skipped for same-file: already folded into dst above)
  if (!same_file) {
    src_attr.set_shared_slice(true);
    src_attr.set_atime(std::max(src_attr.atime(), now_ns));
    src_attr.set_version(src_attr.version() + 1);
    txn->Put(src_inode_key, MetaCodec::EncodeInodeValue(src_attr));
  }

  result_.bytes_copied = actual_len;
  result_.length_delta = length_delta;
  result_.dst_attr = dst_attr;

  return Status::OK();
}

Status SetFsQuotaOperation::Run(TxnUPtr& txn) {
  std::string key = MetaCodec::EncodeFsQuotaKey(fs_id_);
  std::string value;
  auto status = txn->Get(key, value);
  if (!status.ok() && status.error_code() != pb::error::ENOT_FOUND) {
    return status;
  }

  QuotaEntry fs_quota;
  if (!value.empty()) {
    fs_quota = MetaCodec::DecodeFsQuotaValue(value);
  } else {
    fs_quota.set_uuid(utils::GenerateUUID());
    fs_quota.set_create_time_ns(utils::TimestampNs());
  }

  if (quota_.max_bytes() > 0) fs_quota.set_max_bytes(quota_.max_bytes());
  if (quota_.max_inodes() > 0) fs_quota.set_max_inodes(quota_.max_inodes());
  if (quota_.used_inodes() > 0) fs_quota.set_used_inodes(quota_.used_inodes());
  if (quota_.used_bytes() > 0) fs_quota.set_used_bytes(quota_.used_bytes());

  fs_quota.set_version(fs_quota.version() + 1);
  txn->Put(key, MetaCodec::EncodeFsQuotaValue(fs_quota));

  result_.quota = fs_quota;

  return Status::OK();
}

Status GetFsQuotaOperation::Run(TxnUPtr& txn) {
  std::string value;
  auto status = txn->Get(MetaCodec::EncodeFsQuotaKey(fs_id_), value);
  if (!status.ok()) return status;

  if (value.empty()) {
    return Status(pb::error::ENOT_FOUND, "fs quota not found");
  }

  result_.quota = MetaCodec::DecodeFsQuotaValue(value);

  return Status::OK();
}

Status FlushFsUsageOperation::Run(TxnUPtr& txn) {
  std::string key = MetaCodec::EncodeFsQuotaKey(fs_id_);
  std::string value;
  auto status = txn->Get(key, value);
  if (!status.ok()) return status;

  CHECK(!value.empty()) << "fs quota value is empty.";

  auto fs_quota = MetaCodec::DecodeFsQuotaValue(value);

  for (const auto& usage : usages_) {
    if (usage.time_ns() > fs_quota.create_time_ns()) {
      fs_quota.set_used_bytes(fs_quota.used_bytes() + usage.bytes());
      fs_quota.set_used_inodes(fs_quota.used_inodes() + usage.inodes());
    }
  }

  fs_quota.set_version(fs_quota.version() + 1);
  txn->Put(key, MetaCodec::EncodeFsQuotaValue(fs_quota));

  result_.quota = fs_quota;

  return Status::OK();
}

Status DeleteFsQuotaOperation::Run(TxnUPtr& txn) {
  txn->Delete(MetaCodec::EncodeFsQuotaKey(fs_id_));

  return Status::OK();
}

Status SetDirQuotaOperation::Run(TxnUPtr& txn) {
  std::string key = MetaCodec::EncodeDirQuotaKey(fs_id_, ino_);
  std::string value;
  auto status = txn->Get(key, value);
  if (!status.ok() && status.error_code() != pb::error::ENOT_FOUND) {
    return status;
  }

  QuotaEntry dir_quota;
  if (!value.empty()) {
    dir_quota = MetaCodec::DecodeDirQuotaValue(value);
  } else {
    dir_quota.set_uuid(utils::GenerateUUID());
    dir_quota.set_create_time_ns(utils::TimestampNs());
  }

  if (quota_.max_bytes() > 0) dir_quota.set_max_bytes(quota_.max_bytes());
  if (quota_.max_inodes() > 0) dir_quota.set_max_inodes(quota_.max_inodes());
  if (quota_.used_inodes() > 0) dir_quota.set_used_inodes(quota_.used_inodes());
  if (quota_.used_bytes() > 0) dir_quota.set_used_bytes(quota_.used_bytes());

  dir_quota.set_version(dir_quota.version() + 1);
  txn->Put(key, MetaCodec::EncodeDirQuotaValue(dir_quota));

  result_.quota = dir_quota;

  return Status::OK();
}

Status GetDirQuotaOperation::Run(TxnUPtr& txn) {
  std::string value;
  auto status = txn->Get(MetaCodec::EncodeDirQuotaKey(fs_id_, ino_), value);
  if (!status.ok()) return status;

  if (value.empty()) {
    return Status(pb::error::ENOT_FOUND, "not found dir quota");
  }

  auto dir_quota = MetaCodec::DecodeDirQuotaValue(value);
  result_.quota = dir_quota;

  return Status::OK();
}

Status DeleteDirQuotaOperation::Run(TxnUPtr& txn) {
  std::string value;
  auto status = txn->Get(MetaCodec::EncodeDirQuotaKey(fs_id_, ino_), value);
  if (!status.ok()) return status;

  result_.quota = MetaCodec::DecodeDirQuotaValue(value);

  txn->Delete(MetaCodec::EncodeDirQuotaKey(fs_id_, ino_));

  return Status::OK();
}

Status LoadDirQuotasOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetDirQuotaRange(fs_id_);

  return txn->Scan(range, [&](const std::string& key, const std::string& value) -> bool {
    if (!MetaCodec::IsDirQuotaKey(key)) return true;

    uint32_t fs_id;
    uint64_t ino;
    MetaCodec::DecodeDirQuotaKey(key, fs_id, ino);

    auto quota = MetaCodec::DecodeDirQuotaValue(value);
    result_.quotas[ino] = quota;

    return true;
  });
}

Status FlushDirUsagesOperation::Run(TxnUPtr& txn) {
  // generate all keys
  std::vector<std::string> keys;
  keys.reserve(usage_map_.size());
  for (const auto& [ino, _] : usage_map_) {
    keys.push_back(MetaCodec::EncodeDirQuotaKey(fs_id_, ino));
  }

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(keys, kvs);
  if (!status.ok()) return status;

  for (auto& kv : kvs) {
    uint32_t fs_id;
    uint64_t ino;
    MetaCodec::DecodeDirQuotaKey(kv.key, fs_id, ino);

    auto quota = MetaCodec::DecodeDirQuotaValue(kv.value);
    auto it = usage_map_.find(ino);
    if (it != usage_map_.end()) {
      const auto& usages = it->second;
      bool has_update = false;
      for (const auto& usage : usages) {
        if (usage.time_ns() > quota.create_time_ns()) {
          has_update = true;
          quota.set_used_bytes(quota.used_bytes() + usage.bytes());
          quota.set_used_inodes(quota.used_inodes() + usage.inodes());
        }
      }

      if (has_update) {
        quota.set_version(quota.version() + 1);
        txn->Put(kv.key, MetaCodec::EncodeDirQuotaValue(quota));
      }

      result_.quotas[ino] = quota;
    }
  }

  return Status::OK();
}

Status DeleteDirStatOperation::Run(TxnUPtr& txn) {
  txn->Delete(MetaCodec::EncodeDirStatKey(fs_id_, ino_));
  return Status::OK();
}

Status BatchSetDirStatOperation::Run(TxnUPtr& txn) {
  for (const auto& [ino, dir_stat] : dir_stats_) {
    txn->Put(MetaCodec::EncodeDirStatKey(fs_id_, ino), MetaCodec::EncodeDirStatValue(dir_stat));
  }
  return Status::OK();
}

Status GetDirStatOperation::Run(TxnUPtr& txn) {
  std::string key = MetaCodec::EncodeDirStatKey(fs_id_, ino_);
  std::string value;
  auto status = txn->Get(key, value);
  if (status.error_code() == pb::error::ENOT_FOUND) {
    result_.found = false;
    return Status::OK();
  }
  if (!status.ok()) return status;
  result_.found = true;
  result_.dir_stat = MetaCodec::DecodeDirStatValue(value);
  return Status::OK();
}

Status FlushDirStatsOperation::Run(TxnUPtr& txn) {
  // Clear result state at entry: RunAlone re-invokes Run on the same operation
  // object on commit-conflict retry, so without this a missing/negative ino
  // recorded by a failed attempt would survive into a successful retry and get
  // needlessly recomputed (regressing its version / clobbering its deltas).
  result_.missing_inos.clear();

  std::vector<std::string> keys;
  keys.reserve(delta_map_.size());
  for (const auto& [ino, _] : delta_map_) {
    keys.push_back(MetaCodec::EncodeDirStatKey(fs_id_, ino));
  }

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(keys, kvs);
  if (!status.ok()) return status;

  std::set<uint64_t> found;
  for (auto& kv : kvs) {
    uint32_t fs_id;
    uint64_t ino;
    MetaCodec::DecodeDirStatKey(kv.key, fs_id, ino);
    found.insert(ino);

    auto stat = MetaCodec::DecodeDirStatValue(kv.value);
    const auto& d = delta_map_.at(ino);
    int64_t length = stat.length() + d.length;
    int64_t inodes = stat.inodes() + d.inodes;
    int64_t dirs = stat.dirs() + d.dirs;

    if (length < 0 || inodes < 0 || dirs < 0) {
      result_.missing_inos.push_back(ino);
      continue;
    }

    stat.set_length(length);
    stat.set_inodes(inodes);
    stat.set_dirs(dirs);
    txn->Put(kv.key, MetaCodec::EncodeDirStatValue(stat));
  }

  for (const auto& [ino, _] : delta_map_) {
    if (found.find(ino) == found.end()) {
      result_.missing_inos.push_back(ino);
    }
  }

  return Status::OK();
}

Status SeedDirStatOperation::Run(TxnUPtr& txn) {
  result_.seeded = false;

  std::string key = MetaCodec::EncodeDirStatKey(fs_id_, ino_);
  std::string value;
  auto status = txn->Get(key, value);
  if (status.ok()) {
    // Record already exists (created by a concurrent flush/seed). Do not clobber.
    return Status::OK();
  }
  if (status.error_code() != pb::error::ENOT_FOUND) return status;

  txn->Put(key, MetaCodec::EncodeDirStatValue(dir_stat_));
  result_.seeded = true;
  return Status::OK();
}

Status RepairDirStatOperation::Run(TxnUPtr& txn) {
  result_ = Result{};

  std::string key = MetaCodec::EncodeDirStatKey(fs_id_, ino_);
  std::string value;
  auto status = txn->Get(key, value);
  if (status.ok()) {
    result_.found = true;
    result_.stored = MetaCodec::DecodeDirStatValue(value);
  } else if (status.error_code() != pb::error::ENOT_FOUND) {
    return status;
  }

  // Same mismatch criteria as the pre-existing SyncDirStat check (dirs excluded).
  result_.mismatch = !result_.found || result_.stored.length() != calc_.length() ||
                     result_.stored.inodes() != calc_.inodes();

  if (repair_ && result_.mismatch) {
    txn->Put(key, MetaCodec::EncodeDirStatValue(calc_));
    result_.wrote = true;
  }

  return Status::OK();
}

Status UpsertMdsOperation::Run(TxnUPtr& txn) {
  CHECK(mds_meta_.id() > 0) << "mds id is 0";

  txn->Put(MetaCodec::EncodeHeartbeatKey(mds_meta_.id()), MetaCodec::EncodeHeartbeatValue(mds_meta_));

  return Status::OK();
}

Status DeleteMdsOperation::Run(TxnUPtr& txn) {
  txn->Delete(MetaCodec::EncodeHeartbeatKey(mds_id_));

  return Status::OK();
}

Status ScanMdsOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetHeartbeatMdsRange();

  result_.mds_entries.clear();
  return txn->Scan(range, [&](const std::string& key, const std::string& value) -> bool {
    if (!MetaCodec::IsMdsHeartbeatKey(key)) return true;

    MdsEntry mds = MetaCodec::DecodeHeartbeatMdsValue(value);
    result_.mds_entries.push_back(mds);
    return true;
  });
}

Status UpsertClientOperation::Run(TxnUPtr& txn) {
  txn->Put(MetaCodec::EncodeHeartbeatKey(client_.id()), MetaCodec::EncodeHeartbeatValue(client_));

  return Status::OK();
}

Status DeleteClientOperation::Run(TxnUPtr& txn) {
  txn->Delete(MetaCodec::EncodeHeartbeatKey(client_id_));

  return Status::OK();
}

Status ScanClientOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetHeartbeatClientRange();

  result_.client_entries.clear();
  return txn->Scan(range, [&](const std::string& key, const std::string& value) -> bool {
    if (!MetaCodec::IsClientHeartbeatKey(key)) return true;

    ClientEntry client = MetaCodec::DecodeHeartbeatClientValue(value);
    result_.client_entries.push_back(client);
    return true;
  });
}

Status UpsertCacheMemberOperation::Run(TxnUPtr& txn) {
  std::string value;
  CacheMemberEntry cache_member;
  auto status = txn->Get(MetaCodec::EncodeHeartbeatCacheMemberKey(cache_member_id_), value);

  if (status.ok()) {
    cache_member = MetaCodec::DecodeHeartbeatCacheMemberValue(value);
  }

  status = handler_(cache_member, status);
  if (!status.ok()) return status;

  cache_member.set_version(cache_member.version() + 1);
  result_.cache_member = cache_member;
  txn->Put(MetaCodec::EncodeHeartbeatCacheMemberKey(cache_member_id_), MetaCodec::EncodeHeartbeatValue(cache_member));

  return Status::OK();
}

Status DeleteCacheMemberOperation::Run(TxnUPtr& txn) {
  txn->Delete(MetaCodec::EncodeHeartbeatCacheMemberKey(cache_member_id_));

  return Status::OK();
}

Status ScanCacheMemberOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetHeartbeatCacheMemberRange();

  result_.cache_member_entries.clear();
  return txn->Scan(range, [&](const std::string& key, const std::string& value) -> bool {
    if (!MetaCodec::IsCacheMemberHeartbeatKey(key)) return true;

    CacheMemberEntry cache_member = MetaCodec::DecodeHeartbeatCacheMemberValue(value);
    result_.cache_member_entries.push_back(cache_member);
    return true;
  });
}

Status GetCacheMemberOperation::Run(TxnUPtr& txn) {
  std::string value;
  auto status = txn->Get(MetaCodec::EncodeHeartbeatCacheMemberKey(cache_member_id_), value);
  if (!status.ok()) return status;

  result_.cache_member = MetaCodec::DecodeHeartbeatCacheMemberValue(value);

  return Status::OK();
}

Status GetFileSessionOperation::Run(TxnUPtr& txn) {
  std::string value;
  auto status = txn->Get(MetaCodec::EncodeFileSessionKey(fs_id_, ino_, session_id_), value);
  if (!status.ok()) {
    return status;
  }

  result_.file_session = MetaCodec::DecodeFileSessionValue(value);

  return Status::OK();
};

Status ScanFileSessionOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ > 0) << "fs_id is 0";

  Range range;
  if (ino_ == 0) {
    range = MetaCodec::GetFileSessionRange(fs_id_);
  } else {
    CHECK(ino_ > 0) << "ino is 0";
    range = MetaCodec::GetFileSessionRange(fs_id_, ino_);
  }

  return txn->Scan(range, [&](const std::string&, const std::string& value) -> bool {
    return handler_(MetaCodec::DecodeFileSessionValue(value));
  });
}

Status DeleteFileSessionOperation::Run(TxnUPtr& txn) {
  for (const auto& file_session : file_sessions_) {
    txn->Delete(MetaCodec::EncodeFileSessionKey(file_session.fs_id(), file_session.ino(), file_session.session_id()));
  }

  return Status::OK();
}

Status KeepAliveFileSessionOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ != 0) << "fs_id is 0";

  std::vector<std::string> keys;
  keys.reserve(param_.file_sessions.size());
  for (const auto& file_session : param_.file_sessions) {
    for (const auto& session_id : file_session.session_ids) {
      keys.push_back(MetaCodec::EncodeFileSessionKey(fs_id_, file_session.ino, session_id));
    }
  }

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(keys, kvs);
  if (!status.ok()) return status;

  for (auto& kv : kvs) {
    FileSessionEntry file_session = MetaCodec::DecodeFileSessionValue(kv.value);
    file_session.set_expire_time_s(utils::Timestamp() + FLAGS_mds_filesession_live_time_s);
    txn->Put(kv.key, MetaCodec::EncodeFileSessionValue(file_session));
  }

  return Status::OK();
}

Status CleanDelSliceOperation::Run(TxnUPtr& txn) {
  txn->Delete(key_);
  return Status::OK();
}

Status GetDelFileOperation::Run(TxnUPtr& txn) {
  std::string value;
  auto status = txn->Get(MetaCodec::EncodeDelFileKey(fs_id_, ino_), value);
  if (!status.ok()) return status;

  if (!value.empty()) {
    result_.attr = MetaCodec::DecodeDelFileValue(value);
  }

  return Status::OK();
}

Status CleanDelFileOperation::Run(TxnUPtr& txn) {
  txn->Delete(MetaCodec::EncodeDelFileKey(fs_id_, ino_));
  txn->Delete(MetaCodec::EncodeInodeKey(fs_id_, ino_));
  if (maybe_tiny_file_) txn->Delete(MetaCodec::EncodeTinyFileDataKey(fs_id_, ino_));

  return Status::OK();
}

Status ScanLockOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetLockRange();

  result_.kvs.clear();
  return txn->Scan(range, [&](const std::string& key, const std::string& value) -> bool {
    CHECK(MetaCodec::IsLockKey(key)) << fmt::format("invalid lock key({}).", key);

    result_.kvs.push_back(KeyValue{.key = key, .value = value});

    return true;
  });
}

Status ScanFsOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetFsRange();

  result_.fs_infoes.clear();
  return txn->Scan(range, [&](const std::string& key, const std::string& value) -> bool {
    CHECK(MetaCodec::IsFsKey(key)) << fmt::format("invalid fs key({}).", key);

    result_.fs_infoes.push_back(MetaCodec::DecodeFsValue(value));
    return true;
  });
}

Status ScanDentryOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetDentryRange(fs_id_, ino_, false);
  if (!last_name_.empty()) {
    range.start = MetaCodec::EncodeDentryKey(fs_id_, ino_, last_name_);
  }

  return txn->Scan(range, [&](const std::string&, const std::string& value) -> bool {
    return handler_(MetaCodec::DecodeDentryValue(value));
  });
}

Status ScanTrashDentryOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetDentryRange(fs_id_, ino_, false);
  if (!last_name_.empty()) {
    range.start = MetaCodec::EncodeDentryKey(fs_id_, ino_, last_name_);
  }

  return txn->Scan(range, [&](const std::string&, const std::string& value) -> bool {
    return handler_(MetaCodec::DecodeDentryValue(value));
  });
}

Status ScanDirShardOperation::Run(TxnUPtr& txn) {
  const Range complete_range = MetaCodec::GetDentryRange(fs_id_, ino_, false);

  Range range;
  if (!range_.start.empty()) {
    // fetch parent inode
    std::vector<std::string> keys;
    keys.reserve(kDirAttrMutationNum + 1);
    keys.push_back(MetaCodec::EncodeInodeKey(fs_id_, ino_));
    for (uint32_t i = 0; i < kDirAttrMutationNum; ++i) {
      keys.push_back(MetaCodec::EncodeDirInodeMutationKey(fs_id_, ino_, i));
    }
    std::vector<KeyValue> kvs;
    auto status = txn->BatchGet(keys, kvs);
    if (!status.ok()) return status;

    for (const auto& kv : kvs) {
      if (MetaCodec::IsInodeKey(kv.key)) {
        result_.attr_with_mutation.attr = MetaCodec::DecodeInodeValue(kv.value);
      } else if (MetaCodec::IsDirInodeMutationKey(kv.key)) {
        result_.attr_with_mutation.mutations.push_back(MetaCodec::DecodeDirInodeMutationValue(kv.value));
      } else {
        CHECK(false) << fmt::format("invalid key({}) in scan dir shard operation.", kv.key);
      }
    }

    range.start = MetaCodec::EncodeDentryKey(fs_id_, ino_, range_.start);
    range.end = range_.end.empty() ? complete_range.end : MetaCodec::EncodeDentryKey(fs_id_, ino_, range_.end);

  } else {
    range.start = MetaCodec::EncodeInodeKey(fs_id_, ino_);
    range.end = range_.end.empty() ? complete_range.end : MetaCodec::EncodeDentryKey(fs_id_, ino_, range_.end);
  }

  return txn->Scan(range, [&](const std::string& key, const std::string& value) -> bool {
    if (MetaCodec::IsInodeKey(key)) {
      result_.attr_with_mutation.attr = MetaCodec::DecodeInodeValue(value);
      return true;
    } else if (MetaCodec::IsDirInodeMutationKey(key)) {
      result_.attr_with_mutation.mutations.push_back(MetaCodec::DecodeDirInodeMutationValue(value));
      return true;
    }
    return handler_(MetaCodec::DecodeDentryValue(value));
  });
}

Status ScanDelSliceOperation::Run(TxnUPtr& txn) {
  Range range;
  if (ino_ == 0) {
    CHECK(fs_id_ > 0) << "fs_id is 0";
    range = MetaCodec::GetDelSliceRange(fs_id_);

  } else if (chunk_index_ == 0) {
    CHECK(fs_id_ > 0) << "fs_id is 0";
    CHECK(ino_ > 0) << "ino is 0";
    range = MetaCodec::GetDelSliceRange(fs_id_, ino_);

  } else {
    CHECK(fs_id_ > 0) << "fs_id is 0";
    CHECK(ino_ > 0) << "ino is 0";
    CHECK(chunk_index_ > 0) << "chunk_index is 0";
    range = MetaCodec::GetDelSliceRange(fs_id_, ino_, chunk_index_);
  }

  return txn->Scan(range, handler_);
}

Status ScanDelFileOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ > 0) << "fs_id is 0";
  Range range = MetaCodec::GetDelFileTableRange(fs_id_);

  return txn->Scan(range, scan_handler_);
}

Status ScanDirStatOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ > 0) << "fs_id is 0";
  Range range = MetaCodec::GetDirStatRange(fs_id_);

  return txn->Scan(range, scan_handler_);
}

Status ScanMetaTableOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetMetaTableRange();

  return txn->Scan(range, scan_handler_);
}

Status ScanFsMetaTableOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ > 0) << "fs_id is 0";

  Range range = MetaCodec::GetFsMetaTableRange(fs_id_);
  if (!start_key_.empty()) range.start = start_key_;

  return txn->Scan(range, scan_handler_);
}

Status ScanFsOpLogOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ > 0) << "fs_id is 0";

  Range range = MetaCodec::GetFsConfigLogRange(fs_id_);
  return txn->Scan(range, [&](const std::string&, const std::string& value) -> bool {
    return handler_(MetaCodec::DecodeFsOpLogValue(value));
  });
}

Status SaveFsStatsOperation::Run(TxnUPtr& txn) {
  txn->Put(MetaCodec::EncodeFsStatsKey(fs_id_, GetTime()), MetaCodec::EncodeFsStatsValue(fs_stats_));

  return Status::OK();
}

Status ScanFsStatsOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetFsStatsRange(fs_id_);
  range.start = MetaCodec::EncodeFsStatsKey(fs_id_, start_time_ns_);

  return txn->Scan(range, handler_);
}

static void SumFsStats(const FsStatsDataEntry& src_stats, FsStatsDataEntry& dst_stats) {
  dst_stats.set_read_bytes(dst_stats.read_bytes() + src_stats.read_bytes());
  dst_stats.set_read_qps(dst_stats.read_qps() + src_stats.read_qps());
  dst_stats.set_write_bytes(dst_stats.write_bytes() + src_stats.write_bytes());
  dst_stats.set_write_qps(dst_stats.write_qps() + src_stats.write_qps());
  dst_stats.set_s3_read_bytes(dst_stats.s3_read_bytes() + src_stats.s3_read_bytes());
  dst_stats.set_s3_read_qps(dst_stats.s3_read_qps() + src_stats.s3_read_qps());
  dst_stats.set_s3_write_bytes(dst_stats.s3_write_bytes() + src_stats.s3_write_bytes());
  dst_stats.set_s3_write_qps(dst_stats.s3_write_qps() + src_stats.s3_write_qps());
}

Status GetAndCompactFsStatsOperation::Run(TxnUPtr& txn) {
  Range range = MetaCodec::GetFsStatsRange(fs_id_);

  std::string mark_key = MetaCodec::EncodeFsStatsKey(fs_id_, mark_time_ns_);
  FsStatsDataEntry compact_stats;
  FsStatsDataEntry stats;
  bool compacted = false;
  auto status = txn->Scan(range, [&](const std::string& key, const std::string& value) -> bool {
    // compact old stats
    if (key <= mark_key) {
      txn->Delete(key);

    } else if (!compacted) {
      compact_stats = stats;
      compacted = true;
    }

    // sum all stats
    SumFsStats(MetaCodec::DecodeFsStatsValue(value), stats);

    return true;
  });

  if (!status.ok()) {
    return status;
  }

  // put compact stats
  if (compacted) {
    txn->Put(mark_key, MetaCodec::EncodeFsStatsValue(compact_stats));
  }

  result_.fs_stats = std::move(stats);

  return Status::OK();
}

Status GetInodeAttrOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ > 0) << "fs_id is 0";
  CHECK(ino_ > 0) << "ino is 0";

  if (IsDir(ino_)) {
    Range range;
    range.start = MetaCodec::EncodeInodeKey(fs_id_, ino_);
    range.end = MetaCodec::GetDirInodeUpdateOpRange(fs_id_, ino_).end;

    bool found = false;
    auto status = txn->Scan(range, [&](const std::string& key, const std::string& value) -> bool {
      if (MetaCodec::IsInodeKey(key)) {
        result_.attr_with_mutation.attr = MetaCodec::DecodeInodeValue(value);
        found = true;

      } else if (MetaCodec::IsDirInodeMutationKey(key)) {
        result_.attr_with_mutation.mutations.push_back(MetaCodec::DecodeDirInodeMutationValue(value));

      } else {
        LOG(FATAL) << fmt::format("invalid key({}).", Helper::StringToHex(key));
      }

      return true;
    });
    if (!status.ok()) return status;

    // The scan can come back without the base inode key (only mutation keys, or
    // nothing) when the directory inode was deleted while a client still holds a
    // stale reference and issues GetAttr. Surface that as ENOT_FOUND instead of
    // returning a default zero-ino attr, which UpsertInodeCache rejects with
    // LOG(FATAL) -- crashing the MDS on a GetAttr of any missing directory.
    if (!found) {
      return Status(pb::error::ENOT_FOUND, fmt::format("not found inode({})", ino_));
    }
    return Status::OK();

  } else {
    std::string value;
    auto status = txn->Get(MetaCodec::EncodeInodeKey(fs_id_, ino_), value);
    if (!status.ok()) return status;

    result_.attr_with_mutation.attr = MetaCodec::DecodeInodeValue(value);

    return Status::OK();
  }
}

Status BatchGetInodeAttrOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ > 0) << "fs_id is 0";
  CHECK(!inoes_.empty()) << "inoes_ is empty";

  std::vector<std::string> keys;
  keys.reserve(inoes_.size());
  for (const auto& ino : inoes_) {
    keys.push_back(MetaCodec::EncodeInodeKey(fs_id_, ino));

    if (HasDirAttrMutation() && IsDir(ino)) {
      for (uint32_t i = 0; i < kDirAttrMutationNum; ++i) {
        keys.push_back(MetaCodec::EncodeDirInodeMutationKey(fs_id_, ino, i));
      }
    }
  }

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(keys, kvs);
  if (!status.ok()) return status;

  std::map<Ino, AttrWithMutation> attr_with_mutation_map;
  for (const auto& kv : kvs) {
    if (MetaCodec::IsInodeKey(kv.key)) {
      auto attr = MetaCodec::DecodeInodeValue(kv.value);
      attr_with_mutation_map[attr.ino()].attr = attr;

    } else if (MetaCodec::IsDirInodeMutationKey(kv.key)) {
      uint32_t fs_id, index;
      uint64_t ino;
      MetaCodec::DecodeDirInodeMutationKey(kv.key, fs_id, ino, index);
      attr_with_mutation_map[ino].mutations.push_back(MetaCodec::DecodeDirInodeMutationValue(kv.value));

    } else {
      LOG(FATAL) << fmt::format("invalid key({}).", Helper::StringToHex(kv.key));
    }
  }

  result_.attr_with_mutations.clear();
  for (auto& [_, attr_with_mutation] : attr_with_mutation_map) {
    result_.attr_with_mutations.push_back(attr_with_mutation);
  }

  return Status::OK();
}

Status GetDentryOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ > 0) << "fs_id is 0";
  CHECK(parent_ > 0) << "parent is 0";
  CHECK(!name_.empty()) << "name is empty";

  std::string value;
  auto status = txn->Get(MetaCodec::EncodeDentryKey(fs_id_, parent_, name_), value);
  if (!status.ok()) return status;

  result_.dentry = MetaCodec::DecodeDentryValue(value);

  return Status::OK();
}

Status ImportKVOperation::Run(TxnUPtr& txn) {
  CHECK(!kvs_.empty()) << "kvs_ is empty";

  for (const auto& kv : kvs_) {
    CHECK(!kv.key.empty()) << "key is empty";
    CHECK(!kv.value.empty()) << "value is empty";

    txn->Put(kv.key, kv.value);
  }

  return Status::OK();
}

bool ConflictController::GetIndexAndIncRunningCount(uint32_t fs_id, Ino ino, uint32_t& index) {
  bool ret = false;
  running_map_.withWLock(
      [&](Map& map) mutable {
        auto it = map.find(Key{fs_id, ino});
        if (it == map.end()) {
          map.emplace(Key{fs_id, ino}, Value{.dir_mutation_index = 0, .running_count = 1});
          ret = true;
          index = 0;

        } else {
          auto& value = it->second;
          if (value.running_count == 0) {
            ret = true;
            index = 0;

          } else {
            index = ++value.dir_mutation_index;
          }

          ++value.running_count;
        }
      },
      ino);

  return ret;
}

bool ConflictController::IncRunningCount(uint32_t fs_id, Ino ino) {
  bool ret = false;
  running_map_.withWLock(
      [&](Map& map) mutable {
        auto it = map.find(Key{fs_id, ino});
        if (it == map.end()) {
          map.emplace(Key{fs_id, ino}, Value{.running_count = 1});
          ret = true;

        } else {
          auto& value = it->second;
          if (value.running_count == 0) ret = true;
          ++value.running_count;
        }
      },
      ino);

  return ret;
}

void ConflictController::DecRunningCount(uint32_t fs_id, Ino ino) {
  running_map_.withWLock(
      [&](Map& map) mutable {
        auto it = map.find(Key{fs_id, ino});
        CHECK(it != map.end()) << fmt::format("ino({}) not found in running map.", ino);
        CHECK(it->second.running_count > 0) << fmt::format("running count of ino({}) is already 0.", ino);

        --it->second.running_count;
      },
      ino);
}

OperationProcessor::OperationProcessor(KVStorageSPtr kv_storage) : kv_storage_(kv_storage) {
  async_worker_ = Worker::New();
  CHECK(async_worker_ != nullptr) << fmt::format("[operation] create async worker fail.");
}

bool OperationProcessor::Init() {
  thread_ = std::thread([this] {
    // set thread name for better debugging
    utils::SetThreadName("store_operate");

    // bind cpu
    utils::BindThreadToCpu(utils::GetCpuCount() - 1);

    ProcessOperation();
  });

  if (!async_worker_->Init()) {
    LOG(FATAL) << fmt::format("[operation] async worker init fail.");
    return false;
  }

  return true;
}

bool OperationProcessor::Destroy() {
  is_stop_.store(true);

  thread_cond_.notify_all();

  if (thread_.joinable()) thread_.join();

  async_worker_->Destroy();

  return true;
}

bool OperationProcessor::RunBatched(Operation* operation) {
  if (is_stop_.load(std::memory_order_relaxed)) {
    return false;
  }

  operations_.Enqueue(operation);

  thread_cond_.notify_one();

  return true;
}

Status OperationProcessor::RunAlone(Operation* operation) {
  utils::Duration duration;

  auto& trace = operation->GetTrace();
  const uint32_t fs_id = operation->GetFsId();
  const Ino ino = operation->GetIno();

  // for some operations like rmdir and rename, we need to increase the running count to avoid conflict with batch
  // operations.
  bool need_inc_running =
      (operation->GetOpType() == Operation::OpType::kRmDir) || (operation->GetOpType() == Operation::OpType::kRename);
  if (need_inc_running) conflict_controller_.IncRunningCount(fs_id, ino);

  ON_SCOPE_EXIT([&] {
    if (need_inc_running) conflict_controller_.DecRunningCount(fs_id, ino);
  });

  Status status;
  uint32_t retry = 0;
  int64_t txn_id = 0;
  char* commit_type = (char*)"none";
  do {
    utils::Duration once_duration;
    auto txn = kv_storage_->NewTxn(operation->GetIsolationLevel());
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }
    txn_id = txn->ID();

    status = operation->Run(txn);
    if (!status.ok()) {
      if (status.error_code() == pb::error::ESTORE_MAYBE_RETRY) {
        LOG(WARNING) << fmt::format("[operation.{}.{}][{}][{}us] alone run {} lock conflict, retry({}) status({}).",
                                    fs_id, ino, txn_id, once_duration.ElapsedUs(), operation->OpName(), retry,
                                    status.error_str());
        continue;
      }
      break;
    }

    status = txn->Commit();

    auto txn_trace = txn->GetTrace();
    commit_type = txn_trace.commit_type;
    trace.AddTxn(txn_trace);

    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    LOG(WARNING) << fmt::format("[operation.{}.{}][{}][{}us] alone run {} fail, txn({}) retry({}) status({}).", fs_id,
                                ino, txn_id, once_duration.ElapsedUs(), operation->OpName(), commit_type, retry,
                                status.error_str());

    if (once_duration.ElapsedMs() > FLAGS_mds_txn_timeout_ms) {
      break;
    }

  } while (IsRetry(retry));

  trace.RecordElapsedTime("store_operate");

  LOG(INFO) << fmt::format("[operation.{}.{}][{}][{}us] alone run {} finish, txn({}) retry({}) status({}).", fs_id, ino,
                           txn_id, duration.ElapsedUs(), operation->OpName(), commit_type, retry, status.error_str());

  if (!status.ok()) {
    operation->SetStatus(status);
  }

  return status;
}

void OperationTask::Run() {
  auto status = processor_->RunAlone(operation_.get());

  if (status.ok() && post_handler_) post_handler_(operation_);
}

bool OperationProcessor::AsyncRun(OperationSPtr operation, OperationTask::PostHandler post_handler) {
  bool ret = async_worker_->Execute(OperationTask::New(operation, GetSelfPtr(), post_handler));
  if (!ret) {
    LOG(ERROR) << fmt::format("[operation] async worker execute fail, operation({}).", operation->OpName());
  }

  return ret;
}

void OperationProcessor::Grouping(std::vector<Operation*>& operations, BatchOperationMap& batch_operation_map) {
  for (auto* operation : operations) {
    auto [it, inserted] = batch_operation_map.try_emplace(operation->GroupingKey());
    if (inserted) {
      it->second.fs_id = operation->GetFsId();
      it->second.ino = operation->GetIno();
    }

    auto& batch_operation = it->second;
    if (operation->IsCreateType()) {
      batch_operation.create_operations.push_back(operation);

    } else if (operation->IsSetAttrType()) {
      batch_operation.setattr_operations.push_back(operation);

    } else {
      LOG(FATAL) << "[operation] invalid operation type.";
    }
  }
}

void OperationProcessor::ProcessOperation() {
  std::vector<Operation*> stage_operations;
  stage_operations.reserve(FLAGS_mds_store_operation_batch_size);

  BatchOperationMap batch_operation_map;

  Operation* operation = nullptr;
  while (true) {
    operation = nullptr;
    stage_operations.clear();

    while (!operations_.Dequeue(operation) && !is_stop_.load(std::memory_order_relaxed)) {
      std::unique_lock<std::mutex> lk(thread_mutex_);
      thread_cond_.wait(lk);
      lk.unlock();
    }

    if (operation) {
      stage_operations.push_back(operation);
      operation = nullptr;
    }

    if (is_stop_.load(std::memory_order_relaxed) && stage_operations.empty()) {
      break;
    }

    bool waited = false;
    uint32_t try_count = 0;
    do {
      if (operation) {
        stage_operations.push_back(operation);
        operation = nullptr;

      } else {
        if ((!waited || FLAGS_mds_store_operation_wait_multi_time) && FLAGS_mds_store_operation_merge_delay_us > 0) {
          waited = true;
          std::this_thread::sleep_for(std::chrono::microseconds(FLAGS_mds_store_operation_merge_delay_us));
        }
      }

    } while (stage_operations.size() < FLAGS_mds_store_operation_batch_size &&
             (operations_.Dequeue(operation) || ++try_count < kTryMaxCount));

    // grouping operations
    Grouping(stage_operations, batch_operation_map);
    for (auto& [_, batch_operation] : batch_operation_map) {
      LaunchExecuteBatchOperation(std::move(batch_operation));
    }
    batch_operation_map.clear();
  }

  // print pending operations
  while (operations_.Dequeue(operation)) {
    LOG(INFO) << fmt::format("[operation] pending operation type({}) ino({}).", operation->OpName(),
                             operation->GetIno());
  }
}

void OperationProcessor::LaunchExecuteBatchOperation(BatchOperation&& batch_operation) {
  struct Params {
    OperationProcessor* self{nullptr};
    BatchOperation batch_operation;
  };

  Params* params = new Params({.self = this, .batch_operation = std::move(batch_operation)});

  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  if (bthread_start_background(
          &tid, &attr,
          [](void* arg) -> void* {
            Params* params = reinterpret_cast<Params*>(arg);

            params->self->ExecuteBatchOperation(params->batch_operation);

            delete params;

            return nullptr;
          },
          params) != 0) {
    delete params;
    LOG(FATAL) << "[operation] start background thread fail.";
  }
}

static std::string GetName(const BatchOperation& batch_operation) {
  std::string op_names;
  op_names.reserve(kOpNameBufInitSize);

  for (auto* operation : batch_operation.setattr_operations) {
    op_names += fmt::format("{},", operation->OpName());
  }
  for (auto* operation : batch_operation.create_operations) {
    op_names += fmt::format("{},", operation->OpName());
  }

  op_names.resize(op_names.size() > 0 ? op_names.size() - 1 : 0);

  return op_names;
}

static void SetBatchIndex(BatchOperation& batch_operation) {
  uint32_t index = 0;
  for (auto* operation : batch_operation.create_operations) {
    operation->SetBatchIndex(index++);
  }

  for (auto* operation : batch_operation.setattr_operations) {
    operation->SetBatchIndex(index++);
  }
}

static void GenPrefetchKey(BatchOperation& batch_operation, std::vector<std::string>& prefetch_keys) {
  for (auto* operation : batch_operation.create_operations) {
    operation->PrefetchKey(prefetch_keys);
  }

  for (auto* operation : batch_operation.setattr_operations) {
    operation->PrefetchKey(prefetch_keys);
  }
}

static bool IsNeedParentKey(const BatchOperation& batch_operation) {
  for (auto* operation : batch_operation.create_operations) {
    if (!operation->IsDirMutationOperation()) return true;
  }

  for (auto* operation : batch_operation.setattr_operations) {
    if (!operation->IsDirMutationOperation()) return true;
  }

  return false;
}

void OperationProcessor::ExecuteBatchOperation(BatchOperation& batch_operation) {
  const uint32_t fs_id = batch_operation.fs_id;
  const uint64_t ino = batch_operation.ino;
  size_t count = batch_operation.setattr_operations.size() + batch_operation.create_operations.size();

  utils::Duration duration;

  SetElapsedTime(batch_operation, "store_pending");

  if (count > 1) SetBatchIndex(batch_operation);

  uint32_t mutation_index = 0;
  bool need_parent_key = !IsDir(ino) || conflict_controller_.GetIndexAndIncRunningCount(fs_id, ino, mutation_index) ||
                         IsNeedParentKey(batch_operation);
  ON_SCOPE_EXIT([&] {
    if (IsDir(ino)) conflict_controller_.DecRunningCount(fs_id, ino);
  });

  mutation_index = mutation_index % kDirAttrMutationNum;

  // get prefetch keys
  std::vector<std::string> prefetch_keys;
  prefetch_keys.reserve(kStoreOperationBatchSize);
  std::string parent_key, parent_mutation_key;
  if (need_parent_key) {
    parent_key = MetaCodec::EncodeInodeKey(fs_id, ino);
    prefetch_keys.push_back(parent_key);

  } else {
    parent_mutation_key = MetaCodec::EncodeDirInodeMutationKey(fs_id, ino, mutation_index);
    prefetch_keys.push_back(parent_mutation_key);
  }
  GenPrefetchKey(batch_operation, prefetch_keys);

  Operation::BatchSharedParam shared_param;
  Status status;
  uint32_t retry = 0;
  int64_t txn_id = 0;
  char* commit_type = (char*)"none";
  std::string op_names = GetName(batch_operation);
  do {
    utils::Duration once_duration;

    if (retry > 0) {
      shared_param.attr.Clear();
      shared_param.attr_mutation.Clear();
      shared_param.prefetch_kvs.clear();
    }

    auto txn = kv_storage_->NewTxn();
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }
    txn_id = txn->ID();

    status = txn->BatchGet(prefetch_keys, shared_param.prefetch_kvs);
    if (!status.ok()) {
      if (status.error_code() == pb::error::ESTORE_MAYBE_RETRY) {
        LOG(WARNING) << fmt::format("[operation.{}.{}][{}][{}us] batch run {} lock conflict, retry({}) status({}).",
                                    fs_id, ino, txn_id, once_duration.ElapsedUs(), op_names, retry, status.error_str());
        continue;
      }
      break;
    }
    shared_param.RebuildIndex();

    if (need_parent_key) {
      std::string value = FindValue(shared_param.prefetch_index, parent_key);
      if (value.empty()) {
        status = Status(pb::error::ENOT_FOUND, fmt::format("not found inode({})", ino));
        break;
      }
      shared_param.attr = MetaCodec::DecodeInodeValue(value);

    } else {
      shared_param.attr_mutation.set_ino(ino);
      shared_param.attr_mutation.set_index(mutation_index);
      std::string value = FindValue(shared_param.prefetch_index, parent_mutation_key);
      if (!value.empty()) shared_param.attr_mutation = MetaCodec::DecodeDirInodeMutationValue(value);
    }

    CHECK(!need_parent_key || shared_param.attr.ino() != 0)
        << fmt::format("invalid attr ino({}).", shared_param.attr.ino());

    // run set attr operations
    for (auto* operation : batch_operation.setattr_operations) {
      Status s = operation->RunInBatch(txn, shared_param);
      if (!s.ok()) operation->SetStatus(s);
    }

    // run create operations
    for (auto* operation : batch_operation.create_operations) {
      Status s = operation->RunInBatch(txn, shared_param);
      if (!s.ok()) operation->SetStatus(s);
    }

    if (need_parent_key) {
      shared_param.attr.set_version(shared_param.attr.version() + 1);
      txn->Put(parent_key, MetaCodec::EncodeInodeValue(shared_param.attr));

    } else {
      shared_param.attr_mutation.set_delta_version(shared_param.attr_mutation.delta_version() + 1);
      txn->Put(parent_mutation_key, MetaCodec::EncodeDirInodeMutationValue(shared_param.attr_mutation));
    }

    status = txn->Commit();

    auto txn_trace = txn->GetTrace();
    commit_type = txn_trace.commit_type;
    SetTrace(batch_operation, txn_trace);

    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    // Reset per-op status before retrying so a failed RunInBatch in this
    // round (e.g., transient ENOT_FOUND from a stale prefetch) does not
    // poison the next attempt where the data may be present again.
    for (auto* operation : batch_operation.setattr_operations) operation->SetStatus(Status::OK());
    for (auto* operation : batch_operation.create_operations) operation->SetStatus(Status::OK());

    LOG(WARNING) << fmt::format(
        "[operation.{}.{}][{}][{}us] batch run ({}) fail, count({}) parent_key({},{}) txn({}) retry({}) status({}).",
        fs_id, ino, txn_id, once_duration.ElapsedUs(), op_names, count, need_parent_key, mutation_index, commit_type,
        retry, status.error_str());

    if (once_duration.ElapsedMs() > FLAGS_mds_txn_timeout_ms) {
      break;
    }

  } while (IsRetry(retry));

  SetElapsedTime(batch_operation, "store_operate");

  LOG(INFO) << fmt::format(
      "[operation.{}.{}][{}][{}us] batch run ({}) finish, count({}) parent_key({},{}) txn({}) retry({}) status({}) "
      "attr({}).",
      fs_id, ino, txn_id, duration.ElapsedUs(), op_names, count, need_parent_key, mutation_index, commit_type, retry,
      status.error_str(),
      need_parent_key ? DescribeAttr(shared_param.attr) : DescribeAttrMutation(shared_param.attr_mutation));

  if (status.ok()) {
    SetResultAttr(batch_operation, shared_param);

  } else {
    SetError(batch_operation, status);
  }

  // notify operation finish
  Notify(batch_operation);
}

Status OperationProcessor::CheckTable(const Range& range) {
  auto status = kv_storage_->IsExistTable(range.start, range.end);
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      LOG(ERROR) << "[fsset] check fs table exist fail, error: " << status.error_str();
    }
  }

  return status;
}

Status OperationProcessor::CreateTable(const std::string& table_name, const Range& range, int64_t& table_id) {
  KVStorage::TableOption option = {.start_key = range.start, .end_key = range.end};
  Status status = kv_storage_->CreateTable(table_name, option, table_id);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, fmt::format("create table({}) fail, {}", table_name, status.error_str()));
  }

  return Status::OK();
}

}  // namespace mds
}  // namespace dingofs