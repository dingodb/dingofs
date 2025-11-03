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
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "brpc/reloadable_flags.h"
#include "bthread/bthread.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"
#include "mds/common/codec.h"
#include "mds/common/constant.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/common/status.h"
#include "mds/common/time.h"
#include "mds/common/type.h"
#include "mds/storage/storage.h"
#include "utils/uuid.h"

namespace dingofs {
namespace mds {

DEFINE_uint32(mds_store_operation_batch_size, 64, "process operation batch size.");
DEFINE_validator(mds_store_operation_batch_size, brpc::PassValidate);

DEFINE_uint32(mds_txn_max_retry_times, 5, "txn max retry times.");
DEFINE_validator(mds_txn_max_retry_times, brpc::PassValidate);

DEFINE_uint32(mds_store_operation_merge_delay_us, 10, "merge operation delay us.");
DEFINE_validator(mds_store_operation_merge_delay_us, brpc::PassValidate);

DECLARE_uint32(mds_compact_chunk_interval_ms);

DEFINE_bool(mds_compact_chunk_detail_log_enable, true, "compact chunk detal log enable.");
DEFINE_validator(mds_compact_chunk_detail_log_enable, brpc::PassValidate);

static const uint32_t kOpNameBufInitSize = 128;

static uint32_t CalWaitTimeUs(int retry) {
  // exponential backoff
  return Helper::GenerateRealRandomInteger(1000, 5000) * (1 << retry);
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
  auto it = std::find(attr.parents().begin(), attr.parents().end(), parent);
  if (it == attr.parents().end()) {
    attr.add_parents(parent);
  }
}

static void DelParentIno(AttrEntry& attr, Ino parent) {
  auto it = std::find(attr.parents().begin(), attr.parents().end(), parent);
  if (it != attr.parents().end()) {
    attr.mutable_parents()->erase(it);
  }
}

static void SetError(BatchOperation& batch_operation, Status& status) {
  for (auto* operation : batch_operation.setattr_operations) {
    operation->SetStatus(status);
  }

  for (auto* operation : batch_operation.create_operations) {
    operation->SetStatus(status);
  }
}

static void SetAttr(BatchOperation& batch_operation, AttrEntry& attr) {
  for (auto* operation : batch_operation.setattr_operations) {
    operation->SetAttr(attr);
  }

  for (auto* operation : batch_operation.create_operations) {
    operation->SetAttr(attr);
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

    case OpType::kBatchCreateFile:
      return "CreateFile";

    case OpType::kHardLink:
      return "HardLink";

    case OpType::kSmyLink:
      return "SmyLink";

    case OpType::kUpdateAttr:
      return "UpdateAttr";

    case OpType::kUpdateXAttr:
      return "UpdateXAttr";

    case OpType::kRemoveXAttr:
      return "RemoveXAttr";

    case OpType::kFallocate:
      return "Fallocate";

    case OpType::kOpenFile:
      return "OpenFile";

    case OpType::kCloseFile:
      return "CloseFile";

    case OpType::kRmDir:
      return "RmDir";

    case OpType::kUnlink:
      return "Unlink";

    case OpType::kRename:
      return "Rename";

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

    case OpType::kScanDelFile:
      return "ScanDelFile";

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

    case OpType::KGetDentry:
      return "GetDentry";

    case OpType::kImportKV:
      return "ImportKV";

    case OpType::kUpsertCacheMember:
      return "UpsertCacheMember";

    case OpType::kDeleteCacheMember:
      return "DeleteCacheMember";

    case OpType::kScanCacheMember:
      return "ScanCacheMember";

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
  if (!status.ok()) {
    return status;
  }

  result_.fs_info = MetaCodec::DecodeFsValue(value);

  return Status::OK();
}
// message Client {
//   string id = 1;
//   string hostname = 2;
//   uint32 port = 3;
//   string mountpoint = 4;
//   string fs_name = 5;
//   string ip = 6;

//   uint64 last_online_time_ms = 10;
// }

Status MountFsOperation::Run(TxnUPtr& txn) {
  std::string value;
  std::string key = MetaCodec::EncodeFsKey(fs_name_);
  Status status = txn->Get(key, value);
  if (!status.ok()) {
    return status;
  }

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
  client.set_port(mount_point_.port());
  client.set_mountpoint(mount_point_.path());
  client.set_fs_name(fs_info.fs_name());
  client.set_last_online_time_ms(Helper::TimestampMs());

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
  if (!status.ok()) {
    return status;
  }

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
  std::string fs_key = MetaCodec::EncodeFsKey(fs_name_);
  auto status = txn->Get(fs_key, value);
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

  fs_info.set_status(pb::mds::FsStatus::DELETED);
  fs_info.set_is_deleted(true);
  fs_info.set_delete_time_s(Helper::Timestamp());

  txn->Put(fs_key, MetaCodec::EncodeFsValue(fs_info));

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
  if (!status.ok()) {
    return status;
  }

  auto new_fs_info = MetaCodec::DecodeFsValue(value);
  if (fs_info_.capacity() > 0) new_fs_info.set_capacity(fs_info_.capacity());
  if (fs_info_.block_size() > 0) new_fs_info.set_block_size(fs_info_.block_size());
  if (!fs_info_.owner().empty()) new_fs_info.set_owner(fs_info_.owner());
  if (fs_info_.recycle_time_hour() > 0) new_fs_info.set_recycle_time_hour(fs_info_.recycle_time_hour());

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
  if (!status.ok()) {
    return status;
  }

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
  if (!status.ok()) {
    return status;
  }

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
  if (!status.ok()) {
    return status;
  }

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

Status MkDirOperation::RunInBatch(TxnUPtr& txn, AttrEntry& parent_attr, const std::vector<KeyValue>&) {
  const uint32_t fs_id = parent_attr.fs_id();
  const Ino parent = parent_attr.ino();

  // create dentry
  txn->Put(MetaCodec::EncodeDentryKey(fs_id, parent, dentry_.Name()), MetaCodec::EncodeDentryValue(dentry_.Copy()));

  // create inode
  txn->Put(MetaCodec::EncodeInodeKey(fs_id, dentry_.INo()), MetaCodec::EncodeInodeValue(attr_));

  // update parent attr
  parent_attr.set_nlink(parent_attr.nlink() + 1);
  parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
  parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));

  return Status::OK();
}

Status MkNodOperation::RunInBatch(TxnUPtr& txn, AttrEntry& parent_attr, const std::vector<KeyValue>&) {
  const uint32_t fs_id = parent_attr.fs_id();
  const Ino parent = parent_attr.ino();

  // create dentry
  txn->Put(MetaCodec::EncodeDentryKey(fs_id, parent, dentry_.Name()), MetaCodec::EncodeDentryValue(dentry_.Copy()));

  // create inode
  txn->Put(MetaCodec::EncodeInodeKey(fs_id, dentry_.INo()), MetaCodec::EncodeInodeValue(attr_));

  // update parent attr
  parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
  parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));

  return Status::OK();
}

Status BatchCreateFileOperation::RunInBatch(TxnUPtr& txn, AttrEntry& parent_attr, const std::vector<KeyValue>&) {
  const uint32_t fs_id = parent_attr.fs_id();
  const Ino parent = parent_attr.ino();
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
  parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
  parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));

  return Status::OK();
}

Status HardLinkOperation::Run(TxnUPtr& txn) {
  const uint32_t fs_id = dentry_.FsId();
  const Ino parent = dentry_.ParentIno();

  // get parent/child attr
  std::string parent_key = MetaCodec::EncodeInodeKey(fs_id, parent);
  std::string key = MetaCodec::EncodeInodeKey(fs_id, dentry_.INo());

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet({parent_key, key}, kvs);
  if (!status.ok()) {
    return status;
  }
  if (kvs.size() != 2) {
    return Status(pb::error::ENOT_FOUND, fmt::format("get parent/child inode fail, count({})", kvs.size()));
  }

  AttrEntry parent_attr, attr;
  for (auto& kv : kvs) {
    if (kv.key == parent_key) {
      parent_attr = MetaCodec::DecodeInodeValue(kv.value);
    } else if (kv.key == key) {
      attr = MetaCodec::DecodeInodeValue(kv.value);
    } else {
      DINGO_LOG(FATAL) << fmt::format("[operation.{}.{}] invalid key({}), parent_key({}), child_key({}).", fs_id,
                                      dentry_.INo(), Helper::StringToHex(kv.key), Helper::StringToHex(parent_key),
                                      Helper::StringToHex(key));
    }
  }

  // update parent attr
  parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
  parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));
  parent_attr.set_version(parent_attr.version() + 1);

  // update inode nlink
  attr.set_nlink(attr.nlink() + 1);
  attr.set_mtime(std::max(attr.mtime(), GetTime()));
  attr.set_ctime(std::max(attr.ctime(), GetTime()));
  AddParentIno(attr, parent);
  attr.set_version(attr.version() + 1);
  txn->Put(key, MetaCodec::EncodeInodeValue(attr));

  // create dentry
  txn->Put(MetaCodec::EncodeDentryKey(fs_id, parent, dentry_.Name()), MetaCodec::EncodeDentryValue(dentry_.Copy()));

  SetAttr(parent_attr);
  result_.child_attr = attr;

  return status;
}

Status SmyLinkOperation::RunInBatch(TxnUPtr& txn, AttrEntry& parent_attr, const std::vector<KeyValue>&) {
  const uint32_t fs_id = parent_attr.fs_id();
  const Ino parent = parent_attr.ino();

  // create dentry
  txn->Put(MetaCodec::EncodeDentryKey(fs_id, parent, dentry_.Name()), MetaCodec::EncodeDentryValue(dentry_.Copy()));

  // create inode
  txn->Put(MetaCodec::EncodeInodeKey(fs_id, dentry_.INo()), MetaCodec::EncodeInodeValue(attr_));

  // update parent attr
  parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
  parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));

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

void UpdateAttrOperation::ExpandChunk(TxnUPtr& txn, AttrEntry& attr, uint64_t new_length) const {
  if (new_length <= attr.length()) return;

  const uint64_t chunk_size = extra_param_.chunk_size;
  const uint64_t block_size = extra_param_.block_size;

  uint64_t start_index =
      attr.length() % chunk_size == 0 ? attr.length() / chunk_size : (attr.length() / chunk_size) + 1;
  uint64_t end_index = new_length % chunk_size == 0 ? new_length / chunk_size : (new_length / chunk_size) + 1;

  for (; start_index < end_index; ++start_index) {
    ChunkEntry chunk;
    chunk.set_index(start_index);
    chunk.set_chunk_size(chunk_size);
    chunk.set_block_size(block_size);
    chunk.set_version(0);

    txn->Put(MetaCodec::EncodeChunkKey(attr.fs_id(), attr.ino(), start_index), MetaCodec::EncodeChunkValue(chunk));
  }
}

Status UpdateAttrOperation::RunInBatch(TxnUPtr&, AttrEntry& attr, const std::vector<KeyValue>&) {
  if (to_set_ & kSetAttrMode) {
    attr.set_mode(attr_.mode());
  }

  if (to_set_ & kSetAttrUid) {
    attr.set_uid(attr_.uid());
  }

  if (to_set_ & kSetAttrGid) {
    attr.set_gid(attr_.gid());
  }

  if (to_set_ & kSetAttrLength) {
    // ExpandChunk(txn, attr, attr_.length());
    attr.set_length(attr_.length());
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

Status UpdateXAttrOperation::RunInBatch(TxnUPtr&, AttrEntry& attr, const std::vector<KeyValue>&) {
  for (const auto& [key, value] : xattrs_) {
    (*attr.mutable_xattrs())[key] = value;
  }

  // update attr
  attr.set_atime(std::max(attr.atime(), GetTime()));
  attr.set_mtime(std::max(attr.mtime(), GetTime()));
  attr.set_ctime(std::max(attr.ctime(), GetTime()));

  return Status::OK();
}

Status RemoveXAttrOperation::RunInBatch(TxnUPtr&, AttrEntry& attr, const std::vector<KeyValue>&) {
  attr.mutable_xattrs()->erase(name_);

  // update attr
  attr.set_atime(std::max(attr.atime(), GetTime()));
  attr.set_mtime(std::max(attr.mtime(), GetTime()));
  attr.set_ctime(std::max(attr.ctime(), GetTime()));

  return Status::OK();
}

std::vector<std::string> UpsertChunkOperation::PrefetchKey() {
  std::vector<std::string> prefetch_keys;
  prefetch_keys.reserve(delta_slices_.size());
  for (const auto& delta_slices : delta_slices_) {
    prefetch_keys.push_back(MetaCodec::EncodeChunkKey(fs_info_.fs_id(), ino_, delta_slices.chunk_index()));
  }

  return prefetch_keys;
}

Status UpsertChunkOperation::RunInBatch(TxnUPtr& txn, AttrEntry& attr, const std::vector<KeyValue>& prefetch_kvs) {
  result_.effected_chunks.clear();

  uint64_t prev_length = attr.length();
  for (const auto& delta_slices : delta_slices_) {
    ChunkEntry chunk;
    const auto& chunk_index = delta_slices.chunk_index();

    const std::string key = MetaCodec::EncodeChunkKey(fs_info_.fs_id(), ino_, chunk_index);
    auto value = FindValue(prefetch_kvs, key);
    if (!value.empty()) chunk = MetaCodec::DecodeChunkValue(value);

    // not exist chunk, create a new one
    if (chunk.version() == 0) {
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
        return false;
      };

      for (const auto& slice : delta_slices.slices()) {
        if (!is_exist_fn(slice)) *chunk.add_slices() = slice;
      }
    }

    chunk.set_version(chunk.version() + 1);
    txn->Put(key, MetaCodec::EncodeChunkValue(chunk));
    result_.effected_chunks.push_back(std::move(chunk));

    // update length
    for (const auto& slice : delta_slices.slices()) {
      if (attr.length() < (slice.offset() + slice.len())) {
        attr.set_length(slice.offset() + slice.len());
      }
    }
  }

  result_.length_delta = attr.length() - prev_length;

  // update attr
  attr.set_ctime(std::max(attr.ctime(), GetTime()));
  attr.set_mtime(std::max(attr.mtime(), GetTime()));

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

Status FallocateOperation::PreAlloc(TxnUPtr& txn, AttrEntry& attr, uint64_t offset, uint32_t len) {
  uint64_t length = attr.length();
  const uint64_t new_length = offset + len;

  if (length >= new_length) return Status::OK();

  const uint32_t fs_id = attr.fs_id();
  const Ino ino = attr.ino();
  const uint64_t chunk_size = param_.chunk_size;
  const uint64_t block_size = param_.block_size;
  uint64_t slice_id = param_.slice_id;
  const uint32_t slice_num = param_.slice_num;

  ChunkEntry max_chunk;
  auto status = GetChunk(txn, fs_id, ino, length / chunk_size, max_chunk);
  if (!status.ok()) return status;

  std::vector<ChunkEntry> effected_chunks;
  uint32_t count = 0;
  while (length < new_length) {
    uint64_t chunk_pos = length % chunk_size;
    uint64_t chunk_index = length / chunk_size;
    uint64_t delta_size = new_length - length;
    uint64_t delta_chunk_size = (chunk_pos + delta_size > chunk_size) ? (chunk_size - chunk_pos) : delta_size;

    SliceEntry slice;
    slice.set_id(slice_id++);
    slice.set_offset(chunk_pos);
    slice.set_len(delta_chunk_size);
    slice.set_size(delta_chunk_size);
    slice.set_zero(true);

    CHECK(chunk_index >= max_chunk.index()) << fmt::format(
        "chunk_index({}) should be greater than or equal to max_chunk.index({}).", chunk_index, max_chunk.index());

    if (chunk_index > max_chunk.index()) {
      ChunkEntry chunk;
      chunk.set_index(chunk_index);
      chunk.set_chunk_size(chunk_size);
      chunk.set_block_size(block_size);
      chunk.set_version(0);
      chunk.add_slices()->Swap(&slice);

      txn->Put(MetaCodec::EncodeChunkKey(fs_id, ino, chunk_index), MetaCodec::EncodeChunkValue(chunk));
      effected_chunks.push_back(std::move(chunk));

    } else {
      max_chunk.add_slices()->Swap(&slice);
      txn->Put(MetaCodec::EncodeChunkKey(fs_id, ino, chunk_index), MetaCodec::EncodeChunkValue(max_chunk));
      effected_chunks.push_back(max_chunk);
    }

    length += delta_chunk_size;
    ++count;
    if (count > slice_num) {
      return Status(pb::error::EINTERNAL, fmt::format("beyond slice num({})", slice_num));
    }
  }

  result_.effected_chunks = std::move(effected_chunks);

  return Status::OK();
}

// |---------file length--------|
// ------------------------------------------>
// 1. [offset, len)    |-----|
// 2. [offset, len)    |-------------|
// 3. [offset, len)                |-----|
Status FallocateOperation::SetZero(TxnUPtr& txn, AttrEntry& attr, uint64_t offset, uint64_t len, bool keep_size) {
  const uint32_t fs_id = attr.fs_id();
  const Ino ino = attr.ino();
  const uint64_t chunk_size = param_.chunk_size;
  const uint64_t block_size = param_.block_size;
  const uint32_t slice_num = param_.slice_num;
  uint64_t slice_id = param_.slice_id;

  uint64_t end_offset = keep_size ? std::min(attr.length(), offset + len) : (offset + len);

  // scan chunks
  std::map<uint64_t, ChunkEntry> chunks;
  auto status = ScanChunk(txn, fs_id, ino, chunks);
  if (!status.ok()) return status;

  std::vector<ChunkEntry> effected_chunks;
  uint32_t count = 0;
  while (offset < end_offset) {
    uint64_t chunk_pos = offset % chunk_size;
    uint64_t chunk_index = offset / chunk_size;

    uint64_t delta_chunk_size = chunk_size - chunk_pos;

    SliceEntry slice;
    slice.set_id(slice_id++);
    slice.set_offset(chunk_pos);
    slice.set_len(delta_chunk_size);
    slice.set_size(delta_chunk_size);
    slice.set_zero(true);

    // todo
    auto it = chunks.find(chunk_index);
    if (it == chunks.end()) {
      ChunkEntry chunk;
      chunk.set_index(chunk_index);
      chunk.set_chunk_size(chunk_size);
      chunk.set_block_size(block_size);
      chunk.set_version(0);
      chunk.add_slices()->Swap(&slice);

      txn->Put(MetaCodec::EncodeChunkKey(fs_id, ino, chunk_index), MetaCodec::EncodeChunkValue(chunk));
      effected_chunks.push_back(std::move(chunk));

    } else {
      auto& chunk = it->second;
      chunk.add_slices()->Swap(&slice);
      txn->Put(MetaCodec::EncodeChunkKey(fs_id, ino, chunk_index), MetaCodec::EncodeChunkValue(chunk));
      effected_chunks.push_back(chunk);
    }

    offset += delta_chunk_size;
    ++count;
    if (count > slice_num) {
      return Status(pb::error::EINTERNAL, fmt::format("beyond slice num({})", slice_num));
    }
  }

  if (!keep_size && end_offset > attr.length()) {
    attr.set_length(end_offset);
  }

  result_.effected_chunks = std::move(effected_chunks);

  return Status::OK();
}

Status FallocateOperation::RunInBatch(TxnUPtr& txn, AttrEntry& attr, const std::vector<KeyValue>&) {
  const int32_t mode_ = param_.mode;
  const uint64_t offset = param_.offset;
  const uint64_t len = param_.len;

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

  return Status::OK();
}

Status OpenFileOperation::RunInBatch(TxnUPtr& txn, AttrEntry& attr, const std::vector<KeyValue>&) {
  if (flags_ & O_TRUNC) {
    result_.delta_bytes = -static_cast<int64_t>(attr.length());
    attr.set_length(0);

    attr.set_ctime(std::max(attr.ctime(), GetTime()));
    attr.set_mtime(std::max(attr.mtime(), GetTime()));
  }

  attr.set_atime(std::max(attr.atime(), GetTime()));

  // add file session
  txn->Put(MetaCodec::EncodeFileSessionKey(file_session_.fs_id(), file_session_.ino(), file_session_.session_id()),
           MetaCodec::EncodeFileSessionValue(file_session_));

  return Status::OK();
}

Status CloseFileOperation::Run(TxnUPtr& txn) {
  txn->Delete(MetaCodec::EncodeFileSessionKey(fs_id_, ino_, session_id_));
  return Status::OK();
}

static Status CheckDirEmpty(TxnUPtr& txn, uint32_t fs_id, uint64_t ino, bool& is_empty) {
  Range range = MetaCodec::GetDentryRange(fs_id, ino, true);

  std::vector<KeyValue> kvs;
  auto status = txn->Scan(range, 2, kvs);
  if (!status.ok()) {
    return status;
  }

  is_empty = (kvs.size() < 2);

  return Status::OK();
}

Status RmDirOperation::Run(TxnUPtr& txn) {
  const uint32_t fs_id = dentry_.FsId();
  const Ino parent = dentry_.ParentIno();

  // check dentry empty
  bool is_empty = false;
  auto status = CheckDirEmpty(txn, fs_id, dentry_.INo(), is_empty);
  if (!status.ok()) {
    return status;
  }

  if (!is_empty) {
    return Status(pb::error::ENOT_EMPTY, fmt::format("directory({}) is not empty.", dentry_.INo()));
  }

  // update parent attr
  std::string value;
  std::string parent_key = MetaCodec::EncodeInodeKey(fs_id, parent);
  status = txn->Get(parent_key, value);
  if (!status.ok()) {
    return status;
  }

  auto parent_attr = MetaCodec::DecodeInodeValue(value);
  parent_attr.set_nlink(parent_attr.nlink() - 1);
  parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));
  parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
  parent_attr.set_version(parent_attr.version() + 1);

  txn->Put(parent_key, MetaCodec::EncodeInodeValue(parent_attr));

  // delete inode
  txn->Delete(MetaCodec::EncodeInodeKey(fs_id, dentry_.INo()));

  // delete dentry
  txn->Delete(MetaCodec::EncodeDentryKey(fs_id, parent, dentry_.Name()));

  SetAttr(parent_attr);

  return Status::OK();
}

Status UnlinkOperation::Run(TxnUPtr& txn) {
  const uint32_t fs_id = dentry_.FsId();
  const Ino parent = dentry_.ParentIno();

  // get parent/child attr
  std::string parent_key = MetaCodec::EncodeInodeKey(fs_id, parent);
  std::string key = MetaCodec::EncodeInodeKey(fs_id, dentry_.INo());

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet({parent_key, key}, kvs);
  if (!status.ok()) {
    return status;
  }
  if (kvs.size() != 2) {
    return Status(pb::error::ENOT_FOUND, fmt::format("get parent/child inode fail, count({})", kvs.size()));
  }

  AttrEntry parent_attr, attr;
  for (auto& kv : kvs) {
    if (kv.key == parent_key) {
      parent_attr = MetaCodec::DecodeInodeValue(kv.value);

    } else if (kv.key == key) {
      attr = MetaCodec::DecodeInodeValue(kv.value);

    } else {
      DINGO_LOG(FATAL) << fmt::format("[operation.{}.{}] invalid key({}), parent_key({}), child_key({}).", fs_id,
                                      dentry_.INo(), Helper::StringToHex(kv.key), Helper::StringToHex(parent_key),
                                      Helper::StringToHex(key));
    }
  }

  // update parent attr
  parent_attr.set_ctime(std::max(parent_attr.ctime(), GetTime()));
  parent_attr.set_mtime(std::max(parent_attr.mtime(), GetTime()));
  parent_attr.set_version(parent_attr.version() + 1);

  txn->Put(parent_key, MetaCodec::EncodeInodeValue(parent_attr));

  // decrease nlink
  attr.set_nlink(attr.nlink() - 1);
  attr.set_ctime(std::max(attr.ctime(), GetTime()));
  attr.set_version(attr.version() + 1);
  if (attr.nlink() <= 0) {
    // delete inode
    txn->Delete(key);
    // save delete file info
    txn->Put(MetaCodec::EncodeDelFileKey(fs_id, dentry_.INo()), MetaCodec::EncodeDelFileValue(attr));

  } else {
    txn->Put(key, MetaCodec::EncodeInodeValue(attr));
  }

  // delete dentry
  txn->Delete(MetaCodec::EncodeDentryKey(fs_id, parent, dentry_.Name()));

  SetAttr(parent_attr);
  result_.child_attr = attr;

  return Status::OK();
}

Status RenameOperation::Run(TxnUPtr& txn) {
  uint64_t time_ns = GetTime();

  DINGO_LOG(INFO) << fmt::format(
      "[operation.{}] rename old_parent({}), old_name({}), new_parent_ino({}), new_name({}).", fs_id_, old_parent_,
      old_name_, new_parent_, new_name_);

  bool is_same_parent = (old_parent_ == new_parent_);
  // batch get old parent attr/child dentry and new parentattr/child dentry
  std::string old_parent_key = MetaCodec::EncodeInodeKey(fs_id_, old_parent_);
  std::string old_dentry_key = MetaCodec::EncodeDentryKey(fs_id_, old_parent_, old_name_);
  std::string new_parent_key = MetaCodec::EncodeInodeKey(fs_id_, new_parent_);
  std::string new_dentry_key = MetaCodec::EncodeDentryKey(fs_id_, new_parent_, new_name_);

  std::vector<std::string> keys = {old_parent_key, old_dentry_key, new_dentry_key};
  if (!is_same_parent) keys.push_back(new_parent_key);
  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(keys, kvs);
  DINGO_LOG(INFO) << fmt::format("[operation.{}] kvs size({})", fs_id_, kvs.size());
  if (!status.ok()) {
    return status;
  }

  if (kvs.size() < 2) {
    return Status(pb::error::ENOT_FOUND, "not found old parent inode/old dentry");
  }

  AttrEntry old_parent_attr, new_parent_attr;
  DentryEntry old_dentry, prev_new_dentry;
  for (const auto& kv : kvs) {
    if (kv.key == old_parent_key) {
      old_parent_attr = MetaCodec::DecodeInodeValue(kv.value);
      if (is_same_parent) new_parent_attr = old_parent_attr;

    } else if (kv.key == old_dentry_key) {
      old_dentry = MetaCodec::DecodeDentryValue(kv.value);

    } else if (kv.key == new_parent_key) {
      new_parent_attr = MetaCodec::DecodeInodeValue(kv.value);

    } else if (kv.key == new_dentry_key) {
      prev_new_dentry = MetaCodec::DecodeDentryValue(kv.value);
    }
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
  if (!status.ok()) {
    return status;
  }
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
    CHECK(prev_new_attr.ino() != 0) << "prev new inode is null.";

    if (prev_new_dentry.type() == pb::mds::DIRECTORY) {
      // check new dentry is empty
      bool is_empty;
      status = CheckDirEmpty(txn, fs_id_, prev_new_dentry.ino(), is_empty);
      if (!status.ok()) {
        return status;
      }
      if (!is_empty) {
        return Status(pb::error::ENOT_EMPTY, fmt::format("new dentry({}/{}) is not empty.", new_parent_, new_name_));
      }

      // delete exist new inode
      txn->Delete(prev_new_inode_key);

    } else {
      // update exist new inode nlink
      prev_new_attr.set_nlink(prev_new_attr.nlink() - 1);
      prev_new_attr.set_ctime(std::max(prev_new_attr.ctime(), time_ns));
      prev_new_attr.set_mtime(std::max(prev_new_attr.mtime(), time_ns));
      prev_new_attr.set_version(prev_new_attr.version() + 1);
      if (prev_new_attr.nlink() <= 0) {
        // delete exist new inode
        txn->Delete(prev_new_inode_key);
        // save delete file info
        txn->Put(MetaCodec::EncodeDelFileKey(fs_id_, prev_new_attr.ino()),
                 MetaCodec::EncodeDelFileValue(prev_new_attr));

      } else {
        // update exist new inode attr
        txn->Put(prev_new_inode_key, MetaCodec::EncodeInodeValue(prev_new_attr));
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
  if (old_dentry.type() == pb::mds::FileType::DIRECTORY &&
      (!is_same_parent || (is_same_parent && is_exist_new_dentry))) {
    old_parent_attr.set_nlink(old_parent_attr.nlink() - 1);
  }
  old_parent_attr.set_version(old_parent_attr.version() + 1);

  status = txn->Put(old_parent_key, MetaCodec::EncodeInodeValue(old_parent_attr));

  if (!is_same_parent) {
    // update new parent inode attr
    new_parent_attr.set_ctime(std::max(new_parent_attr.ctime(), time_ns));
    new_parent_attr.set_mtime(std::max(new_parent_attr.mtime(), time_ns));
    if (new_dentry.type() == pb::mds::FileType::DIRECTORY && !is_exist_new_dentry)
      new_parent_attr.set_nlink(new_parent_attr.nlink() + 1);
    new_parent_attr.set_version(new_parent_attr.version() + 1);

    txn->Put(new_parent_key, MetaCodec::EncodeInodeValue(new_parent_attr));
  }

  result_.old_parent_attr = old_parent_attr;
  result_.old_dentry = old_dentry;
  result_.old_attr = old_attr;
  result_.new_parent_attr = new_parent_attr;
  result_.prev_new_dentry = prev_new_dentry;
  result_.prev_new_attr = prev_new_attr;
  result_.new_dentry = new_dentry;

  result_.is_same_parent = is_same_parent;
  result_.is_exist_new_dentry = is_exist_new_dentry;

  return Status::OK();
}

bool CompactChunkOperation::MaybeCompact(const FsInfoEntry& fs_info, Ino ino, uint64_t file_length,
                                         const ChunkEntry& chunk) {
  auto trash_slice_list = GenTrashSlices(fs_info, ino, file_length, chunk);

  return !trash_slice_list.slices().empty();
}

TrashSliceList CompactChunkOperation::GenTrashSlices(const FsInfoEntry& fs_info, Ino ino, uint64_t file_length,
                                                     const ChunkEntry& chunk) {
  struct Slice {
    uint32_t sort_id;
    uint64_t id;
    uint32_t compaction_version;
    uint64_t offset;
    uint32_t len;
    uint32_t size;
    bool zero;
  };

  struct Chunk {
    uint64_t index;
    uint64_t chunk_size;
    uint64_t block_size;
    std::vector<Slice> slices;
    uint64_t version;
    uint64_t last_compaction_time_ms;
  };

  struct OffsetRange {
    uint64_t start;
    uint64_t end;
    std::vector<Slice> slices;
  };

  struct Block {
    uint64_t slice_id;
    uint64_t offset;
    uint32_t size;
  };

  // SliceEntry -> Slice
  auto to_slice_fn = [](uint32_t sort_id, const SliceEntry& slice) {
    return Slice{.sort_id = sort_id,
                 .id = slice.id(),
                 .compaction_version = slice.compaction_version(),
                 .offset = slice.offset(),
                 .len = slice.len(),
                 .size = slice.size(),
                 .zero = slice.zero()};
  };

  // ChunkEntry -> Chunk
  auto to_chunk_fn = [to_slice_fn](const ChunkEntry& chunk_entry) {
    Chunk chunk;
    chunk.index = chunk_entry.index();
    chunk.chunk_size = chunk_entry.chunk_size();
    chunk.block_size = chunk_entry.block_size();
    chunk.slices.reserve(chunk_entry.slices_size());
    for (int i = 0; i < chunk_entry.slices_size(); ++i) {
      chunk.slices.push_back(to_slice_fn(i, chunk_entry.slices(i)));
    }
    chunk.version = chunk_entry.version();
    chunk.last_compaction_time_ms = chunk_entry.last_compaction_time_ms();

    return chunk;
  };

  const uint32_t fs_id = fs_info.fs_id();
  const uint64_t chunk_size = fs_info.chunk_size();
  const uint64_t block_size = fs_info.block_size();
  const uint64_t chunk_offset = chunk.index() * chunk_size;

  auto chunk_copy = to_chunk_fn(chunk);

  TrashSliceList trash_slices;
  trash_slices.set_time_ms(Helper::TimestampMs());

  // 1. complete out of file length slices
  // chunk offset:               |______|
  // file length: |____________|
  // 2. partial out of file length slices
  // chunk offset:          |______|
  // file length: |____________|
  if (chunk_offset + chunk_size > file_length) {
    for (const auto& slice : chunk.slices()) {
      if (slice.offset() >= file_length) {
        TrashSliceEntry trash_slice;
        trash_slice.set_fs_id(fs_id);
        trash_slice.set_ino(ino);
        trash_slice.set_chunk_index(chunk.index());
        trash_slice.set_slice_id(slice.id());
        trash_slice.set_block_size(block_size);
        trash_slice.set_chunk_size(chunk_size);
        trash_slice.set_is_partial(false);
        auto* range = trash_slice.add_ranges();
        range->set_offset(slice.offset());
        range->set_len(slice.len());
        range->set_compaction_version(slice.compaction_version());

        trash_slices.add_slices()->Swap(&trash_slice);
      }
    }
  }

  size_t out_of_length_count = trash_slices.slices_size();

  // 2. complete overlapped slices
  //     |______4________|      slices
  // |__1__|___2___|____3_____| slices
  // |________________________| chunk
  // slice-2 is complete overlapped by slice-4
  // sort by offset

  // get offset ranges
  std::set<uint64_t> offsets;
  for (const auto& slice : chunk_copy.slices) {
    offsets.insert(slice.offset);
    offsets.insert(slice.offset + slice.len);
  }

  std::vector<OffsetRange> offset_ranges;
  offset_ranges.reserve(offsets.size());
  for (auto it = offsets.begin(); it != offsets.end(); ++it) {
    auto next_it = std::next(it);
    if (next_it != offsets.end()) {
      offset_ranges.push_back({.start = *it, .end = *next_it});
    }
  }

  for (auto& offset_range : offset_ranges) {
    for (const auto& slice : chunk_copy.slices) {
      uint64_t slice_start = slice.offset;
      uint64_t slice_end = slice.offset + slice.len;

      // check intersect
      if (slice_end <= offset_range.start || slice_start >= offset_range.end) {
        continue;
      }
      offset_range.slices.push_back(slice);
    }
  }

  // get reserve slice ids
  std::set<uint64_t> reserve_slice_ids;
  for (auto& offset_range : offset_ranges) {
    // sort by id, from newest to oldest
    std::sort(offset_range.slices.begin(), offset_range.slices.end(),  // NOLINT
              [](const Slice& a, const Slice& b) { return a.sort_id > b.sort_id; });
    if (!offset_range.slices.empty()) {
      reserve_slice_ids.insert(offset_range.slices.front().id);
    }
  }

  // get delete slices
  for (const auto& slice : chunk_copy.slices) {
    if (reserve_slice_ids.count(slice.id) == 0) {
      TrashSliceEntry trash_slice;
      trash_slice.set_fs_id(fs_id);
      trash_slice.set_ino(ino);
      trash_slice.set_chunk_index(chunk.index());
      trash_slice.set_slice_id(slice.id);
      trash_slice.set_block_size(block_size);
      trash_slice.set_chunk_size(chunk_size);
      trash_slice.set_is_partial(false);
      auto* range = trash_slice.add_ranges();
      range->set_offset(slice.offset);
      range->set_len(slice.len);
      range->set_compaction_version(slice.compaction_version);

      trash_slices.add_slices()->Swap(&trash_slice);
    }
  }

  size_t complete_overlapped_count = trash_slices.slices_size() - out_of_length_count;

  // 3. partial overlapped slices
  //     |______4________|      slices
  // |__1__|___2___|____3_____| slices
  // |________________________| chunk
  // slice-1 and slice-3 are partial overlapped by slice-4
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
  // slice id -> TrashSliceEntry
  // std::map<uint64_t, TrashSliceEntry> temp_trash_slice_map;
  // for (auto& offset_range : offset_ranges) {
  //   if (offset_range.slices.empty()) continue;

  //   auto& slices = offset_range.slices;
  //   for (size_t i = 1; i < slices.size(); ++i) {
  //     auto& slice = slices[i];
  //     if (reserve_slice_ids.count(slice.id) == 0) {
  //       continue;
  //     }

  //     auto start_offset = std::max(offset_range.start, slice.offset);
  //     auto end_offset = std::min(offset_range.end, slice.offset + slice.len);

  //     for (uint64_t block_offset = slice.offset; block_offset < end_offset; block_offset += block_size) {
  //       if (block_offset >= start_offset && block_offset + block_size < end_offset) {
  //         auto it = temp_trash_slice_map.find(slice.id);
  //         if (it == temp_trash_slice_map.end()) {
  //           TrashSliceEntry trash_slice;
  //           trash_slice.set_fs_id(fs_id);
  //           trash_slice.set_ino(ino);
  //           trash_slice.set_chunk_index(chunk.index());
  //           trash_slice.set_slice_id(slice.id);
  //           trash_slice.set_block_size(block_size);
  //           trash_slice.set_chunk_size(chunk_size);
  //           trash_slice.set_is_partial(true);
  //           auto* range = trash_slice.add_ranges();
  //           range->set_offset(slice.offset);
  //           range->set_len(slice.len);
  //           range->set_compaction_version(slice.compaction_version);

  //           temp_trash_slice_map[slice.id] = trash_slice;
  //         } else {
  //           auto* range = it->second.add_ranges();
  //           range->set_offset(block_offset);
  //           range->set_len(block_size);
  //           range->set_compaction_version(slice.compaction_version);
  //         }
  //       }
  //     }
  //   }
  // }

  // for (auto& [_, trash_slice] : temp_trash_slice_map) {
  //   trash_slices.add_slices()->Swap(&trash_slice);
  // }

  size_t partial_overlapped_count = trash_slices.slices_size() - out_of_length_count - complete_overlapped_count;

  std::string slice_id_str;
  slice_id_str.reserve(trash_slices.slices_size() * 9);
  for (const auto& slice : trash_slices.slices()) {
    slice_id_str += std::to_string(slice.slice_id());
    slice_id_str += ",";
  }

  DINGO_LOG(INFO) << fmt::format("[operation.{}.{}.{}] trash slice, count({}/{}/{}/{}) slice_ids({}).", fs_id, ino,
                                 chunk.index(), out_of_length_count, complete_overlapped_count,
                                 partial_overlapped_count, trash_slices.slices_size(), slice_id_str);

  return trash_slices;
}

TrashSliceList CompactChunkOperation::GenTrashSlices(Ino ino, uint64_t file_length, const ChunkEntry& chunk) {
  return GenTrashSlices(fs_info_, ino, file_length, chunk);
}

void CompactChunkOperation::UpdateChunk(ChunkEntry& chunk, const TrashSliceList& trash_slices) {
  auto gen_slice_map_fn = [](const TrashSliceList& trash_slices) {
    // slice id -> TrashSliceEntry
    std::map<uint64_t, TrashSliceEntry> slice_map;
    for (const auto& slice : trash_slices.slices()) {
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

TrashSliceList CompactChunkOperation::DoCompactChunk(Ino ino, uint64_t file_length, ChunkEntry& chunk) {
  auto trash_slice_list = GenTrashSlices(ino, file_length, chunk);
  if (trash_slice_list.slices().empty()) {
    return {};
  }

  UpdateChunk(chunk, trash_slice_list);

  return trash_slice_list;
}

TrashSliceList CompactChunkOperation::CompactChunk(TxnUPtr& txn, uint32_t fs_id, Ino ino, uint64_t file_length,
                                                   ChunkEntry& chunk) {
  auto trash_slice_list = DoCompactChunk(ino, file_length, chunk);
  if (trash_slice_list.slices().empty()) {
    return {};
  }

  txn->Put(MetaCodec::EncodeDelSliceKey(fs_id, ino, chunk.index(), Helper::TimestampNs()),
           MetaCodec::EncodeDelSliceValue(trash_slice_list));

  return trash_slice_list;
}

TrashSliceList CompactChunkOperation::CompactChunks(TxnUPtr& txn, uint32_t fs_id, Ino ino, uint64_t file_length,
                                                    Inode::ChunkMap& chunks) {
  TrashSliceList trash_slice_list;
  for (auto& [_, chunk] : chunks) {
    auto part_trash_slice_list = CompactChunk(txn, fs_id, ino, file_length, chunk);

    for (auto trash_slice = part_trash_slice_list.mutable_slices()->begin();
         trash_slice != part_trash_slice_list.slices().end(); ++trash_slice) {
      trash_slice_list.add_slices()->Swap(&*trash_slice);
    }
  }

  return trash_slice_list;
}

Status CompactChunkOperation::Run(TxnUPtr& txn) {
  const uint32_t fs_id = fs_info_.fs_id();
  CHECK(fs_id > 0) << "fs_id is 0";
  CHECK(ino_ > 0) << "ino is 0.";

  std::string key = MetaCodec::EncodeChunkKey(fs_id, ino_, chunk_index_);
  std::string value;
  auto status = txn->Get(key, value);
  if (!status.ok()) return status;

  ChunkEntry chunk = MetaCodec::DecodeChunkValue(value);

  // reduce compact frequency
  if (!is_force_ && chunk.last_compaction_time_ms() + FLAGS_mds_compact_chunk_interval_ms >
                        static_cast<uint64_t>(Helper::TimestampMs())) {
    return Status(pb::error::ENOT_MATCH, "not match compact condition");
  }

  auto trash_slice_list = CompactChunk(txn, fs_id, ino_, file_length_, chunk);
  if (!trash_slice_list.slices().empty()) {
    chunk.set_version(chunk.version() + 1);
    chunk.set_last_compaction_time_ms(Helper::TimestampMs());
    txn->Put(key, MetaCodec::EncodeChunkValue(chunk));

    result_.trash_slice_list = std::move(trash_slice_list);
    result_.effected_chunk = std::move(chunk);
  }

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
    fs_quota.set_uuid(utils::UUIDGenerator::GenerateUUID());
    fs_quota.set_create_time_ns(Helper::TimestampNs());
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
  std::string key = MetaCodec::EncodeFsQuotaKey(fs_id_);
  std::string value;
  auto status = txn->Get(key, value);
  if (!status.ok()) {
    return status;
  }

  result_.quota = MetaCodec::DecodeFsQuotaValue(value);

  return Status::OK();
}

Status FlushFsUsageOperation::Run(TxnUPtr& txn) {
  std::string key = MetaCodec::EncodeFsQuotaKey(fs_id_);
  std::string value;
  auto status = txn->Get(key, value);
  if (!status.ok()) {
    return status;
  }
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
    dir_quota.set_uuid(utils::UUIDGenerator::GenerateUUID());
    dir_quota.set_create_time_ns(Helper::TimestampNs());
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
  if (!status.ok()) {
    return status;
  }

  if (!value.empty()) {
    auto dir_quota = MetaCodec::DecodeDirQuotaValue(value);
    result_.quota = dir_quota;
  }

  return Status::OK();
}

Status DeleteDirQuotaOperation::Run(TxnUPtr& txn) {
  std::string value;
  auto status = txn->Get(MetaCodec::EncodeDirQuotaKey(fs_id_, ino_), value);
  if (!status.ok()) {
    return status;
  }

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
  if (!status.ok()) {
    return status;
  }

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
  if (!status.ok()) {
    return status;
  }
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

Status CleanDelSliceOperation::Run(TxnUPtr& txn) {
  txn->Delete(key_);
  return Status::OK();
}

Status GetDelFileOperation::Run(TxnUPtr& txn) {
  std::string value;
  auto status = txn->Get(MetaCodec::EncodeDelFileKey(fs_id_, ino_), value);
  if (!status.ok()) {
    return status;
  }

  if (!value.empty()) {
    SetAttr(MetaCodec::DecodeDelFileValue(value));
  }

  return Status::OK();
}

Status CleanDelFileOperation::Run(TxnUPtr& txn) {
  txn->Delete(MetaCodec::EncodeDelFileKey(fs_id_, ino_));
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

  std::string value;
  auto status = txn->Get(MetaCodec::EncodeInodeKey(fs_id_, ino_), value);
  if (!status.ok()) return status;

  SetAttr(MetaCodec::DecodeInodeValue(value));

  return Status::OK();
}

Status BatchGetInodeAttrOperation::Run(TxnUPtr& txn) {
  CHECK(fs_id_ > 0) << "fs_id is 0";
  CHECK(!inoes_.empty()) << "inoes_ is empty";

  std::vector<std::string> keys;
  keys.reserve(inoes_.size());
  for (auto& ino : inoes_) {
    keys.push_back(MetaCodec::EncodeInodeKey(fs_id_, ino));
  }

  std::vector<KeyValue> kvs;
  auto status = txn->BatchGet(keys, kvs);
  if (!status.ok()) return status;

  result_.attrs.clear();
  for (auto& kv : kvs) {
    CHECK(MetaCodec::IsInodeKey(kv.key)) << fmt::format("invalid inode key({}).", kv.key);

    result_.attrs.push_back(MetaCodec::DecodeInodeValue(kv.value));
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

OperationProcessor::OperationProcessor(KVStorageSPtr kv_storage) : kv_storage_(kv_storage) {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << fmt::format("[operation] bthread_mutex_init fail.");
  CHECK(bthread_cond_init(&cond_, nullptr) == 0) << fmt::format("[operation] bthread_cond_init fail.");

  async_worker_ = Worker::New();
  CHECK(async_worker_ != nullptr) << fmt::format("[operation] create async worker fail.");
}

OperationProcessor::~OperationProcessor() {
  bthread_cond_destroy(&cond_);
  bthread_mutex_destroy(&mutex_);
}

bool OperationProcessor::Init() {
  struct Param {
    OperationProcessor* self{nullptr};
  };

  Param* param = new Param({this});

  const bthread_attr_t attr = BTHREAD_ATTR_LARGE;
  if (bthread_start_background(
          &tid_, &attr,
          [](void* arg) -> void* {
            Param* param = reinterpret_cast<Param*>(arg);

            param->self->ProcessOperation();

            delete param;
            return nullptr;
          },
          param) != 0) {
    tid_ = 0;
    delete param;
    LOG(FATAL) << "[operation] start background thread fail.";
    return false;
  }

  if (!async_worker_->Init()) {
    LOG(FATAL) << fmt::format("[operation] async worker init fail.");
    return false;
  }

  return true;
}

bool OperationProcessor::Destroy() {
  is_stop_.store(true);

  if (tid_ > 0) {
    bthread_cond_signal(&cond_);

    if (bthread_stop(tid_) != 0) {
      LOG(ERROR) << fmt::format("[operation] bthread_stop fail.");
    }

    if (bthread_join(tid_, nullptr) != 0) {
      LOG(ERROR) << fmt::format("[operation] bthread_join fail.");
    }
  }

  async_worker_->Destroy();

  return true;
}

bool OperationProcessor::RunBatched(Operation* operation) {
  if (is_stop_.load(std::memory_order_relaxed)) {
    return false;
  }

  operations_.Enqueue(operation);

  bthread_cond_signal(&cond_);

  return true;
}

Status OperationProcessor::RunAlone(Operation* operation) {
  Duration duration;

  auto& trace = operation->GetTrace();
  const uint32_t fs_id = operation->GetFsId();
  const Ino ino = operation->GetIno();
  Status status;
  uint32_t retry = 0;
  int64_t txn_id = 0;
  bool is_one_pc = false;
  do {
    Duration once_duration;
    auto txn = kv_storage_->NewTxn(operation->GetIsolationLevel());
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }
    txn_id = txn->ID();

    status = operation->Run(txn);
    if (!status.ok()) {
      if (status.error_code() == pb::error::ESTORE_MAYBE_RETRY) {
        DINGO_LOG(WARNING) << fmt::format(
            "[operation.{}.{}][{}][{}us] alone run {} lock conflict, retry({}) status({}).", fs_id, ino, txn_id,
            once_duration.ElapsedUs(), operation->OpName(), retry, status.error_str());
        bthread_usleep(CalWaitTimeUs(retry));
        continue;
      }
      break;
    }

    status = txn->Commit();

    auto txn_trace = txn->GetTrace();
    is_one_pc = txn_trace.is_one_pc;
    trace.AddTxn(txn_trace);

    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    DINGO_LOG(WARNING) << fmt::format("[operation.{}.{}][{}][{}us] alone run {} fail, onepc({}) retry({}) status({}).",
                                      fs_id, ino, txn_id, once_duration.ElapsedUs(), operation->OpName(), is_one_pc,
                                      retry, status.error_str());

    bthread_usleep(CalWaitTimeUs(retry));

  } while (++retry <= FLAGS_mds_txn_max_retry_times);

  trace.RecordElapsedTime("store_operate");

  DINGO_LOG(INFO) << fmt::format("[operation.{}.{}][{}][{}us] alone run {} finish, onepc({}) retry({}) status({}).",
                                 fs_id, ino, txn_id, duration.ElapsedUs(), operation->OpName(), is_one_pc, retry,
                                 status.error_str());

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
    DINGO_LOG(ERROR) << fmt::format("[operation] async worker execute fail, operation({}).", operation->OpName());
  }

  return ret;
}

std::map<OperationProcessor::Key, BatchOperation> OperationProcessor::Grouping(std::vector<Operation*>& operations) {
  std::map<Key, BatchOperation> batch_operation_map;

  for (auto* operation : operations) {
    Key key = {.fs_id = operation->GetFsId(), .ino = operation->GetIno()};

    auto it = batch_operation_map.find(key);
    if (it == batch_operation_map.end()) {
      BatchOperation batch_operation = {.fs_id = operation->GetFsId(), .ino = operation->GetIno()};
      if (operation->IsCreateType()) {
        batch_operation.create_operations.push_back(operation);

      } else if (operation->IsSetAttrType()) {
        batch_operation.setattr_operations.push_back(operation);

      } else {
        DINGO_LOG(FATAL) << "[operation] invalid operation type.";
      }
      batch_operation_map.insert(std::make_pair(key, batch_operation));

    } else {
      if (operation->IsCreateType()) {
        it->second.create_operations.push_back(operation);

      } else if (operation->IsSetAttrType()) {
        it->second.setattr_operations.push_back(operation);

      } else {
        DINGO_LOG(FATAL) << "[operation] invalid operation type.";
      }
    }
  }

  return batch_operation_map;
}

void OperationProcessor::ProcessOperation() {
  std::vector<Operation*> stage_operations;
  stage_operations.reserve(FLAGS_mds_store_operation_batch_size);

  while (true) {
    stage_operations.clear();

    Operation* operation = nullptr;
    while (!operations_.Dequeue(operation) && !is_stop_.load(std::memory_order_relaxed)) {
      bthread_mutex_lock(&mutex_);
      bthread_cond_wait(&cond_, &mutex_);
      bthread_mutex_unlock(&mutex_);
    }

    if (is_stop_.load(std::memory_order_relaxed) && stage_operations.empty()) {
      break;
    }

    bool is_waited = false;
    do {
      stage_operations.push_back(operation);
      if (!operations_.Dequeue(operation)) {
        break;
      }

      if (!is_waited && FLAGS_mds_store_operation_merge_delay_us > 0) {
        bthread_usleep(FLAGS_mds_store_operation_merge_delay_us);
        is_waited = true;
      }

    } while (true);

    auto batch_operation_map = Grouping(stage_operations);
    for (auto& [_, batch_operation] : batch_operation_map) {
      LaunchExecuteBatchOperation(batch_operation);
    }
  }
}

void OperationProcessor::LaunchExecuteBatchOperation(const BatchOperation& batch_operation) {
  struct Params {
    OperationProcessor* self{nullptr};
    BatchOperation batch_operation;
  };

  Params* params = new Params({.self = this, .batch_operation = batch_operation});

  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_SMALL;
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

  return op_names;
}

void OperationProcessor::ExecuteBatchOperation(BatchOperation& batch_operation) {
  const uint32_t fs_id = batch_operation.fs_id;
  const uint64_t ino = batch_operation.ino;

  Duration duration;

  // get prefetch keys
  std::string primary_key = MetaCodec::EncodeInodeKey(fs_id, ino);
  std::vector<std::string> keys = {primary_key};
  for (auto* operation : batch_operation.setattr_operations) {
    auto prefetch_keys = operation->PrefetchKey();
    if (!prefetch_keys.empty()) keys.insert(keys.end(), prefetch_keys.begin(), prefetch_keys.end());
  }

  SetElapsedTime(batch_operation, "store_pending");

  AttrEntry attr;
  Status status;
  uint32_t retry = 0;
  int count = 0;
  int64_t txn_id = 0;
  bool is_one_pc = false;
  std::string op_names = GetName(batch_operation);
  do {
    Duration once_duration;

    auto txn = kv_storage_->NewTxn();
    if (txn == nullptr) {
      status = Status(pb::error::EBACKEND_STORE, "new transaction fail");
      continue;
    }
    txn_id = txn->ID();

    std::vector<KeyValue> prefetch_kvs;
    status = txn->BatchGet(keys, prefetch_kvs);
    if (!status.ok()) {
      if (status.error_code() == pb::error::ESTORE_MAYBE_RETRY) {
        DINGO_LOG(WARNING) << fmt::format(
            "[operation.{}.{}][{}][{}us] batch run {} lock conflict, retry({}) status({}).", fs_id, ino, txn_id,
            once_duration.ElapsedUs(), op_names, retry, status.error_str());
        bthread_usleep(CalWaitTimeUs(retry));
        continue;
      }
      break;
    }

    attr = MetaCodec::DecodeInodeValue(FindValue(prefetch_kvs, primary_key));

    // run set attr operations
    for (auto* operation : batch_operation.setattr_operations) {
      operation->RunInBatch(txn, attr, prefetch_kvs);
      if (retry == 0) ++count;
    }

    // run create operations
    for (auto* operation : batch_operation.create_operations) {
      operation->RunInBatch(txn, attr, prefetch_kvs);
      if (retry == 0) ++count;
    }

    attr.set_version(attr.version() + 1);
    txn->Put(primary_key, MetaCodec::EncodeInodeValue(attr));

    status = txn->Commit();

    auto txn_trace = txn->GetTrace();
    is_one_pc = txn_trace.is_one_pc;
    SetTrace(batch_operation, txn_trace);

    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    DINGO_LOG(WARNING) << fmt::format(
        "[operation.{}.{}][{}][{}us] batch run ({}) fail, count({}) onepc({}) retry({}) status({}).", fs_id, ino,
        txn_id, once_duration.ElapsedUs(), op_names, count, is_one_pc, retry, status.error_str());

    bthread_usleep(CalWaitTimeUs(retry));

  } while (++retry <= FLAGS_mds_txn_max_retry_times);

  SetElapsedTime(batch_operation, "store_operate");

  DINGO_LOG(INFO) << fmt::format(
      "[operation.{}.{}][{}][{}us] batch run ({}) finish, count({}) onepc({}) retry({}) status({}) attr({}).", fs_id,
      ino, txn_id, duration.ElapsedUs(), op_names, count, is_one_pc, retry, status.error_str(), DescribeAttr(attr));

  if (status.ok()) {
    SetAttr(batch_operation, attr);

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
      DINGO_LOG(ERROR) << "[fsset] check fs table exist fail, error: " << status.error_str();
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