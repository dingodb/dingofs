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

#include "client/vfs/meta/v2/filesystem.h"

#include <fcntl.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "client/vfs/meta/v2/client_id.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

const uint32_t kMaxHostNameLength = 255;

const uint32_t kMaxXAttrNameLength = 255;
const uint32_t kMaxXAttrValueLength = 64 * 1024;

const uint32_t kHeartbeatIntervalS = 5;  // seconds

const std::set<std::string> kXAttrBlackList = {
    "system.posix_acl_access", "system.posix_acl_default", "system.nfs4_acl"};

DEFINE_uint32(read_dir_batch_size, 1024, "read dir batch size.");

std::string GetHostName() {
  char hostname[kMaxHostNameLength];
  int ret = gethostname(hostname, kMaxHostNameLength);
  if (ret < 0) {
    LOG(ERROR) << "[meta] GetHostName fail, ret=" << ret;
    return "";
  }

  return std::string(hostname);
}

Status DirIteratorImpl::Seek() {
  std::vector<DirEntry> entries;
  auto status = mds_client_->ReadDir(ino_, last_name_,
                                     FLAGS_read_dir_batch_size, true, entries);
  if (!status.ok()) {
    return status;
  }

  offset_ = 0;
  entries_ = std::move(entries);
  if (!entries_.empty()) {
    last_name_ = entries_.back().name;
  }

  return Status::OK();
}

bool DirIteratorImpl::Valid() { return offset_ < entries_.size(); }

DirEntry DirIteratorImpl::GetValue(bool with_attr) {
  CHECK(offset_ < entries_.size()) << "offset out of range";

  with_attr_ = with_attr;
  return entries_[offset_];
}

void DirIteratorImpl::Next() {
  if (++offset_ < entries_.size()) {
    return;
  }

  std::vector<DirEntry> entries;
  auto status = mds_client_->ReadDir(
      ino_, last_name_, FLAGS_read_dir_batch_size, with_attr_, entries);
  if (!status.ok()) {
    return;
  }

  offset_ = 0;
  entries_ = std::move(entries);
  if (!entries_.empty()) {
    last_name_ = entries_.back().name;
  }
}

bool FileSessionMap::Put(uint64_t fh, std::string session_id) {
  utils::WriteLockGuard lk(lock_);

  auto it = file_session_map_.find(fh);
  if (it != file_session_map_.end()) {
    return false;
  }

  file_session_map_.insert(std::make_pair(fh, session_id));

  return true;
}

void FileSessionMap::Delete(uint64_t fh) {
  utils::WriteLockGuard lk(lock_);

  file_session_map_.erase(fh);
}

std::string FileSessionMap::Get(uint64_t fh) {
  utils::ReadLockGuard lk(lock_);

  auto it = file_session_map_.find(fh);
  return (it != file_session_map_.end()) ? it->second : "";
}

MDSV2FileSystem::MDSV2FileSystem(mdsv2::FsInfoPtr fs_info,
                                 const ClientId& client_id,
                                 MDSDiscoveryPtr mds_discovery,
                                 MDSClientPtr mds_client)
    : name_(fs_info->GetName()),
      client_id_(client_id),
      fs_info_(fs_info),
      mds_discovery_(mds_discovery),
      mds_client_(mds_client) {}

MDSV2FileSystem::~MDSV2FileSystem() {}  // NOLINT

Status MDSV2FileSystem::Init() {
  LOG(INFO) << fmt::format("[meta.{}] fs_info: {}.", fs_info_->GetName(),
                           fs_info_->ToString());
  // mount fs
  if (!MountFs()) {
    LOG(ERROR) << fmt::format("[meta.{}] mount fs fail.", name_);
    return Status::MountFailed("mount fs fail");
  }

  // init crontab
  if (!InitCrontab()) {
    LOG(ERROR) << fmt::format("[meta.{}] init crontab fail.", name_);
    return Status::Internal("init crontab fail");
  }

  return Status::OK();
}

void MDSV2FileSystem::UnInit() {
  // unmount fs
  UnmountFs();

  crontab_manager_.Destroy();
}

static StoreType ToStoreType(pb::mdsv2::FsType fs_type) {
  switch (fs_type) {
    case pb::mdsv2::FsType::S3:
      return StoreType::kS3;

    case pb::mdsv2::FsType::RADOS:
      return StoreType::kRados;

    default:
      CHECK(false) << "unknown fs type: " << pb::mdsv2::FsType_Name(fs_type);
  }
}

static S3Info ToS3Info(const pb::mdsv2::S3Info& s3_info) {
  S3Info result;
  result.ak = s3_info.ak();
  result.sk = s3_info.sk();
  result.endpoint = s3_info.endpoint();
  result.bucket_name = s3_info.bucketname();
  return result;
}

static RadosInfo ToRadosInfo(const pb::mdsv2::RadosInfo& rados_info) {
  RadosInfo result;
  result.user_name = rados_info.user_name();
  result.key = rados_info.key();
  result.mon_host = rados_info.mon_host();
  result.pool_name = rados_info.pool_name();
  result.cluster_name = rados_info.cluster_name();
  return result;
}

Status MDSV2FileSystem::GetFsInfo(FsInfo* fs_info) {
  auto temp_fs_info = fs_info_->Get();

  fs_info->name = name_;
  fs_info->id = temp_fs_info.fs_id();
  fs_info->chunk_size = temp_fs_info.chunk_size();
  fs_info->block_size = temp_fs_info.block_size();
  fs_info->uuid = temp_fs_info.uuid();

  fs_info->storage_info.store_type = ToStoreType(temp_fs_info.fs_type());
  if (fs_info->storage_info.store_type == StoreType::kS3) {
    CHECK(temp_fs_info.extra().has_s3_info())
        << "fs type is S3, but s3 info is not set";

    fs_info->storage_info.s3_info = ToS3Info(temp_fs_info.extra().s3_info());

  } else if (fs_info->storage_info.store_type == StoreType::kRados) {
    CHECK(temp_fs_info.extra().has_rados_info())
        << "fs type is Rados, but rados info is not set";

    fs_info->storage_info.rados_info =
        ToRadosInfo(temp_fs_info.extra().rados_info());

  } else {
    LOG(ERROR) << fmt::format("unknown fs type: {}.",
                              pb::mdsv2::FsType_Name(temp_fs_info.fs_type()));
    return Status::InvalidParam("unknown fs type");
  }

  return Status::OK();
}

bool MDSV2FileSystem::MountFs() {
  pb::mdsv2::MountPoint mount_point;
  mount_point.set_client_id(client_id_.ID());
  mount_point.set_hostname(client_id_.Hostname());
  mount_point.set_path(client_id_.Mountpoint());
  mount_point.set_cto(false);

  LOG(INFO) << fmt::format("[meta.{}] mount point: {}.", name_,
                           mount_point.ShortDebugString());

  auto status = mds_client_->MountFs(name_, mount_point);
  if (!status.ok() && status.Errno() != pb::error::EEXISTED) {
    LOG(ERROR) << fmt::format(
        "[meta.{}] mount fs info fail, mountpoint({}), {}.", name_,
        client_id_.Mountpoint(), status.ToString());
    return false;
  }

  return true;
}

bool MDSV2FileSystem::UnmountFs() {
  auto status = mds_client_->UmountFs(name_, client_id_.ID());
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.{}] mount fs info fail, mountpoint({}).",
                              name_, client_id_.Mountpoint());
    return false;
  }

  return true;
}

void MDSV2FileSystem::Heartbeat() {
  auto status = mds_client_->Heartbeat();
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("[meta.{}] heartbeat fail, error: {}.", name_,
                              status.ToString());
  }
}

bool MDSV2FileSystem::InitCrontab() {
  // Add heartbeat crontab
  crontab_configs_.push_back({
      "HEARTBEA",
      kHeartbeatIntervalS * 1000,
      true,
      [this](void*) { this->Heartbeat(); },
  });

  crontab_manager_.AddCrontab(crontab_configs_);

  return true;
}

Status MDSV2FileSystem::StatFs(Ino ino, FsStat* fs_stat) {
  auto status = mds_client_->GetFsQuota(*fs_stat);

  if (fs_stat->max_bytes == 0) {
    fs_stat->max_bytes = UINT64_MAX;
  }

  if (fs_stat->max_inodes == 0) {
    fs_stat->max_inodes = UINT64_MAX;
  }

  return status;
};

Status MDSV2FileSystem::Lookup(Ino parent, const std::string& name,
                               Attr* out_attr) {
  auto status = mds_client_->Lookup(parent, name, *out_attr);
  if (!status.ok()) {
    if (status.Errno() == pb::error::ENOT_FOUND) {
      return Status::NotExist("not found dentry");
    }
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Create(Ino parent, const std::string& name,
                               uint32_t uid, uint32_t gid, uint32_t mode,
                               int flags, Attr* attr, uint64_t fh) {
  auto status = MkNod(parent, name, uid, gid, mode, 0, attr);
  if (!status.ok()) {
    return status;
  }

  return Open(attr->ino, flags, fh);
}

Status MDSV2FileSystem::MkNod(Ino parent, const std::string& name, uint32_t uid,
                              uint32_t gid, uint32_t mode, uint64_t rdev,
                              Attr* out_attr) {
  auto status =
      mds_client_->MkNod(parent, name, uid, gid, mode, rdev, *out_attr);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Open(Ino ino, int flags, uint64_t fh) {
  LOG(INFO) << fmt::format("[meta.{}] open ino({}) flags({}).", name_, ino,
                           flags);

  if ((flags & O_TRUNC) && !(flags & O_WRONLY || flags & O_RDWR)) {
    return Status::NoPermission("O_TRUNC without O_WRONLY or O_RDWR");
  }

  std::string session_id;
  auto status = mds_client_->Open(ino, flags, session_id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.{}] open ino({}) fail, error: {}.", name_,
                              ino, status.ToString());
    return status;
  }

  CHECK(file_session_map_.Put(fh, session_id))
      << fmt::format("put file session fail, ino: {}.", ino);

  return Status::OK();
}

Status MDSV2FileSystem::Close(Ino ino, uint64_t fh) {
  LOG(INFO) << fmt::format("[meta.{}] release ino({}).", name_, ino);

  std::string session_id = file_session_map_.Get(fh);
  CHECK(!session_id.empty())
      << fmt::format("get file session fail, ino({}) fh({}).", ino, fh);

  auto status = mds_client_->Release(ino, session_id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.{}] release ino({}) fail, error: {}.",
                              name_, ino, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::ReadSlice(Ino ino, uint64_t index,
                                  std::vector<Slice>* slices) {
  auto status = mds_client_->ReadSlice(ino, index, slices);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.{}] ReeadSlice ino({}) fail, error: {}.",
                              name_, ino, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::NewSliceId(Ino ino, uint64_t* id) {
  auto status = mds_client_->NewSliceId(ino, id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.{}] NewSliceId fail, error: {}.", name_,
                              status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::WriteSlice(Ino ino, uint64_t index,
                                   const std::vector<Slice>& slices) {
  auto status = mds_client_->WriteSlice(ino, index, slices);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.{}] WriteSlice ino({}) fail, error: {}.",
                              name_, ino, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::MkDir(Ino parent, const std::string& name, uint32_t uid,
                              uint32_t gid, uint32_t mode, Attr* out_attr) {
  auto status = mds_client_->MkDir(parent, name, uid, gid, mode, 0, *out_attr);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::RmDir(Ino parent, const std::string& name) {
  auto status = mds_client_->RmDir(parent, name);
  if (!status.ok()) {
    if (status.Errno() == pb::error::ENOT_EMPTY) {
      return Status::NotEmpty("dir not empty");
    }
    return status;
  }

  return Status::OK();
}

// TODO: implement
Status MDSV2FileSystem::OpenDir(Ino ino) {
  LOG(INFO) << fmt::format("[meta.{}] OpenDir ino({})", name_, ino);

  return Status::OK();
}

DirIterator* MDSV2FileSystem::NewDirIterator(Ino ino) {
  return new DirIteratorImpl(mds_client_, ino);
}

Status MDSV2FileSystem::Link(Ino ino, Ino new_parent,
                             const std::string& new_name, Attr* attr) {
  auto status = mds_client_->Link(ino, new_parent, new_name, *attr);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.{}] Link({}/{}) to ino({}) fail, error: {}.", name_, new_parent,
        new_name, ino, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Unlink(Ino parent, const std::string& name) {
  auto status = mds_client_->UnLink(parent, name);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.{}] UnLink({}/{}) fail, error: {}.", name_,
                              parent, name, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Symlink(Ino parent, const std::string& name,
                                uint32_t uid, uint32_t gid,
                                const std::string& link, Attr* out_attr) {
  auto status = mds_client_->Symlink(parent, name, uid, gid, link, *out_attr);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.{}] Symlink({}/{}) fail, symlink({}) error: {}.", name_, parent,
        name, link, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::ReadLink(Ino ino, std::string* link) {
  auto status = mds_client_->ReadLink(ino, *link);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.{}] ReadLink {} fail, error: {}.", name_,
                              ino, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::GetAttr(Ino ino, Attr* out_attr) {
  auto status = mds_client_->GetAttr(ino, *out_attr);
  if (!status.ok()) {
    return Status::Internal(
        fmt::format("get attr fail, error: {}", ino, status.ToString()));
  }

  return Status::OK();
}

Status MDSV2FileSystem::SetAttr(Ino ino, int set, const Attr& attr,
                                Attr* out_attr) {
  auto status = mds_client_->SetAttr(ino, attr, set, *out_attr);
  if (!status.ok()) {
    return Status::Internal(fmt::format("set attr fail, ino({}) error: {}", ino,
                                        status.ToString()));
  }

  return Status::OK();
}

Status MDSV2FileSystem::GetXattr(Ino ino, const std::string& name,
                                 std::string* value) {
  if (kXAttrBlackList.find(name) != kXAttrBlackList.end()) {
    return Status::OK();
  }

  auto status = mds_client_->GetXAttr(ino, name, *value);
  if (!status.ok()) {
    return Status::NoData(status.Errno(), status.ToString());
  }

  return Status::OK();
}

Status MDSV2FileSystem::SetXattr(Ino ino, const std::string& name,
                                 const std::string& value, int) {
  auto status = mds_client_->SetXAttr(ino, name, value);
  if (!status.ok()) {
    return Status::Internal(
        fmt::format("set xattr({}/{}) fail, ino({}) error: {}", name, value,
                    ino, status.ToString()));
  }

  return Status::OK();
}

Status MDSV2FileSystem::ListXattr(Ino ino, std::vector<std::string>* xattrs) {
  CHECK(xattrs != nullptr) << "xattrs is null.";

  std::map<std::string, std::string> xattr_map;
  auto status = mds_client_->ListXAttr(ino, xattr_map);
  if (!status.ok()) {
    return Status::Internal(fmt::format("list xattr fail, ino({}) error: {}",
                                        ino, status.ToString()));
  }

  for (auto& [key, _] : xattr_map) {
    xattrs->push_back(key);
  }

  return Status::OK();
}

Status MDSV2FileSystem::Rename(Ino old_parent, const std::string& old_name,
                               Ino new_parent, const std::string& new_name) {
  auto status = mds_client_->Rename(old_parent, old_name, new_parent, new_name);
  if (!status.ok()) {
    if (status.Errno() == pb::error::ENOT_EMPTY) {
      return Status::NotEmpty("dist dir not empty");
    }

    return Status::Internal(
        fmt::format("rename fail, {}/{} -> {}/{}, error: {}", old_parent,
                    old_name, new_parent, new_name, status.ToString()));
  }

  return Status::OK();
}

MDSV2FileSystemUPtr MDSV2FileSystem::Build(const std::string& fs_name,
                                           const std::string& mds_addr,
                                           const std::string& mountpoint) {
  LOG(INFO) << fmt::format(
      "[meta.{}] build filesystem mds_addr: {}, mountpoint: {}.", fs_name,
      mds_addr, mountpoint);

  CHECK(!fs_name.empty()) << "fs_name is empty.";
  CHECK(!mds_addr.empty()) << "mds_addr is empty.";
  CHECK(!mountpoint.empty()) << "mountpoint is empty.";

  std::string hostname = GetHostName();
  if (hostname.empty()) {
    LOG(ERROR) << fmt::format("[meta.{}] get hostname fail.", fs_name);
    return nullptr;
  }

  ClientId client_id(hostname, 0, mountpoint);
  LOG(INFO) << fmt::format("[meta.{}] client_id: {}", fs_name, client_id.ID());

  auto rpc = RPC::New(mds_addr);
  if (!rpc->Init()) {
    LOG(ERROR) << fmt::format("[meta.{}] RPC init fail.", fs_name);
    return nullptr;
  }

  auto mds_discovery = MDSDiscovery::New(rpc);
  if (!mds_discovery->Init()) {
    LOG(ERROR) << fmt::format("[meta.{}] MDSDiscovery init fail.", fs_name);
    return nullptr;
  }

  dingofs::pb::mdsv2::FsInfo pb_fs_info;
  auto status = MDSClient::GetFsInfo(rpc, fs_name, pb_fs_info);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.{}] Get fs info fail.", fs_name);
    return nullptr;
  }

  // parent cache
  auto parent_memo = ParentMemo::New();

  // mds router
  MDSRouterPtr mds_router;
  if (pb_fs_info.partition_policy().type() ==
      dingofs::pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    mds_router = MonoMDSRouter::New(mds_discovery);

  } else if (pb_fs_info.partition_policy().type() ==
             dingofs::pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    mds_router = ParentHashMDSRouter::New(mds_discovery, parent_memo);

  } else {
    LOG(ERROR) << fmt::format(
        "[meta.{}] Not support partition policy type({}).", fs_name,
        dingofs::pb::mdsv2::PartitionType_Name(
            pb_fs_info.partition_policy().type()));
    return nullptr;
  }

  if (!mds_router->Init(pb_fs_info.partition_policy())) {
    LOG(ERROR) << fmt::format("[meta.{}] MDSRouter init fail.", fs_name);
    return nullptr;
  }

  auto fs_info = mdsv2::FsInfo::New(pb_fs_info);

  // create mds client
  auto mds_client = MDSClient::New(client_id, fs_info, parent_memo,
                                   mds_discovery, mds_router, rpc);
  if (!mds_client->Init()) {
    LOG(INFO) << fmt::format("[meta.{}] MDSClient init fail.", fs_name);
    return nullptr;
  }

  // create filesystem
  return MDSV2FileSystem::New(fs_info, client_id, mds_discovery, mds_client);
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs