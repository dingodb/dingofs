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

#include <fmt/format.h>

#include <cstdint>
#include <string>
#include <vector>

#include "client/common/status.h"
#include "client/vfs/meta/v2/client_id.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

const uint32_t kMaxHostNameLength = 255;

const uint32_t kMaxXAttrNameLength = 255;
const uint32_t kMaxXAttrValueLength = 64 * 1024;

const std::set<std::string> kXAttrBlackList = {
    "system.posix_acl_access", "system.posix_acl_default", "system.nfs4_acl"};

DEFINE_uint32(read_dir_batch_size, 1024, "Read dir batch size.");

std::string GetHostName() {
  char hostname[kMaxHostNameLength];
  int ret = gethostname(hostname, kMaxHostNameLength);
  if (ret < 0) {
    LOG(ERROR) << "GetHostName fail, ret=" << ret;
    return "";
  }

  return std::string(hostname);
}

Status MdsV2DirIterator::Seek() {
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

bool MdsV2DirIterator::Valid() { return offset_ < entries_.size(); }

DirEntry MdsV2DirIterator::GetValue(bool with_attr) {
  CHECK(offset_ < entries_.size()) << "offset out of range";

  with_attr_ = with_attr;
  return entries_[offset_];
}

void MdsV2DirIterator::Next() {
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
  LOG(INFO) << fmt::format("fs_info: {}.", fs_info_->ToString());
  // mount fs
  if (!MountFs()) {
    LOG(ERROR) << fmt::format("mount fs({}) fail.", name_);
    return Status::MountFailed("mount fs fail");
  }

  return Status::OK();
}

void MDSV2FileSystem::UnInit() {
  // unmount fs
  UnmountFs();
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

Status MDSV2FileSystem::GetFsInfo(FsInfo* fs_info) {
  auto temp_fs_info = fs_info_->Get();

  fs_info->name = name_;
  fs_info->id = temp_fs_info.fs_id();
  fs_info->chunk_size = temp_fs_info.chunk_size();
  fs_info->block_size = temp_fs_info.block_size();
  fs_info->store_type = ToStoreType(temp_fs_info.fs_type());
  fs_info->uuid = temp_fs_info.uuid();

  return Status::OK();
}

Status MDSV2FileSystem::GetS3Info(S3Info* s3_info) {
  auto temp_fs_info = fs_info_->Get();
  auto temp_s3_info = temp_fs_info.extra().s3_info();

  s3_info->ak = temp_s3_info.ak();
  s3_info->sk = temp_s3_info.sk();
  s3_info->endpoint = temp_s3_info.endpoint();
  s3_info->bucket = temp_s3_info.bucketname();

  return Status::OK();
}

bool MDSV2FileSystem::MountFs() {
  pb::mdsv2::MountPoint mount_point;
  mount_point.set_hostname(client_id_.Hostname());
  mount_point.set_path(client_id_.Mountpoint());
  mount_point.set_cto(false);

  LOG(INFO) << fmt::format("mount point: {}.", mount_point.ShortDebugString());

  auto status = mds_client_->MountFs(name_, mount_point);
  if (!status.ok() && status.Errno() != pb::error::EEXISTED) {
    LOG(ERROR) << fmt::format("mount fs({}) info fail, mountpoint({}), {}.",
                              name_, client_id_.Mountpoint(),
                              status.ToString());
    return false;
  }

  return true;
}

bool MDSV2FileSystem::UnmountFs() {
  pb::mdsv2::MountPoint mount_point;
  mount_point.set_hostname(client_id_.Hostname());
  mount_point.set_port(client_id_.Port());
  mount_point.set_path(client_id_.Mountpoint());

  auto status = mds_client_->UmountFs(name_, mount_point);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("mount fs({}) info fail, mountpoint({}).", name_,
                              client_id_.Mountpoint());
    return false;
  }

  return true;
}

Status MDSV2FileSystem::StatFs(Ino ino, FsStat* fs_stat) {  // NOLINT
  fs_stat->max_bytes = 500 * 1000 * 1000 * 1000ul;
  fs_stat->used_bytes = 20 * 1000 * 1000 * 1000ul;
  fs_stat->used_inodes = 100;
  fs_stat->max_inodes = 10000;

  return Status::OK();
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
                               int flags, Attr* attr) {
  auto status = MkNod(parent, name, uid, gid, mode, 0, attr);
  if (!status.ok()) {
    return status;
  }

  return Open(attr->ino, flags);
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

Status MDSV2FileSystem::Open(Ino ino, int flags) {
  LOG(INFO) << fmt::format("Open ino({}).", ino);

  auto status = mds_client_->Open(ino);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Open ino({}) fail, error: {}.", ino,
                              status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Close(Ino ino) {
  LOG(INFO) << fmt::format("Release ino({}).", ino);
  auto status = mds_client_->Release(ino);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Release ino({}) fail, error: {}.", ino,
                              status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::ReadSlice(Ino ino, uint64_t index,
                                  std::vector<Slice>* slices) {
  auto status = mds_client_->ReadSlice(ino, index, slices);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("ReeadSlice ino({}) fail, error: {}.", ino,
                              status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::NewSliceId(uint64_t* id) {
  auto status = mds_client_->NewSliceId(id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("NewSliceId fail, error: {}.", status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::WriteSlice(Ino ino, uint64_t index,
                                   const std::vector<Slice>& slices) {
  auto status = mds_client_->WriteSlice(ino, index, slices);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("WriteSlice ino({}) fail, error: {}.", ino,
                              status.ToString());
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
  LOG(INFO) << fmt::format("OpenDir ino({})", ino);

  return Status::OK();
}

DirIterator* MDSV2FileSystem::NewDirIterator(Ino ino) {
  return new MdsV2DirIterator(mds_client_, ino);
}

Status MDSV2FileSystem::Link(Ino ino, Ino new_parent,
                             const std::string& new_name, Attr* attr) {
  auto status = mds_client_->Link(ino, new_parent, new_name, *attr);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Link({}/{}) to ino({}) fail, error: {}.",
                              new_parent, new_name, ino, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Unlink(Ino parent, const std::string& name) {
  auto status = mds_client_->UnLink(parent, name);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("UnLink({}/{}) fail, error: {}.", parent, name,
                              status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Symlink(Ino parent, const std::string& name,
                                uint32_t uid, uint32_t gid,
                                const std::string& link, Attr* out_attr) {
  auto status = mds_client_->Symlink(parent, name, uid, gid, link, *out_attr);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Symlink({}/{}) fail, symlink({}) error: {}.",
                              parent, name, link, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::ReadLink(Ino ino, std::string* link) {
  auto status = mds_client_->ReadLink(ino, *link);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("ReadLink {} fail, error: {}.", ino,
                              status.ToString());
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
    // LOG(WARNING) << fmt::format("xattr({}) is in black list.", name);
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
  LOG(INFO) << fmt::format("fs_name: {}, mds_addr: {}, mountpoint: {}.",
                           fs_name, mds_addr, mountpoint);

  CHECK(!fs_name.empty()) << "fs_name is empty.";
  CHECK(!mds_addr.empty()) << "mds_addr is empty.";
  CHECK(!mountpoint.empty()) << "mountpoint is empty.";

  std::string hostname = GetHostName();
  if (hostname.empty()) {
    LOG(ERROR) << "get hostname fail.";
    return nullptr;
  }

  ClientId client_id(hostname, 0, mountpoint);
  LOG(INFO) << fmt::format("client_id: {}", client_id.ID());

  auto rpc = RPC::New(mds_addr);
  if (!rpc->Init()) {
    LOG(ERROR) << "RPC init fail.";
    return nullptr;
  }

  auto mds_discovery = MDSDiscovery::New(rpc);
  if (!mds_discovery->Init()) {
    LOG(ERROR) << "MDSDiscovery init fail.";
    return nullptr;
  }

  dingofs::pb::mdsv2::FsInfo pb_fs_info;
  auto status = MDSClient::GetFsInfo(rpc, fs_name, pb_fs_info);
  if (!status.ok()) {
    LOG(ERROR) << "Get fs info fail.";
    return nullptr;
  }

  // parent cache
  auto parent_cache = ParentCache::New();

  // mds router
  MDSRouterPtr mds_router;
  if (pb_fs_info.partition_policy().type() ==
      dingofs::pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    mds_router = MonoMDSRouter::New(mds_discovery);

  } else if (pb_fs_info.partition_policy().type() ==
             dingofs::pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    mds_router = ParentHashMDSRouter::New(mds_discovery, parent_cache);

  } else {
    LOG(ERROR) << fmt::format("Not support partition policy type({}).",
                              dingofs::pb::mdsv2::PartitionType_Name(
                                  pb_fs_info.partition_policy().type()));
    return nullptr;
  }

  if (!mds_router->Init(pb_fs_info.partition_policy())) {
    LOG(ERROR) << "MDSRouter init fail.";
    return nullptr;
  }

  auto fs_info = mdsv2::FsInfo::New(pb_fs_info);

  // create mds client
  auto mds_client = MDSClient::New(client_id, fs_info, parent_cache,
                                   mds_discovery, mds_router, rpc);
  if (!mds_client->Init()) {
    LOG(INFO) << "MDSClient init fail.";
    return nullptr;
  }

  // create filesystem
  return MDSV2FileSystem::New(fs_info, client_id, mds_discovery, mds_client);
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs