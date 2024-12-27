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

#include "curvefs/src/mdsv2/filesystem/filesystem.h"

#include <gflags/gflags.h>
#include <sys/stat.h>

#include <cstdint>
#include <string>

#include "curvefs/src/mdsv2/common/helper.h"
#include "curvefs/src/mdsv2/common/logging.h"
#include "curvefs/src/mdsv2/common/status.h"
#include "curvefs/src/mdsv2/filesystem/codec.h"
#include "fmt/core.h"

namespace dingofs {
namespace mdsv2 {

static const uint64_t kRootInodeId = 1;

static const std::string kFsTableName = "dingofs";

bool FileSystem::Init() {
  if (IsExistFsTable()) {
    return true;
  }

  auto status = CreateFsTable();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "create fs table fail, error: " << status.error_str();
    return false;
  }

  return true;
}

Status FileSystem::CreateFsTable() {
  int64_t table_id = 0;
  KVStorage::TableOption option;
  MetaDataCodec::GetFsTableRange(option.start_key, option.end_key);
  DINGO_LOG(INFO) << fmt::format("Create fs table, start_key({}), end_key({}).", Helper::StringToHex(option.start_key),
                                 Helper::StringToHex(option.end_key));
  return kv_storage_->CreateTable(kFsTableName, option, table_id);
}

bool FileSystem::IsExistFsTable() {
  std::string start_key, end_key;
  MetaDataCodec::GetFsTableRange(start_key, end_key);
  DINGO_LOG(INFO) << fmt::format("Check fs table, start_key({}), end_key({}).", Helper::StringToHex(start_key),
                                 Helper::StringToHex(end_key));
  auto status = kv_storage_->IsExistTable(start_key, end_key);
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      DINGO_LOG(ERROR) << "check fs table exist fail, error: " << status.error_str();
    }
    return false;
  }

  return true;
}

// todo: create fs/dentry/inode table
Status FileSystem::CreateFs(const pb::mds::FsInfo& fs_info) {
  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_info.fs_name());
  // check fs exist
  {
    std::string value;
    Status status = kv_storage_->Get(fs_key, value);
    if (!status.ok()) {
      return Status(pb::error::EINTERNAL, "Get fs info fail");
    }
  }

  // create fs
  KVStorage::WriteOption option;
  Status status = kv_storage_->Put(option, fs_key, fs_info.SerializeAsString());
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, "Put fs info fail");
  }

  // create root inode
  {
    pb::mds::Inode inode;
    inode.set_fs_id(fs_info.fs_id());
    inode.set_inode_id(kRootInodeId);
    inode.set_length(0);

    uint64_t now_ns = Helper::TimestampNs();
    inode.set_ctime(now_ns);
    inode.set_mtime(now_ns);
    inode.set_atime(now_ns);

    inode.set_uid(0);
    inode.set_gid(0);
    inode.set_mode(S_IFDIR | 01777);
    inode.set_nlink(1);
    inode.set_type(pb::mds::FileType::DIRECTORY);
    inode.set_rdev(0);

    std::string key = MetaDataCodec::EncodeDirInodeKey(inode.fs_id(), inode.inode_id());
    std::string value = inode.SerializeAsString();
    Status status = kv_storage_->Put(option, key, value);
    if (!status.ok()) {
      kv_storage_->Delete(fs_key);
      return Status(pb::error::EINTERNAL, "Put root inode info fail");
    }
  }

  return Status::OK();
}

bool IsExistMountPoint(const pb::mds::FsInfo& fs_info, const pb::mds::MountPoint& mount_point) {
  for (const auto& mp : fs_info.mount_points()) {
    if (mp.path() == mount_point.path() && mp.hostname() == mount_point.hostname()) {
      return true;
    }
  }

  return false;
}

Status FileSystem::MountFs(const std::string& fs_name, const pb::mds::MountPoint& mount_point) {
  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("Not found fs({}).", fs_name));
  }

  pb::mds::FsInfo fs_info;
  CHECK(fs_info.ParseFromString(value)) << "Parse fs info fail.";

  if (IsExistMountPoint(fs_info, mount_point)) {
    return Status(pb::error::EEXISTED, "MountPoint already exist.");
  }

  fs_info.add_mount_points()->CopyFrom(mount_point);
  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, fs_key, fs_info.SerializeAsString());
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, "Put root inode info fail");
  }

  return Status::OK();
}

void RemoveMountPoint(pb::mds::FsInfo& fs_info, const pb::mds::MountPoint& mount_point) {
  for (int i = 0; i < fs_info.mount_points_size(); i++) {
    if (fs_info.mount_points(i).path() == mount_point.path() &&
        fs_info.mount_points(i).hostname() == mount_point.hostname()) {
      fs_info.mutable_mount_points()->SwapElements(i, fs_info.mount_points_size() - 1);
      fs_info.mutable_mount_points()->RemoveLast();
      return;
    }
  }
}

Status FileSystem::UmountFs(const std::string& fs_name, const pb::mds::MountPoint& mount_point) {
  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("Not found fs({}).", fs_name));
  }

  pb::mds::FsInfo fs_info;
  CHECK(fs_info.ParseFromString(value)) << "Parse fs info fail.";

  RemoveMountPoint(fs_info, mount_point);

  KVStorage::WriteOption option;
  status = kv_storage_->Put(option, fs_key, fs_info.SerializeAsString());
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, "Put fs fail");
  }

  return Status::OK();
}

// check if fs is mounted
// rename fs name to oldname+"_deleting"
Status FileSystem::DeleteFs(const std::string& fs_name) {
  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("Not found fs({}).", fs_name));
  }

  pb::mds::FsInfo fs_info;
  CHECK(fs_info.ParseFromString(value)) << "Parse fs info fail.";

  if (fs_info.mount_points_size() > 0) {
    return Status(pb::error::EEXISTED, "Fs exist mount point.");
  }

  status = kv_storage_->Delete(fs_key);
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, "Delete fs fail");
  }

  KVStorage::WriteOption option;
  std::string delete_fs_name = fmt::format("{}_deleting", fs_name);
  status = kv_storage_->Put(option, MetaDataCodec::EncodeFSKey(delete_fs_name), fs_info.SerializeAsString());
  if (!status.ok()) {
    return Status(pb::error::EINTERNAL, "Put fs fail");
  }

  return Status::OK();
}

Status FileSystem::GetFsInfo(const std::string& fs_name, pb::mds::FsInfo& fs_info) {
  std::string fs_key = MetaDataCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage_->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("Not found fs({}).", fs_name));
  }

  CHECK(fs_info.ParseFromString(value)) << "Parse fs info fail.";

  return Status::OK();
}

}  // namespace mdsv2
}  // namespace dingofs
