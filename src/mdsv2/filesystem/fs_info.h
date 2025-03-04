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

#ifndef DINGOFS_SRC_MDSV2_FS_INFO_H_
#define DINGOFS_SRC_MDSV2_FS_INFO_H_

#include <memory>
#include <string>

#include "dingofs/mdsv2.pb.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mdsv2 {

class FsInfo;
using FsInfoPtr = std::shared_ptr<FsInfo>;
using FsInfoUPtr = std::unique_ptr<FsInfo>;

class FsInfo {
 public:
  explicit FsInfo(const pb::mdsv2::FsInfo& fs_info) : fs_info_(fs_info) {}
  ~FsInfo() = default;

  static FsInfoPtr New(const pb::mdsv2::FsInfo& fs_info) { return std::make_shared<FsInfo>(fs_info); }
  static FsInfoUPtr NewUnique(const pb::mdsv2::FsInfo& fs_info) { return std::make_unique<FsInfo>(fs_info); }

  pb::mdsv2::FsInfo Get() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_;
  }

  uint32_t GetFsId() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.fs_id();
  }

  std::string GetName() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.fs_name();
  }

  pb::mdsv2::PartitionType GetPartitionType() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.partition_policy().type();
  }

  pb::mdsv2::PartitionPolicy GetPartitionPolicy() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.partition_policy();
  }

  void SetPartitionPolicy(const pb::mdsv2::PartitionPolicy& partition_policy) {
    utils::WriteLockGuard lock(lock_);

    fs_info_.mutable_partition_policy()->CopyFrom(partition_policy);
  }

  std::string ToString() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.ShortDebugString();
  }

  void Update(const pb::mdsv2::FsInfo& fs_info) {
    utils::WriteLockGuard lock(lock_);

    if (fs_info.last_update_time_ns() > fs_info_.last_update_time_ns()) {
      fs_info_ = fs_info;
    }
  }

 private:
  utils::RWLock lock_;
  pb::mdsv2::FsInfo fs_info_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDSV2_FS_INFO_H_
