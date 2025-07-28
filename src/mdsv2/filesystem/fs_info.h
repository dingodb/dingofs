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

#include <cstdint>
#include <memory>
#include <string>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/type.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mdsv2 {

class FsInfo;
using FsInfoPtr = std::shared_ptr<FsInfo>;
using FsInfoUPtr = std::unique_ptr<FsInfo>;

class FsInfo {
 public:
  using DataType = FsInfoType;

  explicit FsInfo(const DataType& fs_info) : fs_info_(fs_info) {}
  ~FsInfo() = default;

  static FsInfoPtr New(const DataType& fs_info) { return std::make_shared<FsInfo>(fs_info); }
  static FsInfoUPtr NewUnique(const DataType& fs_info) { return std::make_unique<FsInfo>(fs_info); }

  DataType Get() {
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

  uint64_t GetBlockSize() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.block_size();
  }

  uint64_t GetChunkSize() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.chunk_size();
  }

  pb::mdsv2::PartitionType GetPartitionType() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.partition_policy().type();
  }

  bool IsMonoPartition() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.partition_policy().type() == pb::mdsv2::MONOLITHIC_PARTITION;
  }

  bool IsHashPartition() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.partition_policy().type() == pb::mdsv2::PARENT_ID_HASH_PARTITION;
  }

  pb::mdsv2::PartitionPolicy GetPartitionPolicy() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.partition_policy();
  }

  uint64_t GetEpoch() {
    utils::ReadLockGuard lock(lock_);
    return fs_info_.partition_policy().epoch();
  }

  void SetPartitionPolicy(const pb::mdsv2::PartitionPolicy& partition_policy) {
    utils::WriteLockGuard lock(lock_);

    fs_info_.mutable_partition_policy()->CopyFrom(partition_policy);
  }

  std::string ToString() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.ShortDebugString();
  }

  void Update(const DataType& fs_info) {
    utils::WriteLockGuard lock(lock_);

    if (fs_info.version() > fs_info_.version()) {
      fs_info_ = fs_info;
    }
  }

 private:
  utils::RWLock lock_;
  DataType fs_info_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDSV2_FS_INFO_H_
