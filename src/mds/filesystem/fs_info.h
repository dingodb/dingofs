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

#ifndef DINGOFS_SRC_MDS_FS_INFO_H_
#define DINGOFS_SRC_MDS_FS_INFO_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "dingofs/mds.pb.h"
#include "mds/common/type.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mds {

class FsInfo;
using FsInfoSPtr = std::shared_ptr<FsInfo>;
using FsInfoUPtr = std::unique_ptr<FsInfo>;

class FsInfo {
 public:
  using DataType = FsInfoEntry;

  explicit FsInfo(const DataType& fs_info) : fs_info_(fs_info) {}
  ~FsInfo() = default;

  static FsInfoSPtr New(const DataType& fs_info) { return std::make_shared<FsInfo>(fs_info); }
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

  std::string GetUUID() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.uuid();
  }

  uint64_t GetVersion() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.version();
  }

  uint64_t GetBlockSize() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.block_size();
  }

  uint64_t GetChunkSize() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.chunk_size();
  }

  pb::mds::FsStatus GetStatus() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.status();
  }

  pb::mds::PartitionType GetPartitionType() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.partition_policy().type();
  }

  bool IsMonoPartition() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.partition_policy().type() == pb::mds::MONOLITHIC_PARTITION;
  }

  bool IsHashPartition() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.partition_policy().type() == pb::mds::PARENT_ID_HASH_PARTITION;
  }

  pb::mds::PartitionPolicy GetPartitionPolicy() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.partition_policy();
  }

  uint64_t GetEpoch() {
    utils::ReadLockGuard lock(lock_);
    return fs_info_.partition_policy().epoch();
  }

  void SetPartitionPolicy(const pb::mds::PartitionPolicy& partition_policy) {
    utils::WriteLockGuard lock(lock_);

    fs_info_.mutable_partition_policy()->CopyFrom(partition_policy);
  }

  std::vector<uint64_t> GetMdsIds() {
    utils::ReadLockGuard lock(lock_);

    std::vector<uint64_t> mds_ids;
    if (fs_info_.partition_policy().type() == pb::mds::MONOLITHIC_PARTITION) {
      mds_ids.push_back(fs_info_.partition_policy().mono().mds_id());

    } else {
      const auto& parent_hash = fs_info_.partition_policy().parent_hash();
      for (const auto& [mds_id, _] : parent_hash.distributions()) {
        mds_ids.push_back(mds_id);
      }
    }

    return mds_ids;
  }

  std::string ToString() {
    utils::ReadLockGuard lock(lock_);

    return fs_info_.ShortDebugString();
  }

  bool Update(const DataType& fs_info, std::function<void(const DataType&, const DataType&)> pre_handle = nullptr) {
    utils::WriteLockGuard lock(lock_);

    if (fs_info.version() > fs_info_.version()) {
      if (pre_handle) pre_handle(fs_info_, fs_info);

      fs_info_ = fs_info;
      return true;
    }

    return false;
  }

 private:
  utils::RWLock lock_;
  DataType fs_info_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_FS_INFO_H_
