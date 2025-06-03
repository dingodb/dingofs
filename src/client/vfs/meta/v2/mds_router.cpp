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

#include "client/vfs/meta/v2/mds_router.h"

#include <cstdint>

#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

bool MonoMDSRouter::UpdateMds(int64_t mds_id) {
  CHECK(mds_id > 0) << fmt::format("invalid mds_id({}).", mds_id);

  mdsv2::MDSMeta mds_meta;
  if (!mds_discovery_->GetMDS(mds_id, mds_meta)) {
    return false;
  }

  {
    utils::WriteLockGuard lk(lock_);

    mds_meta_ = mds_meta;
  }

  return true;
}

bool MonoMDSRouter::Init(const pb::mdsv2::PartitionPolicy& partition_policy) {
  CHECK(partition_policy.type() == pb::mdsv2::MONOLITHIC_PARTITION)
      << fmt::format("invalid partition type({}).",
                     pb::mdsv2::PartitionType_Name(partition_policy.type()));

  return UpdateMds(partition_policy.mono().mds_id());
}

mdsv2::MDSMeta MonoMDSRouter::GetMDSByParentIno(Ino parent) {  // NOLINT
  utils::ReadLockGuard lk(lock_);

  return mds_meta_;
}

mdsv2::MDSMeta MonoMDSRouter::GetMDSByIno(Ino ino) {  // NOLINT
  utils::ReadLockGuard lk(lock_);

  return mds_meta_;
}

bool MonoMDSRouter::UpdateRouter(
    const pb::mdsv2::PartitionPolicy& partition_policy) {
  CHECK(partition_policy.type() == pb::mdsv2::MONOLITHIC_PARTITION)
      << fmt::format("invalid partition type({}).",
                     pb::mdsv2::PartitionType_Name(partition_policy.type()));

  return UpdateMds(partition_policy.mono().mds_id());
}

void ParentHashMDSRouter::UpdateMDSes(
    const pb::mdsv2::HashPartition& hash_partition) {
  utils::WriteLockGuard lk(lock_);

  for (const auto& [mds_id, bucket_set] : hash_partition.distributions()) {
    mdsv2::MDSMeta mds_meta;
    CHECK(mds_discovery_->GetMDS(mds_id, mds_meta))
        << fmt::format("not found mds by mds_id({}).", mds_id);

    for (const auto& bucket_id : bucket_set.bucket_ids()) {
      mdses_[bucket_id] = mds_meta;
    }
  }

  hash_partition_ = hash_partition;
}

bool ParentHashMDSRouter::Init(
    const pb::mdsv2::PartitionPolicy& partition_policy) {
  CHECK(partition_policy.type() == pb::mdsv2::PARENT_ID_HASH_PARTITION)
      << fmt::format("invalid partition type({}).",
                     pb::mdsv2::PartitionType_Name(partition_policy.type()));

  UpdateMDSes(partition_policy.parent_hash());

  return true;
}

mdsv2::MDSMeta ParentHashMDSRouter::GetMDSByParentIno(Ino parent) {
  utils::ReadLockGuard lk(lock_);

  int64_t bucket_id = parent % hash_partition_.bucket_num();
  auto it = mdses_.find(bucket_id);
  CHECK(it != mdses_.end())
      << fmt::format("not found mds by parent_ino({}).", parent);

  return it->second;
}

mdsv2::MDSMeta ParentHashMDSRouter::GetMDSByIno(Ino ino) {
  Ino parent = 1;
  if (ino != 1) {
    CHECK(parent_cache_->GetParent(ino, parent))
        << fmt::format("not found parent_ino by ino({}).", ino);
  }

  utils::ReadLockGuard lk(lock_);

  int64_t bucket_id = parent % hash_partition_.bucket_num();
  auto it = mdses_.find(bucket_id);
  CHECK(it != mdses_.end())
      << fmt::format("not found mds by parent_ino({}).", parent);

  return it->second;
}

bool ParentHashMDSRouter::UpdateRouter(
    const pb::mdsv2::PartitionPolicy& partition_policy) {
  CHECK(partition_policy.type() == pb::mdsv2::PARENT_ID_HASH_PARTITION)
      << fmt::format("invalid partition type({}).",
                     pb::mdsv2::PartitionType_Name(partition_policy.type()));

  UpdateMDSes(partition_policy.parent_hash());

  return true;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs