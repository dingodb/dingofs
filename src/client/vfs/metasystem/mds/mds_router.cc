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

#include "client/vfs/metasystem/mds/mds_router.h"

#include <cstdint>

#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "mds/common/helper.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

bool MonoMDSRouter::UpdateMds(int64_t mds_id) {
  CHECK(mds_id > 0) << fmt::format("invalid mds_id({}).", mds_id);

  mds::MDSMeta mds_meta;
  if (!mds_discovery_.GetMDS(mds_id, mds_meta)) {
    return false;
  }

  {
    utils::WriteLockGuard lk(lock_);

    mds_meta_ = mds_meta;
  }

  return true;
}

bool MonoMDSRouter::Init(const pb::mds::PartitionPolicy& partition_policy) {
  CHECK(partition_policy.type() == pb::mds::MONOLITHIC_PARTITION)
      << fmt::format("invalid partition type({}).",
                     pb::mds::PartitionType_Name(partition_policy.type()));

  return UpdateMds(partition_policy.mono().mds_id());
}

bool MonoMDSRouter::GetMDSByParent(Ino, mds::MDSMeta& mds_meta) {
  utils::ReadLockGuard lk(lock_);

  mds_meta = mds_meta_;
  return true;
}

bool MonoMDSRouter::GetMDS(Ino ino, mds::MDSMeta& mds_meta) {  // NOLINT
  utils::ReadLockGuard lk(lock_);

  mds_meta = mds_meta_;
  return true;
}

bool MonoMDSRouter::GetRandomlyMDS(mds::MDSMeta& mds_meta) {
  mds_meta = mds_meta_;
  return true;
}

bool MonoMDSRouter::UpdateRouter(
    const pb::mds::PartitionPolicy& partition_policy) {
  CHECK(partition_policy.type() == pb::mds::MONOLITHIC_PARTITION)
      << fmt::format("invalid partition type({}).",
                     pb::mds::PartitionType_Name(partition_policy.type()));

  return UpdateMds(partition_policy.mono().mds_id());
}

bool MonoMDSRouter::Dump(Json::Value& value) {
  utils::ReadLockGuard lk(lock_);

  Json::Value mds_routers = Json::arrayValue;
  Json::Value item;
  item["bucket_id"] = 0;
  item["id"] = mds_meta_.ID();
  item["host"] = mds_meta_.Host();
  item["port"] = mds_meta_.Port();
  item["state"] = mds_meta_.StateName(mds_meta_.GetState());
  item["last_online_time_ms"] = mds_meta_.LastOnlineTimeMs();
  item["type"] = "mono";
  mds_routers.append(item);
  value["mds_routers"] = mds_routers;

  return true;
}

void ParentHashMDSRouter::UpdateMDSes(
    const pb::mds::HashPartition& hash_partition) {
  utils::WriteLockGuard lk(lock_);

  for (const auto& [mds_id, bucket_set] : hash_partition.distributions()) {
    mds::MDSMeta mds_meta;
    CHECK(mds_discovery_.GetMDS(mds_id, mds_meta))
        << fmt::format("not found mds by mds_id({}).", mds_id);

    for (const auto& bucket_id : bucket_set.bucket_ids()) {
      mds_map_[bucket_id] = mds_meta;
    }
  }

  hash_partition_ = hash_partition;
}

bool ParentHashMDSRouter::Init(
    const pb::mds::PartitionPolicy& partition_policy) {
  CHECK(partition_policy.type() == pb::mds::PARENT_ID_HASH_PARTITION)
      << fmt::format("invalid partition type({}).",
                     pb::mds::PartitionType_Name(partition_policy.type()));

  UpdateMDSes(partition_policy.parent_hash());

  return true;
}

bool ParentHashMDSRouter::GetMDSByParent(Ino parent, mds::MDSMeta& mds_meta) {
  utils::ReadLockGuard lk(lock_);

  int64_t bucket_id = parent % hash_partition_.bucket_num();
  auto it = mds_map_.find(bucket_id);
  CHECK(it != mds_map_.end())
      << fmt::format("not found mds by parent({}).", parent);

  mds_meta = it->second;

  return true;
}

bool ParentHashMDSRouter::GetMDS(Ino ino, mds::MDSMeta& mds_meta) {
  Ino parent = 1;
  if (ino != 1 && !parent_memo_.GetParent(ino, parent)) {
    return false;
  }

  utils::ReadLockGuard lk(lock_);

  int64_t bucket_id = parent % hash_partition_.bucket_num();
  auto it = mds_map_.find(bucket_id);
  CHECK(it != mds_map_.end())
      << fmt::format("not found mds by parent({}).", parent);

  mds_meta = it->second;

  return true;
}

bool ParentHashMDSRouter::GetRandomlyMDS(mds::MDSMeta& mds_meta) {
  utils::ReadLockGuard lk(lock_);

  Ino parent =
      mds::Helper::GenerateRandomInteger(0, hash_partition_.bucket_num());
  int64_t bucket_id = parent % hash_partition_.bucket_num();
  auto it = mds_map_.find(bucket_id);
  CHECK(it != mds_map_.end())
      << fmt::format("not found mds by parent({}).", parent);

  mds_meta = it->second;
  return true;
}

bool ParentHashMDSRouter::UpdateRouter(
    const pb::mds::PartitionPolicy& partition_policy) {
  CHECK(partition_policy.type() == pb::mds::PARENT_ID_HASH_PARTITION)
      << fmt::format("invalid partition type({}).",
                     pb::mds::PartitionType_Name(partition_policy.type()));

  UpdateMDSes(partition_policy.parent_hash());

  return true;
}

bool ParentHashMDSRouter::Dump(Json::Value& value) {
  utils::ReadLockGuard lk(lock_);

  Json::Value mds_routers = Json::arrayValue;
  for (const auto& [bucket_id, mds_meta] : mds_map_) {
    Json::Value item;
    item["bucket_id"] = bucket_id;
    item["id"] = mds_meta.ID();
    item["host"] = mds_meta.Host();
    item["port"] = mds_meta.Port();
    item["state"] = mds_meta.StateName(mds_meta.GetState());
    item["last_online_time_ms"] = mds_meta.LastOnlineTimeMs();
    item["type"] = "parent_hash";
    mds_routers.append(item);
  }
  value["mds_routers"] = mds_routers;

  return true;
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs