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

#include "client/filesystemv2/mds_router.h"

#include <cstdint>

#include "fmt/core.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace filesystem {

bool MonoMDSRouter::Init() { return true; }

mdsv2::MDSMeta MonoMDSRouter::GetMDSByParentIno(int64_t parent_ino) {  // NOLINT
  utils::ReadLockGuard lk(lock_);

  return mds_meta_;
}

mdsv2::MDSMeta MonoMDSRouter::GetMDSByIno(int64_t ino) {  // NOLINT
  utils::ReadLockGuard lk(lock_);

  return mds_meta_;
}

void MonoMDSRouter::UpdateMDS(const mdsv2::MDSMeta& mds_meta) {
  utils::WriteLockGuard lk(lock_);

  mds_meta_ = mds_meta;
}

bool ParentHashMDSRouter::Init() {
  for (const auto& [mds_id, bucket_set] : hash_partition_.distributions()) {
    mdsv2::MDSMeta mds_meta;
    CHECK(mds_discovery_->GetMDS(mds_id, mds_meta))
        << fmt::format("not found mds by mds_id({}).", mds_id);

    for (const auto& bucket_id : bucket_set.bucket_ids()) {
      mdses_[bucket_id] = mds_meta;
    }
  }

  return true;
}

mdsv2::MDSMeta ParentHashMDSRouter::GetMDSByParentIno(int64_t parent_ino) {
  utils::ReadLockGuard lk(lock_);

  int64_t bucket_id = parent_ino % hash_partition_.bucket_num();
  auto it = mdses_.find(bucket_id);
  CHECK(it != mdses_.end())
      << fmt::format("not found mds by parent_ino({}).", parent_ino);

  return it->second;
}

mdsv2::MDSMeta ParentHashMDSRouter::GetMDSByIno(int64_t ino) {
  int64_t parent_ino = 1;
  if (ino != 1) {
    CHECK(parent_cache_->Get(ino, parent_ino))
        << fmt::format("not found parent_ino by ino({}).", ino);
  }

  utils::ReadLockGuard lk(lock_);

  int64_t bucket_id = parent_ino % hash_partition_.bucket_num();
  auto it = mdses_.find(bucket_id);
  CHECK(it != mdses_.end())
      << fmt::format("not found mds by parent_ino({}).", parent_ino);

  return it->second;
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs