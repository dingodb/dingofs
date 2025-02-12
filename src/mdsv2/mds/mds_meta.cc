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

#include "mdsv2/mds/mds_meta.h"

#include <cstdint>
#include <vector>

#include "fmt/core.h"

namespace dingofs {
namespace mdsv2 {

MDSMeta::MDSMeta(const MDSMeta& mds_meta) {
  id_ = mds_meta.id_;
  host_ = mds_meta.host_;
  port_ = mds_meta.port_;
  state_ = mds_meta.state_;
  register_time_ms_ = mds_meta.register_time_ms_;
  last_online_time_ms_ = mds_meta.last_online_time_ms_;
}

std::string MDSMeta::ToString() const {
  return fmt::format("MDSMeta[id={}, host={}, port={}, state={}, register_time_ms={}, last_online_time_ms={}]", id_,
                     host_, port_, static_cast<int>(state_), register_time_ms_, last_online_time_ms_);
}

void MDSMetaMap::UpsertMDSMeta(const MDSMeta& mds_meta) {
  utils::WriteLockGuard lk(lock_);

  mds_meta_map_[mds_meta.ID()] = mds_meta;
}

void MDSMetaMap::DeleteMDSMeta(int64_t id) {
  utils::WriteLockGuard lk(lock_);

  mds_meta_map_.erase(id);
}

bool MDSMetaMap::GetMDSMeta(int64_t id, MDSMeta& mds_meta) {
  utils::ReadLockGuard lk(lock_);

  auto it = mds_meta_map_.find(id);
  if (it == mds_meta_map_.end()) {
    return false;
  }

  mds_meta = it->second;
  return true;
}

std::vector<MDSMeta> MDSMetaMap::GetAllMDSMeta() {
  utils::ReadLockGuard lk(lock_);

  std::vector<MDSMeta> mds_metas;
  mds_metas.reserve(mds_meta_map_.size());
  for (const auto& [id, mds_meta] : mds_meta_map_) {
    mds_metas.push_back(mds_meta);
  }

  return mds_metas;
}

}  // namespace mdsv2
}  // namespace dingofs
