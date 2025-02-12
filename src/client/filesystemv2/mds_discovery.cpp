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

#include "client/filesystemv2/mds_discovery.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include "fmt/core.h"

namespace dingofs {
namespace client {
namespace filesystem {

MDSDiscovery::MDSDiscovery(mdsv2::CoordinatorClientPtr coordinator_client)
    : coordinator_client_(coordinator_client) {}

bool MDSDiscovery::Init() { return UpdateMDSList(); }

void MDSDiscovery::Destroy() {}

bool MDSDiscovery::GetMDS(int64_t mds_id, mdsv2::MDSMeta& mds_meta) {
  utils::ReadLockGuard lk(lock_);

  auto it = mdses_.find(mds_id);
  if (it == mdses_.end()) {
    return false;
  }

  mds_meta = it->second;

  return true;
}

bool MDSDiscovery::PickFirstMDS(mdsv2::MDSMeta& mds_meta) {
  utils::ReadLockGuard lk(lock_);

  if (mdses_.empty()) {
    return false;
  }

  mds_meta = mdses_.begin()->second;

  return true;
}

std::vector<mdsv2::MDSMeta> MDSDiscovery::GetAllMDS() {
  utils::ReadLockGuard lk(lock_);

  std::vector<mdsv2::MDSMeta> mdses;
  mdses.reserve(mdses_.size());
  for (const auto& [_, mds_meta] : mdses_) {
    mdses.push_back(mds_meta);
  }

  return mdses;
}

bool MDSDiscovery::UpdateMDSList() {
  std::vector<mdsv2::MDSMeta> mdses;
  auto status = coordinator_client_->GetMDSList(mdses);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("get mds list fail, error: {}.",
                              status.error_str());
    return false;
  }

  {
    utils::WriteLockGuard lk(lock_);

    for (const auto& mds : mdses) {
      LOG(INFO) << fmt::format("update mds: {}.", mds.ToString());
      CHECK(mds.ID() != 0) << "mds id is 0.";
      mdses_[mds.ID()] = mds;
    }
  }

  return true;
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs