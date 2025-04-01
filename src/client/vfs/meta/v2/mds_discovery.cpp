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

#include "client/vfs/meta/v2/mds_discovery.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

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

std::vector<mdsv2::MDSMeta> MDSDiscovery::GetMDSByState(
    mdsv2::MDSMeta::State state) {
  utils::ReadLockGuard lk(lock_);

  std::vector<mdsv2::MDSMeta> mdses;
  mdses.reserve(mdses_.size());
  for (const auto& [_, mds_meta] : mdses_) {
    if (mds_meta.GetState() == state) {
      mdses.push_back(mds_meta);
    }
  }

  return mdses;
}

Status MDSDiscovery::GetMDSList(std::vector<mdsv2::MDSMeta>& mdses) {
  pb::mdsv2::GetMDSListRequest request;
  pb::mdsv2::GetMDSListResponse response;

  auto status =
      rpc_->SendRequest("MDSService", "GetMDSList", request, response);
  if (!status.ok()) {
    return status;
  }

  mdses.reserve(response.mdses_size());
  for (const auto& mds : response.mdses()) {
    mdses.push_back(mdsv2::MDSMeta(mds));
  }

  return Status::OK();
}

bool MDSDiscovery::UpdateMDSList() {
  std::vector<mdsv2::MDSMeta> mdses;
  auto status = GetMDSList(mdses);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("get mds list fail, error: {}.",
                              status.ToString());
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

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs