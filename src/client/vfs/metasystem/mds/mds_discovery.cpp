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

#include "client/vfs/metasystem/mds/mds_discovery.h"

#include <cstdint>

#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

static const uint32_t kWaitTimeMs = 100;

bool MDSDiscovery::Init() { return RefreshFullyMDSList(); }

void MDSDiscovery::Destroy() {}

bool MDSDiscovery::GetMDS(int64_t mds_id, mds::MDSMeta& mds_meta) {
  utils::ReadLockGuard lk(lock_);

  auto it = mdses_.find(mds_id);
  if (it == mdses_.end()) {
    return false;
  }

  mds_meta = it->second;

  return true;
}

void MDSDiscovery::PickFirstMDS(mds::MDSMeta& mds_meta) {
  do {
    {
      utils::ReadLockGuard lk(lock_);

      for (auto& [_, mds] : mdses_) {
        if (mds.GetState() == mds::MDSMeta::State::kNormal) {
          mds_meta = mds;
          return;
        }
      }
    }

    LOG(INFO) << "[meta.discovery] not pick normal mds, try refresh mds list.";

    RefreshFullyMDSList();

  } while (true);
}

std::vector<mds::MDSMeta> MDSDiscovery::GetAllMDS() {
  utils::ReadLockGuard lk(lock_);

  std::vector<mds::MDSMeta> mdses;
  mdses.reserve(mdses_.size());
  for (const auto& [_, mds_meta] : mdses_) {
    mdses.push_back(mds_meta);
  }

  return mdses;
}

std::vector<mds::MDSMeta> MDSDiscovery::GetMDSByState(
    mds::MDSMeta::State state) {
  utils::ReadLockGuard lk(lock_);

  std::vector<mds::MDSMeta> mdses;
  mdses.reserve(mdses_.size());
  for (const auto& [_, mds_meta] : mdses_) {
    if (mds_meta.GetState() == state) {
      mdses.push_back(mds_meta);
    }
  }

  return mdses;
}

std::vector<mds::MDSMeta> MDSDiscovery::GetNormalMDS(bool force) {
  for (;;) {
    auto mdses = GetMDSByState(mds::MDSMeta::State::kNormal);
    if (!force) return mdses;
    if (!mdses.empty()) return mdses;

    RefreshFullyMDSList();
  }
}

Status MDSDiscovery::GetMDSList(std::vector<mds::MDSMeta>& mdses) {
  pb::mds::GetMDSListRequest request;
  pb::mds::GetMDSListResponse response;

  request.mutable_info()->set_request_id(
      std::to_string(mds::Helper::TimestampNs()));

  auto status =
      rpc_->SendRequest("MDSService", "GetMDSList", request, response);
  if (!status.ok()) {
    return status;
  }

  mdses.reserve(response.mdses_size());
  for (const auto& mds : response.mdses()) {
    mdses.push_back(mds::MDSMeta(mds));
  }

  return Status::OK();
}

void MDSDiscovery::SetAbnormalMDS(int64_t mds_id) {
  utils::WriteLockGuard lk(lock_);

  auto it = mdses_.find(mds_id);
  if (it != mdses_.end()) {
    it->second.SetState(mds::MDSMeta::State::kAbnormal);
  }
}

bool MDSDiscovery::RefreshFullyMDSList() {
  uint64_t retries = 0;
  std::vector<mds::MDSMeta> mdses;
  for (;;) {
    auto status = GetMDSList(mdses);

    LOG(INFO) << fmt::format(
        "[meta.discovery] get mds list finish, retries({}) error({}).", retries,
        status.ToString());

    if (!mdses.empty()) break;

    bthread_usleep(kWaitTimeMs * 1000);
    ++retries;
  }

  {
    utils::WriteLockGuard lk(lock_);

    mdses_.clear();
    for (const auto& mds : mdses) {
      LOG(INFO) << fmt::format("[meta.discovery] update mds: {}.",
                               mds.ToString());
      CHECK(mds.ID() != 0) << "mds id is 0.";
      mdses_[mds.ID()] = mds;
    }
  }

  for (auto& mds : mdses) {
    rpc_->AddFallbackEndpoint(StrToEndpoint(mds.Host(), mds.Port()));
  }

  return true;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs