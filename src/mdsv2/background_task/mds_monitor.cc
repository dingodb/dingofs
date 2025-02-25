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

#include "mdsv2/background_task/mds_monitor.h"

#include <butil/endpoint.h>

#include <atomic>
#include <cstdint>
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/synchronization.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/service/service_access.h"

DEFINE_uint32(mds_offline_period_time_ms, 60 * 1000, "mds offline period time ms");

namespace dingofs {
namespace mdsv2 {

void GetOfflineMDS(const std::vector<MDSMeta>& mdses, std::vector<MDSMeta>& online_mdses,
                   std::vector<MDSMeta>& offline_mdses) {
  int64_t now_ms = Helper::TimestampMs();

  for (const auto& mds : mdses) {
    if (mds.LastOnlineTimeMs() + FLAGS_mds_offline_period_time_ms < now_ms) {
      offline_mdses.push_back(mds);
    } else {
      online_mdses.push_back(mds);
    }
  }
}

bool IsOfflineMDS(const std::vector<MDSMeta>& offline_mdses, int64_t mds_id) {
  for (const auto& offline_mds : offline_mdses) {
    if (mds_id == offline_mds.ID()) {
      return true;
    }
  }

  return false;
}

using BucketSet = pb::mdsv2::HashPartition::BucketSet;

std::map<uint64_t, BucketSet> GetDistributions(const pb::mdsv2::HashPartition& partition) {
  std::map<uint64_t, BucketSet> distributions;
  for (const auto& [mds_id, bucket_set] : partition.distributions()) {
    distributions[mds_id] = bucket_set;
  }

  return distributions;
}

std::map<uint64_t, BucketSet> AdjustParentHashDistribution(const std::map<uint64_t, BucketSet>& distributions,
                                                           std::vector<MDSMeta>& online_mdses,
                                                           std::vector<MDSMeta>& offline_mdses) {
  std::map<uint64_t, BucketSet> new_distributions = distributions;

  std::vector<int64_t> pending_bucket_set;
  for (auto it = new_distributions.begin(); it != new_distributions.end(); ++it) {
    int64_t mds_id = it->first;
    const auto& bucket_set = it->second;
    if (IsOfflineMDS(offline_mdses, mds_id)) {
      pending_bucket_set.insert(pending_bucket_set.end(), bucket_set.bucket_ids().begin(),
                                bucket_set.bucket_ids().end());
      it = new_distributions.erase(it);
    }
  }

  for (const auto& bucket_id : pending_bucket_set) {
    auto mds = online_mdses[Helper::GenerateRandomInteger(0, 1000) % online_mdses.size()];
    new_distributions.at(mds.ID()).add_bucket_ids(bucket_id);
  }

  return new_distributions;
}

bool MDSMonitor::Init() { return dist_lock_->Init(); }

void MDSMonitor::Destroy() { dist_lock_->Destroy(); }

// 1. get mds list
// 2. check mds status
// 3. get fs info
// 4. eliminate dead mds, add new mds
// 5. notify new mds
void MDSMonitor::MonitorMDS() {
  // bool running = false;
  // if (!is_running_.compare_exchange_strong(running, true)) {
  //   DINGO_LOG(INFO) << "monitor mds already running......";
  //   return;
  // }
  // DEFER(is_running_.store(false));

  // if (!dist_lock_->IsLocked()) {
  //   DINGO_LOG(INFO) << "monitor mds not own lock......";
  //   return;
  // }

  // DINGO_LOG(INFO) << "monitor mds running......";

  // std::vector<MDSMeta> mdses;
  // auto status = coordinator_client_->GetMDSList(mdses);
  // if (!status.ok()) {
  //   DINGO_LOG(ERROR) << fmt::format("get mds list fail, {}", status.error_str());
  //   return;
  // }

  // std::vector<MDSMeta> online_mdses, offline_mdses;
  // GetOfflineMDS(mdses, online_mdses, offline_mdses);
  // if (offline_mdses.empty()) {
  //   return;
  // }

  // auto fs_set = fs_set_->GetAllFileSystem();
  // if (fs_set.empty()) {
  //   return;
  // }

  // auto is_offline_func = [offline_mdses](const uint64_t mds_id) -> bool {
  //   for (const auto& offline_mds : offline_mdses) {
  //     if (mds_id == offline_mds.ID()) {
  //       return true;
  //     }
  //   }
  //   return false;
  // };

  // auto pick_mds_func = [online_mdses]() -> MDSMeta {
  //   return online_mdses[Helper::GenerateRandomInteger(0, 1000) % online_mdses.size()];
  // };

  // for (const auto& fs : fs_set) {
  //   auto fs_info = fs->FsInfo();
  //   const auto& partition_policy = fs_info.partition_policy();
  //   if (partition_policy.type() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
  //     if (is_offline_func(partition_policy.mono().mds_id())) {
  //       auto new_mds = pick_mds_func();
  //       auto status = fs->UpdatePartitionPolicy(new_mds.ID());
  //       if (!status.ok()) {
  //         DINGO_LOG(ERROR) << fmt::format("update partition policy fail, fs: {}, error: {}", fs->FsName(),
  //                                         status.error_str());
  //         continue;
  //       }

  //       butil::EndPoint endpoint;
  //       butil::str2endpoint(new_mds.Host().c_str(), new_mds.Port(), &endpoint);
  //       status = ServiceAccess::RefreshFsInfo(endpoint, fs->FsName());
  //       if (!status.ok()) {
  //         DINGO_LOG(ERROR) << fmt::format("refresh fs info fail, fs: {}, error: {}", fs->FsName(),
  //         status.error_str());
  //       }
  //     }

  //   } else if (partition_policy.type() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
  //     auto new_distributions =
  //         AdjustParentHashDistribution(GetDistributions(partition_policy.parent_hash()), online_mdses,
  //         offline_mdses);

  //     fs->UpdatePartitionPolicy(new_distributions);
  //   }
  // }
}

}  // namespace mdsv2
}  // namespace dingofs
