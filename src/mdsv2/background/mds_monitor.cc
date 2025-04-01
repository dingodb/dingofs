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

#include "mdsv2/background/mds_monitor.h"

#include <atomic>
#include <cstdint>
#include <vector>

#include "butil/endpoint.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/synchronization.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/server.h"
#include "mdsv2/service/service_access.h"

namespace dingofs {
namespace mdsv2 {

DEFINE_uint32(mds_offline_period_time_ms, 30 * 1000, "mds offline period time ms");

void GetOfflineMDS(const std::vector<MDSMeta>& mdses, std::vector<MDSMeta>& online_mdses,
                   std::vector<MDSMeta>& offline_mdses) {
  int64_t now_ms = Helper::TimestampMs();

  for (const auto& mds : mdses) {
    // DINGO_LOG(INFO) << fmt::format("[monitor] mds: {}, last online time: {}, now: {}, offline period: {}", mds.ID(),
    //   mds.LastOnlineTimeMs(), now_ms, FLAGS_mds_offline_period_time_ms);
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

static std::vector<uint64_t> GetMdsIds(const pb::mdsv2::HashPartition& partition) {
  std::vector<uint64_t> mds_ids;
  mds_ids.reserve(partition.distributions().size());

  for (const auto& [mds_id, bucket_set] : partition.distributions()) {
    mds_ids.push_back(mds_id);
  }

  return mds_ids;
}

static std::vector<uint64_t> GetMdsIds(const std::map<uint64_t, BucketSet>& distributions) {
  std::vector<uint64_t> mds_ids;
  mds_ids.reserve(distributions.size());

  for (const auto& [mds_id, bucket_set] : distributions) {
    mds_ids.push_back(mds_id);
  }

  return mds_ids;
}

static std::vector<MDSMeta> GetMdsMetas(const std::vector<MDSMeta>& src_mds_metas,
                                        const std::vector<uint64_t>& mds_ids) {
  std::vector<MDSMeta> mds_metas;
  mds_metas.reserve(mds_ids.size());

  for (const auto& mds_id : mds_ids) {
    for (const auto& mds_meta : src_mds_metas) {
      if (mds_meta.ID() == mds_id) {
        mds_metas.push_back(mds_meta);
        break;
      }
    }
  }

  return mds_metas;
}

static std::map<uint64_t, BucketSet> GetDistributions(const pb::mdsv2::HashPartition& partition) {
  std::map<uint64_t, BucketSet> distributions;
  for (const auto& [mds_id, bucket_set] : partition.distributions()) {
    distributions[mds_id] = bucket_set;
  }

  return distributions;
}

static std::map<uint64_t, BucketSet> AdjustParentHashDistribution(const std::map<uint64_t, BucketSet>& distributions,
                                                                  const std::vector<MDSMeta>& online_mdses,
                                                                  const std::vector<MDSMeta>& offline_mdses) {
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
    const auto& mds = online_mdses[Helper::GenerateRandomInteger(0, 1000) % online_mdses.size()];
    new_distributions.at(mds.ID()).add_bucket_ids(bucket_id);
  }

  return new_distributions;
}

bool MDSMonitor::Init() { return dist_lock_->Init(); }

void MDSMonitor::Destroy() { dist_lock_->Destroy(); }

void MDSMonitor::Run() {
  DINGO_LOG(INFO) << "[monitor] monitor mds start......";
  auto status = MonitorMDS();
  DINGO_LOG(INFO) << fmt::format("[monitor] monitor mds finish, {}.", status.error_str());
}

void MDSMonitor::NotifyRefreshFs(const MDSMeta& mds, const std::string& fs_name) {
  butil::EndPoint endpoint;
  butil::str2endpoint(mds.Host().c_str(), mds.Port(), &endpoint);
  auto status = ServiceAccess::RefreshFsInfo(endpoint, fs_name);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[monitor] refresh fs info fail, fs: {}, error: {}", fs_name, status.error_str());
  }
}

void MDSMonitor::NotifyRefreshFs(const std::vector<MDSMeta>& mdses, const std::string& fs_name) {
  for (const auto& mds : mdses) {
    NotifyRefreshFs(mds, fs_name);
  }
}

void CheckMdsAlive(std::vector<MDSMeta>& offline_mdses, std::vector<MDSMeta>& online_mdses) {
  for (auto it = offline_mdses.begin(); it != offline_mdses.end();) {
    auto& mds_meta = *it;

    butil::EndPoint endpoint;
    butil::str2endpoint(mds_meta.Host().c_str(), mds_meta.Port(), &endpoint);

    auto status = ServiceAccess::CheckAlive(endpoint);
    if (status.ok()) {
      online_mdses.push_back(mds_meta);
      it = offline_mdses.erase(it);
    } else {
      ++it;
    }
  }
}

// 1. get mds list
// 2. check mds status
// 3. get fs info
// 4. eliminate dead mds, add new mds
// 5. notify new mds
Status MDSMonitor::MonitorMDS() {
  bool running = false;
  if (!is_running_.compare_exchange_strong(running, true)) {
    DINGO_LOG(INFO) << "[monitor] mds already running......";
    return Status::OK();
  }
  DEFER(is_running_.store(false));

  auto& server = Server::GetInstance();
  auto heartbeat = server.GetHeartbeat();
  auto mds_meta_map = Server::GetInstance().GetMDSMetaMap();

  // get all mds meta
  std::vector<MDSMeta> mdses;
  auto status = heartbeat->GetMDSList(mdses);
  if (!status.ok()) {
    return Status(status.error_code(), fmt::format("get mds list fail, {}", status.error_str()));
  }

  if (mdses.empty()) {
    return Status(pb::error::EINTERNAL, "mds list is empty");
  }

  for (const auto& mds_meta : mdses) {
    DINGO_LOG(DEBUG) << "[monitor] upsert mds meta: " << mds_meta.ToString();
    mds_meta_map->UpsertMDSMeta(mds_meta);
  }

  if (!dist_lock_->IsLocked()) {
    return Status(pb::error::EINTERNAL, "not own lock");
  }

  // just own lock can process fault mds
  return ProcessFaultMDS(mdses);
}

Status MDSMonitor::ProcessFaultMDS(std::vector<MDSMeta>& mdses) {
  auto fs_set = fs_set_->GetAllFileSystem();
  if (fs_set.empty()) {
    return Status::OK();
  }

  std::vector<MDSMeta> online_mdses, offline_mdses;
  GetOfflineMDS(mdses, online_mdses, offline_mdses);

  // check mds offline again
  CheckMdsAlive(offline_mdses, online_mdses);
  DINGO_LOG(INFO) << fmt::format("[monitor] online mdses: {}, offline mdses: {}", online_mdses.size(),
                                 offline_mdses.size());

  if (offline_mdses.empty()) {
    return Status(pb::error::EINTERNAL, "not has offline mds");
  }
  if (online_mdses.empty()) {
    return Status(pb::error::EINTERNAL, "not has online mds");
  }

  auto is_offline_func = [&offline_mdses](const uint64_t mds_id) -> bool {
    for (const auto& offline_mds : offline_mdses) {
      if (mds_id == offline_mds.ID()) {
        return true;
      }
    }
    return false;
  };

  auto is_offlines_func = [&offline_mdses](const std::vector<uint64_t>& mds_ids) -> bool {
    for (const auto& offline_mds : offline_mdses) {
      for (auto mds_id : mds_ids) {
        if (mds_id == offline_mds.ID()) {
          return true;
        }
      }
    }
    return false;
  };

  auto pick_mds_func = [&online_mdses]() -> MDSMeta {
    return online_mdses[Helper::GenerateRandomInteger(0, 1000) % online_mdses.size()];
  };

  for (const auto& fs : fs_set) {
    auto fs_info = fs->FsInfo();
    const auto& partition_policy = fs_info.partition_policy();
    if (partition_policy.type() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
      if (is_offline_func(partition_policy.mono().mds_id())) {
        auto new_mds = pick_mds_func();
        auto status = fs->UpdatePartitionPolicy(new_mds.ID());
        if (!status.ok()) {
          DINGO_LOG(ERROR) << fmt::format("[monitor] transfer fs({}) from mds({}) to mds({}) fail, {}.", fs->FsName(),
                                          partition_policy.mono().mds_id(), new_mds.ID(), status.error_str());
          continue;
        }

        DINGO_LOG(INFO) << fmt::format("[monitor] transfer fs({}) from mds({}) to mds({}) finish.", fs->FsName(),
                                       partition_policy.mono().mds_id(), new_mds.ID());

        NotifyRefreshFs(new_mds, fs->FsName());
      }

    } else if (partition_policy.type() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
      auto mds_ids = GetMdsIds(partition_policy.parent_hash());
      if (is_offlines_func(mds_ids)) {
        auto new_distributions =
            AdjustParentHashDistribution(GetDistributions(partition_policy.parent_hash()), online_mdses, offline_mdses);

        fs->UpdatePartitionPolicy(new_distributions);

        // notify new mds to start serve partition
        auto mds_metas = GetMdsMetas(mdses, GetMdsIds(new_distributions));
        NotifyRefreshFs(mds_metas, fs->FsName());
      }
    }
  }

  return Status::OK();
}

}  // namespace mdsv2
}  // namespace dingofs
