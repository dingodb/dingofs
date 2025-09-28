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

#include "mds/background/monitor.h"

#include <atomic>
#include <cstdint>
#include <vector>

#include "butil/endpoint.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/common/partition_helper.h"
#include "mds/common/status.h"
#include "mds/common/synchronization.h"
#include "mds/mds/mds_helper.h"
#include "mds/mds/mds_meta.h"
#include "mds/server.h"
#include "mds/service/service_access.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_heartbeat_mds_offline_period_time_ms);

DEFINE_uint32(mds_monitor_client_clean_period_time_s, 180 * 1000, "client clean period time ms");
DEFINE_validator(mds_monitor_client_clean_period_time_s, brpc::PassValidate);

static void GetOfflineMDS(const std::vector<MDSMeta>& mdses, std::vector<MDSMeta>& online_mdses,
                          std::vector<MDSMeta>& offline_mdses) {
  uint64_t now_ms = Helper::TimestampMs();

  for (const auto& mds : mdses) {
    // DINGO_LOG(INFO) << fmt::format("[monitor] mds: {}, last online time: {}, now: {}, offline period: {}", mds.ID(),
    //   mds.LastOnlineTimeMs(), now_ms, FLAGS_mds_heartbeat_mds_offline_period_time_ms);
    if (mds.LastOnlineTimeMs() + FLAGS_mds_heartbeat_mds_offline_period_time_ms < now_ms) {
      offline_mdses.push_back(mds);
    } else {
      online_mdses.push_back(mds);
    }
  }
}

bool Monitor::Init() { return dist_lock_->Init(); }

void Monitor::Destroy() { dist_lock_->Destroy(); }

void Monitor::Run() {
  bool running = false;
  if (!is_running_.compare_exchange_strong(running, true)) {
    DINGO_LOG(INFO) << "[monitor] already running......";
    return;
  }
  DEFER(is_running_.store(false));

  auto status = MonitorMDS();
  DINGO_LOG(INFO) << fmt::format("[monitor] monitor mds finish, {}.", status.error_str());

  status = MonitorClient();
  DINGO_LOG(INFO) << fmt::format("[monitor] monitor client finish, {}.", status.error_str());
}

void Monitor::NotifyRefreshFs(const MDSMeta& mds, const FsInfoEntry& fs_info) {
  notify_buddy_->AsyncNotify(notify::RefreshFsInfoMessage::Create(mds.ID(), fs_info.fs_id(), fs_info.fs_name()));
}

void Monitor::NotifyRefreshFs(const std::vector<MDSMeta>& mdses, const FsInfoEntry& fs_info) {
  for (const auto& mds : mdses) {
    NotifyRefreshFs(mds, fs_info);
  }
}

static void CheckMdsAlive(std::vector<MDSMeta>& offline_mdses, std::vector<MDSMeta>& online_mdses) {
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
Status Monitor::MonitorMDS() {
  auto& server = Server::GetInstance();
  auto heartbeat = server.GetHeartbeat();
  auto mds_meta_map = server.GetMDSMetaMap();

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

Status Monitor::ProcessFaultMDS(std::vector<MDSMeta>& mdses) {
  auto fs_set = fs_set_->GetAllFileSystem();
  if (fs_set.empty()) {
    return Status::OK();
  }

  std::vector<MDSMeta> online_mdses, offline_mdses;
  GetOfflineMDS(mdses, online_mdses, offline_mdses);

  // check mds offline again
  CheckMdsAlive(offline_mdses, online_mdses);
  DINGO_LOG(INFO) << fmt::format("[monitor] online mdses({}) offline mdses({}).",
                                 Helper::VectorToString(MdsHelper::GetMdsIds(online_mdses)),
                                 Helper::VectorToString(MdsHelper::GetMdsIds(offline_mdses)));

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

  auto has_offlines_func = [&offline_mdses](const std::vector<uint64_t>& mds_ids) -> bool {
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
    auto fs_info = fs->GetFsInfo();
    const auto& partition_policy = fs_info.partition_policy();
    if (partition_policy.type() == pb::mds::PartitionType::MONOLITHIC_PARTITION) {
      if (is_offline_func(partition_policy.mono().mds_id())) {
        auto new_mds = pick_mds_func();
        Context ctx;
        auto status = fs->JoinMonoFs(ctx, new_mds.ID(), "fault transfer fs by monitor");
        if (!status.ok()) {
          DINGO_LOG(ERROR) << fmt::format("[monitor] transfer fs({}) from mds({}) to mds({}) fail, {}.", fs->FsName(),
                                          partition_policy.mono().mds_id(), new_mds.ID(), status.error_str());
          continue;
        }

        DINGO_LOG(INFO) << fmt::format("[monitor] transfer fs({}) from mds({}) to mds({}) finish.", fs->FsName(),
                                       partition_policy.mono().mds_id(), new_mds.ID());

        NotifyRefreshFs(new_mds, fs_info);
      }

    } else if (partition_policy.type() == pb::mds::PartitionType::PARENT_ID_HASH_PARTITION) {
      auto mds_ids = Helper::GetMdsIds(partition_policy.parent_hash());
      if (has_offlines_func(mds_ids)) {
        auto new_distributions = HashPartitionHelper::AdjustDistribution(
            partition_policy, MdsHelper::GetMdsIdSet(online_mdses), MdsHelper::GetMdsIdSet(offline_mdses));

        auto status = fs->UpdatePartitionPolicy(new_distributions, "fault migration by monitor");

        DINGO_LOG(INFO) << fmt::format("[monitor] transfer fs({}) from mds({}) to mds({}) finish, status({}).",
                                       fs->FsName(), Helper::VectorToString(mds_ids),
                                       Helper::VectorToString(Helper::GetMdsIds(new_distributions)),
                                       status.error_str());

        // notify new mds to start serve partition
        auto mds_metas = MdsHelper::FilterMdsMetas(mdses, Helper::GetMdsIds(new_distributions));
        NotifyRefreshFs(mds_metas, fs_info);
      }
    }
  }

  return Status::OK();
}

Status Monitor::MonitorClient() {
  auto& server = Server::GetInstance();
  auto heartbeat = server.GetHeartbeat();

  std::vector<ClientEntry> clients;
  auto status = heartbeat->GetClientList(clients);
  if (!status.ok()) {
    return Status(status.error_code(), fmt::format("get client list fail, {}", status.error_str()));
  }
  if (clients.empty()) return Status::OK();

  uint64_t now_ms = Helper::TimestampMs();

  // umount all offline clients
  for (const auto& client : clients) {
    if (client.last_online_time_ms() + FLAGS_mds_monitor_client_clean_period_time_s > now_ms) {
      // client is online
      continue;
    }

    // umount client
    if (!client.fs_name().empty()) {
      Context ctx;
      auto status = fs_set_->UmountFs(ctx, client.fs_name(), client.id());
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("[monitor] umount fs({}) from client({}) fail, {}.", client.fs_name(),
                                        client.id(), status.error_str());
        continue;
      }
    }

    // clean client
    status = heartbeat->CleanClient(client.id());
    DINGO_LOG(INFO) << fmt::format("[monitor] clean client({}) finish, status({}).", client.id(), status.error_str());
  }

  return Status::OK();
}

}  // namespace mds
}  // namespace dingofs
