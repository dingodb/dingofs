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

#include "mds/background/heartbeat.h"

#include <cstdint>
#include <string>
#include <vector>

#include "common/logging.h"
#include "fmt/format.h"
#include "mds/common/context.h"
#include "mds/common/helper.h"
#include "mds/common/status.h"
#include "mds/common/tracing.h"
#include "mds/filesystem/store_operation.h"
#include "mds/mds/mds_meta.h"
#include "mds/server.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_scan_batch_size);

DEFINE_uint32(mds_heartbeat_mds_offline_period_time_ms, 30 * 1000, "mds offline period time ms");
DEFINE_validator(mds_heartbeat_mds_offline_period_time_ms, brpc::PassValidate);
DEFINE_uint32(mds_heartbeat_client_offline_period_ms, 30 * 1000, "client offline period time ms");
DEFINE_validator(mds_heartbeat_client_offline_period_ms, brpc::PassValidate);

bool Heartbeat::Init() {
  std::vector<MDSMeta> mdses;
  auto status = GetMDSList(mdses);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[heartbeat] get mds list fail, error({}).", status.error_str());
    return false;
  }

  return true;
}

bool Heartbeat::Destroy() { return true; }

void Heartbeat::Run() {
  bool running = false;
  if (!is_running_.compare_exchange_strong(running, true)) {
    LOG(INFO) << "[heartbeat] heartbeat already running......";
    return;
  }
  DEFER(is_running_.store(false));

  SendHeartbeat();
}

void Heartbeat::SendHeartbeat() {
  auto& self_mds_meta = Server::GetInstance().GetMDSMeta();

  Context ctx;
  auto mds = self_mds_meta.ToProto();
  SendHeartbeat(ctx, mds);
}

Status Heartbeat::SendHeartbeat(Context& ctx, MdsEntry& mds) {
  if (mds.id() == 0) {
    LOG(ERROR) << "[heartbeat] send fail, mds id is 0.";
    return Status(pb::error::Errno::EINTERNAL, "mds id is 0");
  }

  mds.set_last_online_time_ms(Helper::TimestampMs());

  LOG_DEBUG << fmt::format("[heartbeat] mds {}.", mds.ShortDebugString());

  auto& trace = ctx.GetTrace();
  UpsertMdsOperation operation(trace, mds);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[heartbeat] send fail, mds({}) error({}).", mds.ShortDebugString(), status.error_str());
  }

  return status;
}

Status Heartbeat::SendHeartbeat(Context& ctx, ClientEntry& client) {
  client.set_last_online_time_ms(Helper::TimestampMs());

  LOG_DEBUG << fmt::format("[heartbeat] client {}.", client.ShortDebugString());

  auto& trace = ctx.GetTrace();
  UpsertClientOperation operation(trace, client);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[heartbeat] send fail, client({}) error({}).", client.ShortDebugString(),
                              status.error_str());
  }

  return status;
}

Status Heartbeat::SendHeartbeat(Context& ctx, CacheMemberEntry& heartbeat_cache_member) {
  const auto& ip = heartbeat_cache_member.ip();
  auto port = heartbeat_cache_member.port();
  auto now_time = Helper::TimestampMs();
  auto handler = [ip, port, now_time](CacheMemberEntry& cache_member, const Status& status) -> Status {
    if (!status.ok()) {
      return status;
    }
    if (ip != cache_member.ip() || port != cache_member.port()) {
      return Status(pb::error::Errno::ENOT_MATCH, "cache member not match");
    }
    cache_member.set_last_online_time_ms(now_time);
    return Status::OK();
  };

  LOG_DEBUG << fmt::format("[heartbeat] heartbeat_cache_member {}.", heartbeat_cache_member.ShortDebugString());

  auto& trace = ctx.GetTrace();
  UpsertCacheMemberOperation operation(trace, heartbeat_cache_member.member_id(), handler);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[heartbeat] send fail, heartbeat_cache_member({}) error({}).",
                              heartbeat_cache_member.ShortDebugString(), status.error_str());
    return status;
  }
  auto& result = operation.GetResult();
  cache_group_member_manager_->UpsertCacheMemberToCache(result.cache_member);

  return status;
}

Status Heartbeat::GetMDSList(Context& ctx, std::vector<MdsEntry>& mdses) {
  auto& trace = ctx.GetTrace();
  ScanMdsOperation operation(trace);
  operation.SetIsolationLevel(Txn::kReadCommitted);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[heartbeat] get mds list fail, error({}).", status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  mdses = std::move(result.mds_entries);

  // set online status
  uint64_t now_ms = Helper::TimestampMs();
  for (auto& mds : mdses) {
    mds.set_is_online((mds.last_online_time_ms() + FLAGS_mds_heartbeat_mds_offline_period_time_ms < now_ms) ? false
                                                                                                            : true);
  }

  return Status::OK();
}

Status Heartbeat::GetMDSList(std::vector<MDSMeta>& mdses) {
  std::vector<MdsEntry> pb_mdses;
  Context ctx;
  auto status = GetMDSList(ctx, pb_mdses);
  if (!status.ok()) return status;

  auto mds_meta_map = Server::GetInstance().GetMDSMetaMap();
  for (auto& pb_mds : pb_mdses) {
    auto mds_meta = MDSMeta(pb_mds);
    mds_meta_map->UpsertMDSMeta(mds_meta);
    mdses.push_back(mds_meta);
  }

  return Status::OK();
}

Status Heartbeat::GetClientList(std::vector<ClientEntry>& clients) {
  Trace trace;
  ScanClientOperation operation(trace);
  operation.SetIsolationLevel(Txn::kReadCommitted);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[heartbeat] get client list fail, error({}).", status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  clients = std::move(result.client_entries);

  return Status::OK();
}

Status Heartbeat::CleanClient(const std::string& client_id) {
  Trace trace;
  DeleteClientOperation operation(trace, client_id);

  return operation_processor_->RunAlone(&operation);
}

Status Heartbeat::GetCacheMemberList(std::vector<CacheMemberEntry>& cache_members) {
  Trace trace;
  ScanCacheMemberOperation operation(trace);
  operation.SetIsolationLevel(Txn::kReadCommitted);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[heartbeat] get cache member list fail, error({}).", status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  cache_members = std::move(result.cache_member_entries);

  return Status::OK();
}

}  // namespace mds
}  // namespace dingofs
