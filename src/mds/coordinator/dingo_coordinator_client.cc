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

#include "mds/coordinator/dingo_coordinator_client.h"

#include <string>
#include <vector>

#include "common/logging.h"
#include "dingofs/error.pb.h"
#include "fmt/core.h"
#include "mds/coordinator/coordinator_client.h"
#include "mds/mds/mds_meta.h"

namespace dingofs {
namespace mds {

static dingodb::sdk::MDS::State MDSMetaStateToSdkState(MDSMeta::State state) {
  switch (state) {
    case MDSMeta::State::kInit:
      return dingodb::sdk::MDS::State::kInit;
    case MDSMeta::State::kNormal:
      return dingodb::sdk::MDS::State::kNormal;
    case MDSMeta::State::kAbnormal:
      return dingodb::sdk::MDS::State::kAbnormal;
    default:
      LOG(FATAL) << "Unknown MDSMeta state: " << static_cast<int>(state);
      break;
  }

  return dingodb::sdk::MDS::State::kInit;
}

static dingodb::sdk::MDS MDSMeta2SdkMDS(const MDSMeta& mds_meta) {
  dingodb::sdk::MDS mds;
  mds.id = mds_meta.ID();
  mds.location.host = mds_meta.Host();
  mds.location.port = mds_meta.Port();
  mds.state = MDSMetaStateToSdkState(mds_meta.GetState());
  mds.last_online_time_ms = mds_meta.LastOnlineTimeMs();

  return mds;
}

static MDSMeta SdkMDS2MDSMeta(const dingodb::sdk::MDS& sdk_mds) {
  MDSMeta mds;

  mds.SetID(sdk_mds.id);
  mds.SetHost(sdk_mds.location.host);
  mds.SetPort(sdk_mds.location.port);
  mds.SetState(static_cast<MDSMeta::State>(sdk_mds.state));
  mds.SetLastOnlineTimeMs(sdk_mds.last_online_time_ms);

  return mds;
}

static std::vector<MDSMeta> SdkMDSList2MDSes(const std::vector<dingodb::sdk::MDS>& sdk_mdses) {
  std::vector<MDSMeta> mdses;
  mdses.reserve(sdk_mdses.size());
  for (const auto& sdk_mds : sdk_mdses) {
    mdses.push_back(SdkMDS2MDSMeta(sdk_mds));
  }

  return mdses;
}

bool DingoCoordinatorClient::Init(const std::string& addr) {
  LOG(INFO) << fmt::format("init dingo coordinator client, addr({}).", addr);

  coordinator_addr_ = addr;
  auto status = dingodb::sdk::Client::BuildFromAddrs(addr, &client_);
  CHECK(status.ok()) << fmt::format("build dingo sdk client fail, error: {}", status.ToString());

  status = client_->NewCoordinator(&coordinator_);
  CHECK(status.ok()) << fmt::format("new dingo sdk coordinator fail, error: {}", status.ToString());

  status = client_->NewVersion(&version_);
  CHECK(status.ok()) << fmt::format("new dingo sdk version fail, error: {}", status.ToString());

  return true;
}

bool DingoCoordinatorClient::Destroy() {
  LOG(INFO) << fmt::format("destroy dingo coordinator client.");

  delete coordinator_;
  delete version_;
  delete client_;

  return true;
}

Status DingoCoordinatorClient::MDSHeartbeat(const MDSMeta& mds, std::vector<MDSMeta>& out_mdses) {
  std::vector<dingodb::sdk::MDS> mdses;
  auto status = coordinator_->MDSHeartbeat(MDSMeta2SdkMDS(mds), mdses);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("MDSHeartbeat fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  for (const auto& mds : mdses) {
    out_mdses.push_back(SdkMDS2MDSMeta(mds));
  }

  return Status::OK();
}

Status DingoCoordinatorClient::GetMDSList(std::vector<MDSMeta>& mdses) {
  std::vector<dingodb::sdk::MDS> sdk_mdses;
  auto status = coordinator_->GetMDSList(sdk_mdses);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("GetMDSList fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  mdses = SdkMDSList2MDSes(sdk_mdses);

  return Status::OK();
}

Status DingoCoordinatorClient::CreateAutoIncrement(int64_t table_id, int64_t start_id) {
  auto status = coordinator_->CreateAutoIncrement(table_id, start_id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("CreateAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::DeleteAutoIncrement(int64_t table_id) {
  auto status = coordinator_->DeleteAutoIncrement(table_id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("DeleteAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::UpdateAutoIncrement(int64_t table_id, int64_t start_id) {
  auto status = coordinator_->UpdateAutoIncrement(table_id, start_id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("UpdateAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::GenerateAutoIncrement(int64_t table_id, int64_t count, int64_t& start_id,
                                                     int64_t& end_id) {
  auto status = coordinator_->GenerateAutoIncrement(table_id, count, start_id, end_id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("GenerateAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::GetAutoIncrement(int64_t table_id, int64_t& start_id) {
  auto status = coordinator_->GetAutoIncrement(table_id, start_id);
  if (!status.ok()) {
    if (status.IsNotFound()) {
      return Status(pb::error::ENOT_FOUND, status.ToString());
    }
    LOG(ERROR) << fmt::format("GetAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::GetAutoIncrements(std::vector<AutoIncrement>& auto_increments) {
  std::vector<dingodb::sdk::TableIncrement> table_increments;
  auto status = coordinator_->GetAutoIncrements(table_increments);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("GetAutoIncrements fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  for (const auto& table_increment : table_increments) {
    auto_increments.push_back(AutoIncrement{table_increment.table_id, table_increment.start_id, 0});
  }

  return Status::OK();
}

static dingodb::sdk::Version::Options Options2SdkOptions(const CoordinatorClient::Options& options) {
  dingodb::sdk::Version::Options sdk_options;
  sdk_options.need_prev_kv = options.need_prev_kv;
  sdk_options.ignore_value = options.ignore_value;
  sdk_options.ignore_lease = options.ignore_lease;
  sdk_options.keys_only = options.keys_only;
  sdk_options.count_only = options.count_only;
  sdk_options.lease_id = options.lease_id;

  return sdk_options;
}

static dingodb::sdk::Version::Range Range2SdkRange(const CoordinatorClient::Range& range) {
  dingodb::sdk::Version::Range sdk_range;
  sdk_range.start_key = range.start;
  sdk_range.end_key = range.end;

  return sdk_range;
}

static dingodb::sdk::Version::KVPair KVPair2SdkKVPair(const CoordinatorClient::KVPair& kv) {
  dingodb::sdk::Version::KVPair sdk_kv;
  sdk_kv.key = kv.key;
  sdk_kv.value = kv.value;

  return sdk_kv;
}

static CoordinatorClient::KVWithExt SdkKVWithExt2KVWithExt(const dingodb::sdk::Version::KVWithExt& sdk_kv) {
  CoordinatorClient::KVWithExt kv;
  kv.kv.key = sdk_kv.kv.key;
  kv.kv.value = sdk_kv.kv.value;
  kv.create_revision = sdk_kv.create_revision;
  kv.mod_revision = sdk_kv.mod_revision;
  kv.version = sdk_kv.version;
  kv.lease = sdk_kv.lease;

  return kv;
}

Status DingoCoordinatorClient::KvRange(const Options& options, const Range& range, int64_t limit,
                                       std::vector<KVWithExt>& out_kvs, bool& out_more, int64_t& out_count) {
  std::vector<dingodb::sdk::Version::KVWithExt> out_sdk_kvs;
  auto status =
      version_->KvRange(Options2SdkOptions(options), Range2SdkRange(range), limit, out_sdk_kvs, out_more, out_count);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("KvRange fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  out_kvs.reserve(out_sdk_kvs.size());
  for (auto& out_sdk_kv : out_sdk_kvs) {
    out_kvs.push_back(SdkKVWithExt2KVWithExt(out_sdk_kv));
  }

  return Status::OK();
}

Status DingoCoordinatorClient::KvPut(const Options& options, const KVPair& kv, KVWithExt& out_prev_kv) {
  dingodb::sdk::Version::KVWithExt out_sdk_prev_kv;
  auto status = version_->KvPut(Options2SdkOptions(options), KVPair2SdkKVPair(kv), out_sdk_prev_kv);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("KvPut fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  out_prev_kv = SdkKVWithExt2KVWithExt(out_sdk_prev_kv);

  return Status::OK();
}

Status DingoCoordinatorClient::KvDeleteRange(const Options& options, const Range& range, int64_t& out_deleted,
                                             std::vector<KVWithExt>& out_prev_kvs) {
  std::vector<dingodb::sdk::Version::KVWithExt> out_sdk_prev_kvs;
  auto status =
      version_->KvDeleteRange(Options2SdkOptions(options), Range2SdkRange(range), out_deleted, out_sdk_prev_kvs);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("KvDeleteRange fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  out_prev_kvs.reserve(out_sdk_prev_kvs.size());
  for (auto& out_sdk_prev_kv : out_sdk_prev_kvs) {
    out_prev_kvs.push_back(SdkKVWithExt2KVWithExt(out_sdk_prev_kv));
  }

  return Status::OK();
}

Status DingoCoordinatorClient::KvCompaction(const Range& range, int64_t revision, int64_t& out_count) {
  auto status = version_->KvCompaction(Range2SdkRange(range), revision, out_count);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("KvCompaction fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::LeaseGrant(int64_t id, int64_t ttl, int64_t& out_id, int64_t& out_ttl) {
  auto status = version_->LeaseGrant(id, ttl, out_id, out_ttl);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("LeaseGrant fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::LeaseRevoke(int64_t id) {
  auto status = version_->LeaseRevoke(id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("LeaseRevoke fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::LeaseRenew(int64_t id, int64_t& out_ttl) {
  auto status = version_->LeaseRenew(id, out_ttl);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("LeaseRenew fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::LeaseQuery(int64_t id, bool is_get_key, int64_t& out_ttl, int64_t& out_granted_ttl,
                                          std::vector<std::string>& out_keys) {
  auto status = version_->LeaseQuery(id, is_get_key, out_ttl, out_granted_ttl, out_keys);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("LeaseQuery fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::ListLeases(std::vector<int64_t>& out_ids) {
  auto status = version_->ListLeases(out_ids);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("ListLeases fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

static CoordinatorClient::EventType SdkEventType2EventType(dingodb::sdk::Version::EventType sdk_event_type) {
  switch (sdk_event_type) {
    case dingodb::sdk::Version::EventType::kNone:
      return CoordinatorClient::EventType::kNone;
    case dingodb::sdk::Version::EventType::kPut:
      return CoordinatorClient::EventType::kPut;
    case dingodb::sdk::Version::EventType::kDelete:
      return CoordinatorClient::EventType::kDelete;
    case dingodb::sdk::Version::EventType::kNotExists:
      return CoordinatorClient::EventType::kNotExists;
    default:
      LOG(FATAL) << "Unknown sdk event type: " << static_cast<int>(sdk_event_type);
      break;
  }

  return CoordinatorClient::EventType::kNone;
}

static CoordinatorClient::Event SdkEvent2Event(const dingodb::sdk::Version::Event& sdk_event) {
  CoordinatorClient::Event event;
  event.type = SdkEventType2EventType(sdk_event.type);
  event.kv = SdkKVWithExt2KVWithExt(sdk_event.kv);
  event.prev_kv = SdkKVWithExt2KVWithExt(sdk_event.prev_kv);

  return event;
}

static CoordinatorClient::WatchOut SdkWatchOut2WatchOut(const dingodb::sdk::Version::WatchOut& sdk_watch_out) {
  CoordinatorClient::WatchOut watch_out;
  for (const auto& sdk_event : sdk_watch_out.events) {
    watch_out.events.push_back(SdkEvent2Event(sdk_event));
  }

  return watch_out;
}

Status DingoCoordinatorClient::Watch(const std::string& key, int64_t start_revision, WatchOut& out) {
  dingodb::sdk::Version::WatchParams param;
  param.type = dingodb::sdk::Version::WatchType::kOneTime;
  param.one_time_watch.key = key;
  param.one_time_watch.start_revision = start_revision;
  param.one_time_watch.filter_types.push_back(dingodb::sdk::Version::FilterType::kNoput);
  param.one_time_watch.need_prev_kv = false;
  param.one_time_watch.wait_on_not_exist_key = false;

  dingodb::sdk::Version::WatchOut sdk_out;
  auto status = version_->Watch(param, sdk_out);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Watch fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  out = SdkWatchOut2WatchOut(sdk_out);

  return Status::OK();
}

}  // namespace mds
}  // namespace dingofs