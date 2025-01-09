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

#include "mdsv2/coordinator/dingo_coordinator_client.h"

#include <string>
#include <vector>

#include "dingofs/error.pb.h"
#include "fmt/core.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/mds/mds_meta.h"

namespace dingofs {
namespace mdsv2 {

static dingodb::sdk::MDS::State MDSMetaStateToSdkState(MDSMeta::State state) {
  switch (state) {
    case MDSMeta::State::kInit:
      return dingodb::sdk::MDS::State::kInit;
    case MDSMeta::State::kNormal:
      return dingodb::sdk::MDS::State::kNormal;
    case MDSMeta::State::kAbnormal:
      return dingodb::sdk::MDS::State::kAbnormal;
    default:
      DINGO_LOG(FATAL) << "Unknown MDSMeta state: " << static_cast<int>(state);
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
  mds.register_time_ms = mds_meta.RegisterTimeMs();
  mds.last_online_time_ms = mds_meta.LastOnlineTimeMs();

  return mds;
}

static MDSMeta SdkMDS2MDSMeta(const dingodb::sdk::MDS& sdk_mds) {
  MDSMeta mds;

  mds.SetID(sdk_mds.id);
  mds.SetHost(sdk_mds.location.host);
  mds.SetPort(sdk_mds.location.port);
  mds.SetState(static_cast<MDSMeta::State>(sdk_mds.state));
  mds.SetRegisterTimeMs(sdk_mds.register_time_ms);
  mds.SetLastOnlineTimeMs(sdk_mds.last_online_time_ms);

  return mds;
}

static std::vector<MDSMeta> SdkMDSList2MDSes(const std::vector<dingodb::sdk::MDS>& sdk_mdses) {
  std::vector<MDSMeta> mdses;
  for (const auto& sdk_mds : sdk_mdses) {
    mdses.push_back(SdkMDS2MDSMeta(sdk_mds));
  }

  return mdses;
}

bool DingoCoordinatorClient::Init(const std::string& addr) {
  DINGO_LOG(INFO) << fmt::format("Init coordinator client, addr({}).", addr);

  coordinator_addr_ = addr;
  auto status = dingodb::sdk::Client::BuildFromAddrs(addr, &client_);
  CHECK(status.ok()) << fmt::format("Build dingo sdk client fail, error: {}", status.ToString());

  status = client_->NewCoordinator(&coordinator_);
  CHECK(status.ok()) << fmt::format("New dingo sdk coordinator fail, error: {}", status.ToString());

  return true;
}

bool DingoCoordinatorClient::Destroy() {
  delete coordinator_;
  delete client_;

  return true;
}

Status DingoCoordinatorClient::MDSHeartbeat(const MDSMeta& mds) {
  auto status = coordinator_->MDSHeartbeat(MDSMeta2SdkMDS(mds));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("MDSHeartbeat fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::GetMDSList(std::vector<MDSMeta>& mdses) {
  std::vector<dingodb::sdk::MDS> sdk_mdses;
  auto status = coordinator_->GetMDSList(sdk_mdses);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("GetMDSList fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  mdses = SdkMDSList2MDSes(sdk_mdses);

  return Status::OK();
}

Status DingoCoordinatorClient::CreateAutoIncrement(int64_t table_id, int64_t start_id) {
  auto status = coordinator_->CreateAutoIncrement(table_id, start_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("CreateAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::DeleteAutoIncrement(int64_t table_id) {
  auto status = coordinator_->DeleteAutoIncrement(table_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("DeleteAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::UpdateAutoIncrement(int64_t table_id, int64_t start_id) {
  auto status = coordinator_->UpdateAutoIncrement(table_id, start_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("UpdateAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::GenerateAutoIncrement(int64_t table_id, int64_t count, int64_t& start_id,
                                                     int64_t& end_id) {
  auto status = coordinator_->GenerateAutoIncrement(table_id, count, start_id, end_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("GenerateAutoIncrement fail, error: {}", status.ToString());
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
    DINGO_LOG(ERROR) << fmt::format("GetAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status DingoCoordinatorClient::GetAutoIncrements(std::vector<AutoIncrement>& auto_increments) {
  std::vector<dingodb::sdk::TableIncrement> table_increments;
  auto status = coordinator_->GetAutoIncrements(table_increments);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("GetAutoIncrements fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  for (const auto& table_increment : table_increments) {
    auto_increments.push_back(AutoIncrement{table_increment.table_id, table_increment.start_id, 0});
  }

  return Status::OK();
}

}  // namespace mdsv2
}  // namespace dingofs