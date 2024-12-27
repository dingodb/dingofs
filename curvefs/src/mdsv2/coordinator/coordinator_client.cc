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

#include "curvefs/src/mdsv2/coordinator/coordinator_client.h"

#include <string>

#include "curvefs/proto/error.pb.h"
#include "curvefs/src/mdsv2/common/logging.h"
#include "fmt/core.h"

namespace dingofs {

namespace mdsv2 {

bool CoordinatorClient::Init(const std::string& addr) {
  DINGO_LOG(INFO) << fmt::format("Init coordinator client, addr({}).", addr);

  coordinator_addr_ = addr;
  auto status = dingodb::sdk::Client::BuildFromAddrs(addr, &client_);
  CHECK(status.ok()) << fmt::format("Build dingo sdk client fail, error: {}", status.ToString());

  status = client_->NewCoordinator(&coordinator_);
  CHECK(status.ok()) << fmt::format("New dingo sdk coordinator fail, error: {}", status.ToString());

  return true;
}

bool CoordinatorClient::Destroy() {
  delete coordinator_;
  delete client_;

  return true;
}

Status CoordinatorClient::MDSHeartbeat(const dingodb::sdk::MDS& mds) {
  auto status = coordinator_->MDSHeartbeat(mds);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("MDSHeartbeat fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status CoordinatorClient::GetMDSList(std::vector<dingodb::sdk::MDS>& mdses) {
  auto status = coordinator_->GetMDSList(mdses);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("GetMDSList fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status CoordinatorClient::CreateAutoIncrement(int64_t table_id, int64_t start_id) {
  auto status = coordinator_->CreateAutoIncrement(table_id, start_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("CreateAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status CoordinatorClient::DeleteAutoIncrement(int64_t table_id) {
  auto status = coordinator_->DeleteAutoIncrement(table_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("DeleteAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status CoordinatorClient::UpdateAutoIncrement(int64_t table_id, int64_t start_id) {
  auto status = coordinator_->UpdateAutoIncrement(table_id, start_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("UpdateAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status CoordinatorClient::GenerateAutoIncrement(int64_t table_id, int64_t count, int64_t& start_id, int64_t& end_id) {
  auto status = coordinator_->GenerateAutoIncrement(table_id, count, start_id, end_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("GenerateAutoIncrement fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

Status CoordinatorClient::GetAutoIncrement(int64_t table_id, int64_t& start_id) {
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

Status CoordinatorClient::GetAutoIncrements(std::vector<dingodb::sdk::TableIncrement>& table_increments) {
  auto status = coordinator_->GetAutoIncrements(table_increments);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("GetAutoIncrements fail, error: {}", status.ToString());
    return Status(pb::error::ECOORDINATOR, status.ToString());
  }

  return Status::OK();
}

}  // namespace mdsv2

}  // namespace dingofs