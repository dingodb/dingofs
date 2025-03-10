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

#include "mdsv2/background/heartbeat.h"

#include <vector>

#include "fmt/core.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/coordinator/coordinator_client.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/server.h"

namespace dingofs {
namespace mdsv2 {

void HeartbeatTask::Run() { SendHeartbeat(coordinator_client_); }

void HeartbeatTask::SendHeartbeat(CoordinatorClientPtr coordinator_client) {
  auto& self_mds_meta = Server::GetInstance().GetMDSMeta();
  DINGO_LOG(INFO) << fmt::format("[heartbeat] {}.", self_mds_meta.ToString());

  std::vector<MDSMeta> mdses;
  auto status = coordinator_client->MDSHeartbeat(self_mds_meta, mdses);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "[heartbeat] send fail,  error: " << status.error_str();
  }

  // update other mds meta
  auto mds_meta_map = Server::GetInstance().GetMDSMetaMap();
  for (const auto& mds_meta : mdses) {
    DINGO_LOG(DEBUG) << "[heartbeat] upsert mds meta: " << mds_meta.ToString();
    mds_meta_map->UpsertMDSMeta(mds_meta);
  }

  DINGO_LOG(INFO) << fmt::format("[heartbeat] finish, {}.", self_mds_meta.ToString());
}

void HeartbeatTask::HandleHeartbeatResponse() {}

bool Heartbeat::Init() {
  worker_ = Worker::New();
  return worker_->Init();
}

bool Heartbeat::Destroy() {
  if (worker_) {
    worker_->Destroy();
  }

  return true;
}

bool Heartbeat::Execute(TaskRunnablePtr task) {
  if (worker_ == nullptr) {
    DINGO_LOG(ERROR) << "[heartbeat] worker is nullptr.";
    return false;
  }
  return worker_->Execute(task);
}

void Heartbeat::TriggerHeartbeat() {
  auto task = std::make_shared<HeartbeatTask>(Server::GetInstance().GetCoordinatorClient());
  Server::GetInstance().GetHeartbeat().Execute(task);
}

}  // namespace mdsv2
}  // namespace dingofs
