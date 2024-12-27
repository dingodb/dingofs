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

#include "dingofs/src/mdsv2/service/heartbeat.h"

#include "dingofs/src/mdsv2/common/logging.h"
#include "dingofs/src/mdsv2/coordinator/coordinator_client.h"
#include "dingofs/src/mdsv2/server.h"
#include "dingofs/src/mdsv2/service/mds_meta.h"
#include "fmt/core.h"

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

void HeartbeatTask::Run() { SendHeartbeat(coordinator_client_); }

void HeartbeatTask::SendHeartbeat(CoordinatorClient& coordinator_client) {
  auto& mds_meta = Server::GetInstance().GetMDSMeta();
  DINGO_LOG(INFO) << fmt::format("send heartbeat {}.", mds_meta.ToString());

  auto mds = MDSMeta2SdkMDS(mds_meta);
  auto status = coordinator_client.MDSHeartbeat(mds);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "send heartbeat fail,  error: " << status.error_str();
  }
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
    DINGO_LOG(ERROR) << "Heartbeat worker is nullptr.";
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
