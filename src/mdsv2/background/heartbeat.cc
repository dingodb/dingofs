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

#include <string>
#include <vector>

#include "fmt/format.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/filesystem/store_operation.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/server.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_int32(fs_scan_batch_size);

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

void Heartbeat::Run() {
  bool running = false;
  if (!is_running_.compare_exchange_strong(running, true)) {
    DINGO_LOG(INFO) << "[heartbeat] heartbeat already running......";
    return;
  }
  DEFER(is_running_.store(false));

  SendHeartbeat();
}

void Heartbeat::SendHeartbeat() {
  auto& self_mds_meta = Server::GetInstance().GetMDSMeta();

  auto mds = self_mds_meta.ToProto();
  SendHeartbeat(mds);
}

Status Heartbeat::SendHeartbeat(MdsEntry& mds) {
  mds.set_last_online_time_ms(Helper::TimestampMs());

  DINGO_LOG(DEBUG) << fmt::format("[heartbeat] mds {}.", mds.ShortDebugString());

  Trace trace;
  UpsertMdsOperation operation(trace, mds);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[heartbeat] send fail, mds({}) error({}).", mds.ShortDebugString(),
                                    status.error_str());
  }

  return status;
}

Status Heartbeat::SendHeartbeat(ClientEntry& client) {
  client.set_last_online_time_ms(Helper::TimestampMs());

  DINGO_LOG(DEBUG) << fmt::format("[heartbeat] client {}.", client.ShortDebugString());

  Trace trace;
  UpsertClientOperation operation(trace, client);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[heartbeat] send fail, client({}) error({}).", client.ShortDebugString(),
                                    status.error_str());
  }

  return status;
}

Status Heartbeat::GetMDSList(std::vector<MdsEntry>& mdses) {
  Trace trace;
  ScanMdsOperation operation(trace);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[heartbeat] get mds list fail, error({}).", status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  mdses = std::move(result.mds_entries);

  return Status::OK();
}

Status Heartbeat::GetMDSList(std::vector<MDSMeta>& mdses) {
  std::vector<MdsEntry> pb_mdses;
  auto status = GetMDSList(pb_mdses);
  if (!status.ok()) {
    return status;
  }

  for (auto& pb_mds : pb_mdses) {
    mdses.push_back(MDSMeta(pb_mds));
  }

  return Status::OK();
}

Status Heartbeat::GetClientList(std::vector<ClientEntry>& clients) {
  Trace trace;
  ScanClientOperation operation(trace);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[heartbeat] get client list fail, error({}).", status.error_str());
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

}  // namespace mdsv2
}  // namespace dingofs
