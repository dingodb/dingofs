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

#ifndef DINGOFS_MDSV2_HEARTBEAT_H_
#define DINGOFS_MDSV2_HEARTBEAT_H_

#include "mdsv2/common/runnable.h"
#include "mdsv2/coordinator/coordinator_client.h"

namespace dingofs {

namespace mdsv2 {

class HeartbeatTask : public TaskRunnable {
 public:
  HeartbeatTask(CoordinatorClientPtr coordinator_client) : coordinator_client_(coordinator_client) {}

  ~HeartbeatTask() override = default;

  std::string Type() override { return "HEARTBEAT"; }

  void Run() override;

  static void SendHeartbeat(CoordinatorClientPtr coordinator_client);
  static void HandleHeartbeatResponse();

  static std::atomic<uint64_t> heartbeat_counter;

 private:
  bool is_update_epoch_version_;
  std::vector<int64_t> region_ids_;
  CoordinatorClientPtr coordinator_client_;
};

class Heartbeat {
 public:
  Heartbeat() = default;
  ~Heartbeat() = default;

  bool Init();
  bool Destroy();

  static void TriggerHeartbeat();

 private:
  bool Execute(TaskRunnablePtr task);

  WorkerPtr worker_;
};

}  // namespace mdsv2

}  // namespace dingofs

#endif  // DINGOFS_MDSV2_HEARTBEAT_H_