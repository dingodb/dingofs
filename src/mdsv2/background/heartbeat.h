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

#ifndef DINGOFS_MDSV2_BACKGROUND_HEARTBEAT_H_
#define DINGOFS_MDSV2_BACKGROUND_HEARTBEAT_H_

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

class Heartbeat;
using HeartbeatPtr = std::shared_ptr<Heartbeat>;

class HeartbeatTask : public TaskRunnable {
 public:
  HeartbeatTask(HeartbeatPtr heartbeat) : heartbeat_(heartbeat) {}
  ~HeartbeatTask() override = default;

  std::string Type() override { return "HEARTBEAT"; }

  void Run() override;

 private:
  HeartbeatPtr heartbeat_;
};

class Heartbeat {
 public:
  Heartbeat(KVStoragePtr kv_storage) : kv_storage_(kv_storage) {};
  ~Heartbeat() = default;

  static HeartbeatPtr New(KVStoragePtr kv_storage) { return std::make_shared<Heartbeat>(kv_storage); }

  bool Init();
  bool Destroy();

  static void TriggerHeartbeat();

  void SendHeartbeat();
  Status SendHeartbeat(pb::mdsv2::MDS& mds);
  Status SendHeartbeat(pb::mdsv2::Client& client);

  Status GetMDSList(std::vector<pb::mdsv2::MDS>& mdses);
  Status GetMDSList(std::vector<MDSMeta>& mdses);

 private:
  bool Execute(TaskRunnablePtr task);

  KVStoragePtr kv_storage_;

  WorkerPtr worker_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_BACKGROUND_HEARTBEAT_H_