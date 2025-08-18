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

#include "mdsv2/common/context.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/store_operation.h"
#include "mdsv2/mds/mds_meta.h"

namespace dingofs {
namespace mdsv2 {

class Heartbeat;
using HeartbeatSPtr = std::shared_ptr<Heartbeat>;

class Heartbeat {
 public:
  Heartbeat(OperationProcessorSPtr operation_processor) : operation_processor_(operation_processor) {};
  ~Heartbeat() = default;

  static HeartbeatSPtr New(OperationProcessorSPtr operation_processor) {
    return std::make_shared<Heartbeat>(operation_processor);
  }

  bool Init();
  bool Destroy();

  void Run();

  void SendHeartbeat();
  Status SendHeartbeat(Context& ctx, MdsEntry& mds);
  Status SendHeartbeat(Context& ctx, ClientEntry& client);
  Status SendHeartbeat(Context& ctx, CacheMemberEntry& heartbeat_cache_member);

  Status GetMDSList(Context& ctx, std::vector<MdsEntry>& mdses);
  Status GetMDSList(std::vector<MDSMeta>& mdses);

  Status GetClientList(std::vector<ClientEntry>& clients);

  Status GetCacheMemberList(std::vector<CacheMemberEntry>& cache_members);

  Status CleanClient(const std::string& client_id);

 private:
  std::atomic<bool> is_running_{false};

  OperationProcessorSPtr operation_processor_;

  WorkerSPtr worker_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_BACKGROUND_HEARTBEAT_H_