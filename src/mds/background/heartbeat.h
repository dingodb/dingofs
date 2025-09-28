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

#ifndef DINGOFS_MDS_BACKGROUND_HEARTBEAT_H_
#define DINGOFS_MDS_BACKGROUND_HEARTBEAT_H_

#include "mds/cachegroup/member_manager.h"
#include "mds/common/context.h"
#include "mds/common/runnable.h"
#include "mds/common/status.h"
#include "mds/filesystem/store_operation.h"
#include "mds/mds/mds_meta.h"

namespace dingofs {
namespace mds {

class Heartbeat;
using HeartbeatSPtr = std::shared_ptr<Heartbeat>;

class Heartbeat {
 public:
  Heartbeat(OperationProcessorSPtr operation_processor, CacheGroupMemberManagerSPtr cache_group_member_manager)
      : operation_processor_(operation_processor), cache_group_member_manager_(cache_group_member_manager) {};
  ~Heartbeat() = default;

  static HeartbeatSPtr New(OperationProcessorSPtr operation_processor,
                           CacheGroupMemberManagerSPtr cache_group_member_manager) {
    return std::make_shared<Heartbeat>(operation_processor, cache_group_member_manager);
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

  CacheGroupMemberManagerSPtr cache_group_member_manager_;

  WorkerSPtr worker_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_BACKGROUND_HEARTBEAT_H_