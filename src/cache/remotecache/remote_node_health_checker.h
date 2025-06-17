// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_HEALTH_CHECKER_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_HEALTH_CHECKER_H_

#include <memory>

#include "cache/common/proto.h"
#include "cache/utils/state_machine.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {

class RemoteNodeHealthChecker {
 public:
  RemoteNodeHealthChecker(const PBCacheGroupMember& member,
                          StateMachineSPtr state_machine);
  virtual ~RemoteNodeHealthChecker() = default;

  virtual void Start();
  virtual void Shutdown();

 private:
  void RunCheck();
  void PingNode();
  Status SendPingrequest();

  std::atomic<bool> running_;
  const PBCacheGroupMember member_info_;
  StateMachineSPtr state_machine_;
  std::unique_ptr<Executor> executor_;
};

using RemoteNodeHealthCheckerUPtr = std::unique_ptr<RemoteNodeHealthChecker>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_STATE_HEALTH_CHECKER_H_
