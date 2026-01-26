/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_HEARTBEAT_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_HEARTBEAT_H_

#include <memory>

#include "cache/common/mds_client.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {

class Heartbeat {
 public:
  Heartbeat(MDSClientSPtr mds_client);
  void Start();
  void Shutdown();

 private:
  void SendHeartbeat();
  void PeriodicSendHeartbeat();

 private:
  std::atomic<bool> running_;
  MDSClientSPtr mds_client_;
  ExecutorUPtr executor_;
};

using HeartbeatUPtr = std::unique_ptr<Heartbeat>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_HEARTBEAT_H_
