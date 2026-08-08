/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CACHE_V2_NODE_HEARTBEAT_H_
#define DINGOFS_CACHE_V2_NODE_HEARTBEAT_H_

#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "cache/v2/common/mds_client.h"

namespace dingofs {
namespace cache {
namespace v2 {

class Heartbeat {
 public:
  explicit Heartbeat(MDSClient* mds_client);
  ~Heartbeat();

  Heartbeat(const Heartbeat&) = delete;
  Heartbeat& operator=(const Heartbeat&) = delete;

  void Start();
  void Shutdown();

 private:
  void PeriodicSendHeartbeat();
  void SendHeartbeat();

  bool running_ = false;
  MDSClient* mds_client_;
  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

using HeartbeatUPtr = std::unique_ptr<Heartbeat>;

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_NODE_HEARTBEAT_H_
