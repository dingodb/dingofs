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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SESSION_MANAGER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SESSION_MANAGER_H_

#include <memory>
#include <vector>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/timer.h"
#include "blockcache/infiniband/base/completion_queue.h"
#include "blockcache/infiniband/base/device.h"
#include "blockcache/infiniband/server/session.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class SessionManager final {
 public:
  SessionManager(Device* device, CompletionQueue* completion_queue);

  ~SessionManager();

  SessionManager(const SessionManager&) = delete;
  SessionManager& operator=(const SessionManager&) = delete;

  void Start();
  Future<> Shutdown();

  Future<Status> AddSession(SessionUPtr session);
  Gate::Holder EnterGate() { return Gate::Holder(gate_); }

 private:
  Future<> CleanupSessions();

  bool running_ = false;
  bool cleanuping_ = false;
  Device* device_;
  CompletionQueue* completion_queue_;
  std::unique_ptr<Timer> cleanup_timer_;
  std::vector<SessionUPtr> sessions_;
  Gate gate_;
};

using SessionManagerUPtr = std::unique_ptr<SessionManager>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif
