
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
 * Created Date: 2026-01-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_PEER_HEALTH_CHECKER_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_PEER_HEALTH_CHECKER_H_

#include <brpc/channel.h>
#include <bthread/mutex.h>
#include <bthread/rwlock.h>
#include <butil/iobuf.h>
#include <butil/memory/scope_guard.h>

#include <atomic>
#include <memory>
#include <string>

#include "cache/iutil/state_machine.h"
#include "cache/remotecache/peer_connection.h"
#include "common/status.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {

class PeerHealthChecker {
 public:
  PeerHealthChecker(const std::string& ip, uint32_t port);
  void Start();
  void Shutdown();

  void IOError() { num_stage_error_.fetch_add(1, std::memory_order_relaxed); }
  void IOSuccess() {
    num_stage_success_.fetch_add(1, std::memory_order_relaxed);
  }
  bool IsHealthy() { return is_healthy_.load(std::memory_order_relaxed); }

 private:
  Status SendPingRequest();
  void CheckPeer();
  void PeriodicCheckPeer();

  void CommitStageIOResult();
  void PeriodicCommitStageIOResult();

  std::atomic<bool> running_;
  std::string ip_;
  uint32_t port_;
  ExecutorUPtr executor_;
  iutil::StateMachineUPtr state_machine_;
  std::atomic<uint64_t> num_stage_error_{0};
  std::atomic<uint64_t> num_stage_success_{0};
  std::atomic<bool> is_healthy_{true};
  std::unique_ptr<PeerConnection> conn_;
};

using PeerHealthCheckerUPtr = std::unique_ptr<PeerHealthChecker>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_PEER_HEALTH_CHECKER_H_
