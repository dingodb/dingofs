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

#ifndef DINGOFS_BLOCKCACHE_STORE_HEALTH_H_
#define DINGOFS_BLOCKCACHE_STORE_HEALTH_H_

#include <cstdint>
#include <memory>
#include <string>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/store/layout.h"
#include "blockcache/store/stats.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {

class HealthStateMachine {
 public:
  HealthStateMachine(uint32_t tick_seconds, uint32_t max_errors,
                     uint32_t min_successes, uint32_t down_after_seconds)
      : tick_seconds_(tick_seconds),
        max_errors_(max_errors),
        min_successes_(min_successes),
        down_after_seconds_(down_after_seconds) {}

  void OnSuccess(uint32_t n) { successes_ += n; }
  void OnError(uint32_t n) { errors_ += n; }

  void Advance(uint32_t now_sec) {
    switch (state_) {
      case DiskHealth::kNormal:
        if (errors_ > max_errors_) {
          state_ = DiskHealth::kUnstable;
          unstable_since_sec_ = now_sec;
          ResetWindow(now_sec);
        }
        break;

      case DiskHealth::kUnstable:
        if (successes_ > min_successes_ && errors_ == 0) {
          state_ = DiskHealth::kNormal;
          ResetWindow(now_sec);
        } else if (now_sec - unstable_since_sec_ >= down_after_seconds_) {
          state_ = DiskHealth::kDown;
        }
        break;

      case DiskHealth::kDown:
        return;
    }

    if (now_sec - window_start_sec_ >= tick_seconds_) {
      ResetWindow(now_sec);
    }
  }

  void TransitionToDown() { state_ = DiskHealth::kDown; }

  DiskHealth state() const { return state_; }

 private:
  void ResetWindow(uint32_t now_sec) {
    window_start_sec_ = now_sec;
    errors_ = 0;
    successes_ = 0;
  }

  const uint32_t tick_seconds_;
  const uint32_t max_errors_;
  const uint32_t min_successes_;
  const uint32_t down_after_seconds_;
  uint32_t window_start_sec_ = 0;
  uint32_t unstable_since_sec_ = 0;
  uint32_t errors_ = 0;
  uint32_t successes_ = 0;
  DiskHealth state_ = DiskHealth::kNormal;
};

class HealthChecker {
 public:
  explicit HealthChecker(const DiskCacheLayout& layout);
  ~HealthChecker();

  HealthChecker(const HealthChecker&) = delete;
  HealthChecker& operator=(const HealthChecker&) = delete;

  Future<> Start();
  Future<> Shutdown();

  void IoSuccess() { health_state_.OnSuccess(1); }
  void IoError() {
    health_state_.OnError(1);
    io_errors_++;
  }

  bool IsNormal() const { return state() == DiskHealth::kNormal; }

  DiskHealth state() const { return health_state_.state(); }
  uint64_t io_errors() const { return io_errors_; }

 private:
  static constexpr uint32_t kProbeBytes = 4096;

  Future<> PeriodicCheckDisk();
  Future<bool> CheckLockFile() const;
  Future<bool> CheckDisk() const;
  Future<bool> WriteProbeFile(std::string path) const;
  Future<bool> ReadProbeFile(std::string path) const;

  std::string ProbePath() const {
    return layout_.ProbeDir() + "/probe" + std::to_string(ThisShardId());
  }

  bool running_ = false;
  DiskCacheLayout layout_;
  HealthStateMachine health_state_;
  char* write_buffer_ = nullptr;
  char* read_buffer_ = nullptr;
  uint64_t io_errors_ = 0;
  Gate gate_;
};

using HealthCheckerUPtr = std::unique_ptr<HealthChecker>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_STORE_HEALTH_H_
