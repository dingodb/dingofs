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

#ifndef DINGOFS_BLOCKCACHE_CORE_REACTOR_IDLE_H_
#define DINGOFS_BLOCKCACHE_CORE_REACTOR_IDLE_H_

#include <cstdint>

#include "blockcache/utils/cpu.h"
#include "blockcache/utils/time.h"

namespace dingofs {
namespace blockcache {

class IdleSpinner {
 public:
  explicit IdleSpinner(bool poll_mode) : poll_mode_(poll_mode) {}

  // return true if should sleep
  bool Spin() {
    if (!idle_) {
      idle_ = true;
      since_ns_ = TimestampNs();
      tls_cached_timestamp = since_ns_;
      spins_ = 0;
    }

    CpuRelax();

    if ((++spins_ & 63) != 0) {
      return false;
    }

    // Cached-clock heartbeat: poll mode must take this read too, not bail.
    const uint64_t now = TimestampNs();
    tls_cached_timestamp = now;
    if (poll_mode_) {
      return false;
    }

    return now - since_ns_ > kMaxPollTimeNs;
  }

  // return the time spent idle
  uint64_t EndIdle() {
    if (!idle_) {
      return 0;
    }

    idle_ = false;
    const uint64_t now = TimestampNs();
    tls_cached_timestamp = now;
    return now - since_ns_;
  }

 private:
  static constexpr uint64_t kMaxPollTimeNs = 200'000;

  uint64_t since_ns_ = 0;
  uint64_t spins_ = 0;
  bool idle_ = false;
  const bool poll_mode_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_REACTOR_IDLE_H_
