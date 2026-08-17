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

#include "cache/v2/remote/circuit_breaker.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>

#include <algorithm>

#include "cache/v2/utils/time.h"

namespace dingofs {
namespace cache {
namespace v2 {

DEFINE_uint32(remote_breaker_failures, 8, "errors that isolate a cache node");
DEFINE_validator(remote_breaker_failures, brpc::PassValidate);
DEFINE_uint32(remote_breaker_open_ms, 1000, "first isolation window");
DEFINE_validator(remote_breaker_open_ms, brpc::PassValidate);
DEFINE_uint32(remote_breaker_max_open_ms, 30000, "isolation window ceiling");
DEFINE_validator(remote_breaker_max_open_ms, brpc::PassValidate);

bool CircuitBreaker::AllowRequest() {
  switch (state_) {
    case State::kClosed:
      return true;

    case State::kOpen:
      if (CachedTimestampNs() < open_until_ns_) {
        return false;
      }
      state_ = State::kHalfOpen;
      probing_ = true;
      return true;

    case State::kHalfOpen:
      if (probing_) {
        return false;
      }
      probing_ = true;
      return true;
  }
  return true;
}

void CircuitBreaker::OnCallEnd(const Status& status) {
  probing_ = false;

  if (status.ok()) {
    failures_ = 0;
    if (state_ == State::kHalfOpen) {
      state_ = State::kClosed;
      open_ms_ = 0;
    }
    return;
  } else if (status.IsNotFound() || status.IsNotSupport()) {
    return;
  }

  switch (state_) {
    case State::kClosed:
      if (++failures_ >= FLAGS_remote_breaker_failures) {
        Trip();
      }
      return;

    case State::kOpen:
      return;

    case State::kHalfOpen:
      Trip();
      return;
  }
}

void CircuitBreaker::Trip() {
  state_ = State::kOpen;
  if (open_ms_ == 0) {
    open_ms_ = FLAGS_remote_breaker_open_ms;
  } else {
    open_ms_ =
        std::min<uint64_t>(open_ms_ * 2, FLAGS_remote_breaker_max_open_ms);
  }
  open_until_ns_ = CachedTimestampNs() + (open_ms_ * 1000 * 1000);
  failures_ = 0;
  probing_ = false;
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
