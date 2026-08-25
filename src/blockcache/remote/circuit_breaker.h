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

#ifndef DINGOFS_BLOCKCACHE_REMOTE_CIRCUIT_BREAKER_H_
#define DINGOFS_BLOCKCACHE_REMOTE_CIRCUIT_BREAKER_H_

#include <cstdint>
#include <memory>

#include "common/status.h"

namespace dingofs {
namespace blockcache {

class CircuitBreaker {
 public:
  bool AllowRequest();
  void OnCallEnd(const Status& status);

 private:
  enum class State : uint8_t { kClosed, kOpen, kHalfOpen };

  void Trip();

  State state_ = State::kClosed;
  bool probing_ = false;
  uint32_t failures_ = 0;
  uint64_t open_ms_ = 0;
  uint64_t open_until_ns_ = 0;
};

using CircuitBreakerUPtr = std::unique_ptr<CircuitBreaker>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_REMOTE_CIRCUIT_BREAKER_H_
