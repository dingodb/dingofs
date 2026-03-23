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

#ifndef DINGOFS_CLIENT_COMMON_CLIENT_STATE_H_
#define DINGOFS_CLIENT_COMMON_CLIENT_STATE_H_

#include <atomic>
#include <cstdint>

#include "utils/time.h"

namespace dingofs {
namespace client {

// Client session state. Written by the SDK layer (Start/Stop/Load/Dump),
// readable from any layer without a DingofsClient instance.
struct ClientState {
  static uint64_t GetEpoch() { return epoch_.load(std::memory_order_relaxed); }
  static uint64_t GetStartTime() {
    return start_time_ms_.load(std::memory_order_relaxed);
  }
  static uint64_t GetFirstStartTime() {
    return first_start_time_ms_.load(std::memory_order_relaxed);
  }

  static void SetEpoch(uint64_t epoch) {
    epoch_.store(epoch, std::memory_order_relaxed);
  }
  static void SetFirstStartTime(uint64_t t) {
    first_start_time_ms_.store(t, std::memory_order_relaxed);
  }

 private:
  inline static std::atomic<uint64_t> epoch_{1};
  inline static std::atomic<uint64_t> start_time_ms_{utils::TimestampMs()};
  inline static std::atomic<uint64_t> first_start_time_ms_{
      utils::TimestampMs()};
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_COMMON_CLIENT_STATE_H_
