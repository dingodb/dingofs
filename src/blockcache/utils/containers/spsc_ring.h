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

#ifndef DINGOFS_BLOCKCACHE_UTILS_CONTAINERS_SPSC_RING_H_
#define DINGOFS_BLOCKCACHE_UTILS_CONTAINERS_SPSC_RING_H_

#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <type_traits>

#include "blockcache/utils/align.h"

namespace dingofs {
namespace blockcache {

template <typename T, uint32_t N>
class SpscRing {
  static_assert(std::is_trivially_copyable_v<T>);
  static_assert(std::has_single_bit(N));

 public:
  bool Push(T v) {
    uint32_t tail = tail_.load(std::memory_order_relaxed);
    if (tail - head_cache_ >= N) {
      head_cache_ = head_.load(std::memory_order_acquire);
      if (tail - head_cache_ >= N) {
        return false;
      }
    }

    slots_[tail & kMask] = v;
    tail_.store(tail + 1, std::memory_order_release);
    return true;
  }
  template <typename F>
  uint32_t ConsumeAll(F f) {
    uint32_t head = head_.load(std::memory_order_relaxed);
    uint32_t tail = tail_.load(std::memory_order_acquire);
    uint32_t n = tail - head;
    for (uint32_t i = 0; i < n; ++i) {
      f(slots_[(head + i) & kMask]);
    }
    if (n != 0) {
      tail_cache_ = tail;
      head_.store(tail, std::memory_order_release);
    }
    return n;
  }

 private:
  static constexpr uint32_t kMask = N - 1;

  alignas(kCacheLineSize) std::atomic<uint32_t> tail_{0};
  uint32_t head_cache_ = 0;
  alignas(kCacheLineSize) std::atomic<uint32_t> head_{0};
  uint32_t tail_cache_ = 0;
  alignas(kCacheLineSize) std::array<T, N> slots_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_UTILS_CONTAINERS_SPSC_RING_H_
