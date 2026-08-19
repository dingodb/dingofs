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

#ifndef DINGOFS_BLOCKCACHE_CORE_MEMORY_BUFFER_VIEW_H_
#define DINGOFS_BLOCKCACHE_CORE_MEMORY_BUFFER_VIEW_H_

#include <cstdint>
#include <span>

// The vocabulary for borrowed bytes. A leaf on purpose: the store layer
// speaks it without taking a dependency on the runtime that owns pools.

namespace dingofs {
namespace blockcache {

// Borrowed range, owns nothing. Deliberately no lkey field (per-PD state).
// uint32_t size: RegionDesc::len and LocalBuf::len are 32-bit.
struct BufferView {
  void* data = nullptr;
  uint32_t size = 0;

  BufferView() = default;
  BufferView(const void* data, uint32_t size)
      : data(const_cast<void*>(data)), size(size) {}

  bool empty() const { return data == nullptr || size == 0; }
};

// One body spread over several ranges, in order. Borrowed twice over: both
// the bytes and the array of views must outlive the operation reading them.
// A writer accumulating into fixed-size pages hands its pages over this way
// rather than gathering them into one buffer first.
using BufferViews = std::span<const BufferView>;

// The most ranges one body may be split into. A 4 MiB block accumulated in
// 64 KiB pages is 64 of them; the rdma frame's kMaxRegions is held to this.
inline constexpr size_t kMaxBufferViews = 64;

inline uint64_t TotalBytes(BufferViews views) {
  uint64_t total = 0;
  for (const BufferView& view : views) {
    total += view.size;
  }
  return total;
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_MEMORY_BUFFER_VIEW_H_
