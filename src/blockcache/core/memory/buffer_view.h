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

namespace dingofs {
namespace blockcache {

struct BufferView {
  void* data = nullptr;
  uint32_t size = 0;

  BufferView() = default;
  BufferView(const void* data, uint32_t size)
      : data(const_cast<void*>(data)), size(size) {}

  bool empty() const { return data == nullptr || size == 0; }
};

using BufferViews = std::span<const BufferView>;

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
