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

#ifndef DINGOFS_BLOCKCACHE_UTILS_ALIGN_H_
#define DINGOFS_BLOCKCACHE_UTILS_ALIGN_H_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <type_traits>

namespace dingofs {
namespace blockcache {

inline constexpr uint64_t kKiB = 1024;
inline constexpr uint64_t kMiB = 1024 * kKiB;

inline constexpr size_t kCacheLineSize = 64;
inline constexpr size_t kIoAlign = 4096;
inline constexpr size_t kDirectIoAlign = 512;

template <typename T>
constexpr T AlignUp(T v, T align) {
  return (v + align - 1) & ~(align - 1);
}

template <typename T>
constexpr T AlignDown(T v, T align) {
  return v & ~(align - 1);
}

template <typename T>
  requires std::is_integral_v<T>
constexpr bool IsAligned(T v, size_t align) {
  return (static_cast<size_t>(v) & (align - 1)) == 0;
}

inline bool IsAligned(const void* p, size_t align) {
  return (reinterpret_cast<uintptr_t>(p) & (align - 1)) == 0;
}

constexpr size_t AlignUp4K(size_t n) { return AlignUp(n, kIoAlign); }

inline char* AlignedAlloc(size_t n) {
  void* ptr = nullptr;
  if (posix_memalign(&ptr, kIoAlign, AlignUp4K(n)) != 0) {
    return nullptr;
  }
  return static_cast<char*>(ptr);
}

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_UTILS_ALIGN_H_
