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

#ifndef DINGOFS_CACHE_CORE_UTILS_ALIGN_H_
#define DINGOFS_CACHE_CORE_UTILS_ALIGN_H_

#include <cstddef>

namespace dingofs {
namespace cache {

inline constexpr size_t kCacheLineSize = 64;

template <typename T>
constexpr T AlignUp(T v, T align) {
  return (v + align - 1) & ~(align - 1);
}

template <typename T>
constexpr T AlignDown(T v, T align) {
  return v & ~(align - 1);
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_CORE_UTILS_ALIGN_H_
