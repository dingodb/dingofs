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

#ifndef DINGOFS_SRC_COMMON_WRITEMEMPOOL_CPU_LOCAL_H_
#define DINGOFS_SRC_COMMON_WRITEMEMPOOL_CPU_LOCAL_H_

#include <cstdint>

namespace dingofs {

// Returns a power-of-two CPU-local slot count that is at least eight and at
// least std::thread::hardware_concurrency(). A zero hardware hint therefore
// degrades to eight slots.
uint32_t CpuLocalCacheCount();

// Maps the current logical CPU to a slot. `cache_count` must be the
// power-of-two value returned by CpuLocalCacheCount(). If the current CPU
// cannot be queried, a thread-local hash supplies a stable fallback.
uint32_t CurrentCpuLocalCache(uint32_t cache_count);

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_WRITEMEMPOOL_CPU_LOCAL_H_
