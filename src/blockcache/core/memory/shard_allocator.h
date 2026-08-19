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

#ifndef DINGOFS_BLOCKCACHE_CORE_MEMORY_SHARD_ALLOCATOR_H_
#define DINGOFS_BLOCKCACHE_CORE_MEMORY_SHARD_ALLOCATOR_H_

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "blockcache/utils/align.h"

namespace dingofs {
namespace blockcache {
namespace memory {

inline constexpr int kShardShift = 34;  // 16GiB vm per shard
inline constexpr size_t kSpanSize = 64 * 1024;
inline constexpr size_t kCommitStep = 2 * 1024 * 1024;
inline constexpr size_t kMaxSmallSize = 4096;
inline constexpr int kNumClasses = 16;

void GlobalInit(unsigned shard_count);
void ShardInit(unsigned shard_id, int numa_node = -1);

int LocalNumaNode();
bool IsShardMemory(const void* p);
unsigned ShardOfPointer(const void* p);
bool IsLocal(const void* p);

void* Alloc(size_t size);
void Free(void* p);
unsigned DrainCrossShardFree();

}  // namespace memory
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_MEMORY_SHARD_ALLOCATOR_H_
