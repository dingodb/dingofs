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

#ifndef DINGOFS_CACHE_V2_UTILS_CPU_H_
#define DINGOFS_CACHE_V2_UTILS_CPU_H_

#include <pthread.h>

#include <string>
#include <vector>

#include "cache/v2/common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

// x86 PAUSE / aarch64 YIELD for the idle-spin loop.
inline void CpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__)
  asm volatile("yield" ::: "memory");
#else
  asm volatile("" ::: "memory");
#endif
}

std::vector<int> GetAllCpus();

StatusOr<std::vector<int>> ParseCpuSet(const std::string& spec);

std::vector<int> GetPhyCores(const std::vector<int>& cpus);

int NumaNode(int cpu);

void BindPages(void* addr, size_t length, int numa_node);

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_UTILS_CPU_H_
