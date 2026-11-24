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

#include "blockcache/utils/memory.h"

#include <linux/mempolicy.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cstddef>

namespace dingofs {
namespace blockcache {

void BindMemoryToNumaNode(void* addr, size_t length, int numa_node) {
  constexpr int kMaskBits = sizeof(unsigned long) * 8;
  if (numa_node < 0 || numa_node >= kMaskBits) {
    return;
  }
  const unsigned long nodemask = 1ul << numa_node;
  (void)::syscall(SYS_mbind, addr, length, MPOL_BIND, &nodemask, kMaskBits + 1,
                  0);
}

void AdviseHugePages(void* addr, size_t length) {
  (void)::madvise(addr, length, MADV_HUGEPAGE);
}

}  // namespace blockcache
}  // namespace dingofs
