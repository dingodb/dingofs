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

#ifndef DINGOFS_BLOCKCACHE_UTILS_MEMORY_H_
#define DINGOFS_BLOCKCACHE_UTILS_MEMORY_H_

#include <cstddef>

namespace dingofs {
namespace blockcache {

void BindMemoryToNumaNode(void* addr, size_t length, int numa_node);
void AdviseHugePages(void* addr, size_t length);

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_UTILS_MEMORY_H_
