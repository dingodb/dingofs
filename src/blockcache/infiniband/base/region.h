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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_BASE_REGION_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_BASE_REGION_H_

#include <cstddef>
#include <cstdint>
#include <span>

namespace dingofs {
namespace blockcache {
namespace infiniband {

struct LocalRegion {
  void* addr = nullptr;
  uint32_t len = 0;
  uint32_t lkey = 0;
};

struct __attribute__((packed)) RemoteRegion {
  uint64_t addr;
  uint64_t len;
  uint32_t rkey;
};
static_assert(sizeof(RemoteRegion) == 20,
              "RemoteRegion is a protocol structure");

inline uint64_t GetLength(std::span<const RemoteRegion> regions) {
  uint64_t total = 0;
  for (const RemoteRegion& region : regions) {
    total += region.len;
  }
  return total;
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_BASE_REGION_H_
