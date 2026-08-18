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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_WR_ID_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_WR_ID_H_

#include <cstdint>

namespace dingofs {
namespace cache {
namespace v2 {

// wr_id = completion-object pointer | 3-bit tag; reaping is a switch + call.
inline constexpr uint64_t kWrTagBits = 3;
inline constexpr uint64_t kWrTagMask = (1ULL << kWrTagBits) - 1;

enum WrTag : uint8_t {
  kTagOp = 0,        // OpAwaiter (single signalled op)
  kTagBatchEnd = 1,  // BatchAwaiter (tail sentinel of a chain)
  kTagRecv = 2,      // receive-ring slot
  kTagFrame = 3,     // RPC frame send buffer
};

template <typename T>
inline uint64_t MakeWrId(T* p, WrTag tag) {
  static_assert(alignof(T) > kWrTagMask,
                "wr_id steals the low bits, so the object must be aligned");
  return reinterpret_cast<uint64_t>(p) | static_cast<uint64_t>(tag);
}

inline WrTag WrIdTag(uint64_t wr_id) {
  return static_cast<WrTag>(wr_id & kWrTagMask);
}

inline void* WrIdPtr(uint64_t wr_id) {
  return reinterpret_cast<void*>(wr_id & ~kWrTagMask);
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_WR_ID_H_
