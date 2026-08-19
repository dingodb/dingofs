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

#ifndef DINGOFS_BLOCKCACHE_NET_BODY_H_
#define DINGOFS_BLOCKCACHE_NET_BODY_H_

#include <cstdint>

#include "blockcache/core/memory/buffer.h"

namespace dingofs {
namespace blockcache {

// The largest body any verb may carry: one SlabPool superblock.
inline constexpr uint32_t kMaxBodyBytes = 4u << 20;
static_assert(kMaxBodyBytes == SlabPool::kSuperblockSize,
              "one body == one superblock is a load-bearing identity");

// Which way a verb's body travels; the opcode tells each end which it is.
enum class BodyShape : uint8_t {
  kNone = 0,
  kToServer = 1,  // the caller supplies bytes the handler reads
  kToClient = 2,  // the handler supplies bytes the caller receives
};

// The caller's half of a body transfer: Send/Recv mirror kToServer/kToClient.
//
// Send takes a list because a writer accumulates into fixed-size pages and
// gathering them into one buffer first would be a full-body memcpy on every
// write. The peer reads them as one stream: over rdma they become one
// RegionDesc each and the far side pulls them into its own single buffer.
// Recv is one range -- a reader always has somewhere contiguous to land.
class Body {
 public:
  static Body None() { return {}; }
  static Body Send(BufferViews source) {
    return Body(BodyShape::kToServer, source, BufferView{});
  }
  static Body Recv(BufferView destination) {
    return Body(BodyShape::kToClient, {}, destination);
  }

  BodyShape shape() const { return shape_; }
  // Send side; empty otherwise.
  BufferViews source() const { return source_; }
  // Recv side; empty otherwise.
  const BufferView& destination() const { return destination_; }

  uint64_t size() const {
    return shape_ == BodyShape::kToServer ? TotalBytes(source_)
                                          : destination_.size;
  }
  bool none() const { return shape_ == BodyShape::kNone; }
  bool empty() const { return size() == 0; }

 private:
  Body() = default;
  Body(BodyShape shape, BufferViews source, BufferView destination)
      : source_(source), destination_(destination), shape_(shape) {}

  BufferViews source_;
  BufferView destination_;
  BodyShape shape_ = BodyShape::kNone;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_BODY_H_
