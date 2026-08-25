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

#ifndef DINGOFS_BLOCKCACHE_NET_TYPES_H_
#define DINGOFS_BLOCKCACHE_NET_TYPES_H_

#include <cstdint>

#include "blockcache/core/memory/slab_pool.h"

namespace dingofs {
namespace blockcache {

using Opcode = uint16_t;

using ReplyCode = uint16_t;

inline constexpr ReplyCode kReplyOk = 0;
inline constexpr ReplyCode kReplyBadOpcode = 0xff01;
inline constexpr ReplyCode kReplyHandlerError = 0xff02;
inline constexpr ReplyCode kReplyTooLarge = 0xff03;
inline constexpr ReplyCode kReplyBadRequest = 0xff04;

inline constexpr Opcode kOpUnspecified = 0;

inline constexpr uint32_t kMaxAttachmentBytes = 4u << 20;
static_assert(kMaxAttachmentBytes == SlabPool::kSuperblockSize,
              "one attachment == one superblock is a load-bearing identity");

}  // namespace blockcache
}  // namespace dingofs

#endif
