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

#ifndef DINGOFS_COMMON_BLOCK_BLOCK_CONTEXT_H_
#define DINGOFS_COMMON_BLOCK_BLOCK_CONTEXT_H_

#include <cstdint>

#include "common/block/block_key.h"

namespace dingofs {

// BlockContext = BlockKey (block identity) + routing metadata.
//
// BlockKey is a pure block identifier: {id, index, size}.
// It determines Filename/StoreKey and is used for cache lookup, disk layout,
// and hash-based placement. fs_id has nothing to do with block identity.
//
// BlockContext carries the extra routing metadata that the cache layer needs
// (e.g. fs_id for StorageClientPool to pick the right S3 config) without
// polluting BlockKey itself.
struct BlockContext {
  BlockKey key;
  uint32_t fs_id{0};  // routing: StorageClientPool selects S3 config by fs_id

  BlockContext() = default;

  BlockContext(const BlockKey& key, uint32_t fs_id) : key(key), fs_id(fs_id) {}

  BlockContext(uint64_t id, uint32_t index, uint32_t size, uint32_t fs_id)
      : key(id, index, size), fs_id(fs_id) {}
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_BLOCK_CONTEXT_H_
