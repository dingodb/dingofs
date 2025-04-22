/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/remote_block_cache.h"

#include <butil/iobuf.h>

#include <memory>

#include "cache/remotecache/remote_node_group.h"

namespace dingofs {
namespace cache {
namespace remotecache {

RemoteBlockCacheImpl::RemoteBlockCacheImpl(
    RemoteBlockCacheOption option, std::shared_ptr<MdsClient> mds_client)
    : inited_(false),
      option_(option),
      mds_client_(mds_client),
      node_group_(std::make_unique<RemoteNodeGroupImpl>(option, mds_client)) {}

Status RemoteBlockCacheImpl::Init() { return node_group_->Start(); }

void RemoteBlockCacheImpl::Shutdown() {}

Status RemoteBlockCacheImpl::Range(const BlockKey& block_key, size_t block_size,
                                   off_t offset, size_t length,
                                   butil::IOBuf* buffer) {
  auto node = node_group_->Get(block_key.Filename());
  return node->Range(block_key, block_size, offset, length, buffer);
}

}  // namespace remotecache
}  // namespace cache
}  // namespace dingofs
