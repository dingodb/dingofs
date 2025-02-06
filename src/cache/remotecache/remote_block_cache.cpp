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

namespace dingofs {
namespace cache {
namespace cachegroup {

RemoteBlockCacheImpl::RemoteBlockCacheImpl(
    BlockCacheClientOption option, std::shared_ptr<MdsClient> mds_client)
    : option_(option), mds_client_(mds_client) {
  member_manager_ = std::make_unique<CacheGroupMemberManager>(
      option.group_name, option.load_members_interval_millsecond, mds_client);
}

bool BlockCacheClientImpl::Init() { return member_manager_->Start(); }

Errno BlockCacheClientImpl::Range(const BlockKey& block_key, off_t offset,
                                  size_t length, IOBuf* buffer) {
  auto node = member_manager_->GetCacheGroupNode(block_key.Filename());
  return node->Range(block_key, offset, length, buffer);
}

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs
