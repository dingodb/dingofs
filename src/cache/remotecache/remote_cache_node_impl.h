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
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_IMPL_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_IMPL_H_

#include <brpc/channel.h>

#include "cache/blockcache/block_cache.h"
#include "cache/common/state_machine.h"
#include "cache/remotecache/remote_cache_node.h"
#include "cache/remotecache/remote_cache_node_health_checker.h"
#include "cache/remotecache/rpc_client.h"
#include "cache/utils/bthread.h"
#include "cache/utils/context.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

struct SubRangeRequest {
  off_t offset;
  size_t length;
  IOBuffer buffer;
  Status status;
};

class RemoteCacheNodeImpl final : public RemoteCacheNode {
 public:
  RemoteCacheNodeImpl(const CacheGroupMember& member);

  Status Start() override;
  Status Shutdown() override;

  Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block) override;
  Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
               size_t length, IOBuffer* buffer, RangeOption option) override;
  Status Cache(ContextSPtr ctx, const BlockKey& key,
               const Block& block) override;
  Status Prefetch(ContextSPtr ctx, const BlockKey& key, size_t length) override;

 private:
  bool IsRunning() { return running_.load(std::memory_order_relaxed); }

  std::vector<SubRangeRequest> SplitRange(off_t offset, size_t length,
                                          size_t blksize);
  void Subrequest(std::function<void()> func);
  Status SubrequestRanges(ContextSPtr ctx, const BlockKey& key, off_t offset,
                          size_t length, IOBuffer* buffer, RangeOption option);

  Status CheckHealth(ContextSPtr ctx) const;
  Status CheckStatus(Status status);

  std::atomic<bool> running_;
  const CacheGroupMember member_;
  RPCClientUPtr rpc_;
  StateMachineSPtr state_machine_;
  RemoteCacheNodeHealthCheckerUPtr health_checker_;
  BthreadJoinerUPtr joiner_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_CACHE_NODE_IMPL_H_
