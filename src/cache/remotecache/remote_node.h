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

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_H_

#include <brpc/channel.h>

#include "cache/blockcache/block_cache.h"
#include "cache/common/common.h"
#include "cache/config/config.h"
#include "cache/utils/state_machine.h"

namespace dingofs {
namespace cache {

class RemoteNode {
 public:
  virtual ~RemoteNode() = default;

  virtual Status Init() = 0;

  virtual Status Put(const BlockKey& key, const Block& block) = 0;
  virtual Status Range(const BlockKey& key, off_t offset, size_t length,
                       IOBuffer* buffer, uint64_t block_size) = 0;
  virtual Status Cache(const BlockKey& key, const Block& block) = 0;
  virtual Status Prefetch(const BlockKey& key, size_t length) = 0;
};

using RemoteNodeSPtr = std::shared_ptr<RemoteNode>;

class RemoteNodeImpl final : public RemoteNode {
 public:
  RemoteNodeImpl(const PB_CacheGroupMember& member, RemoteAccessOption option);

  Status Init() override;

  Status Put(const BlockKey& key, const Block& block) override;
  Status Range(const BlockKey& key, off_t offset, size_t length,
               IOBuffer* buffer, size_t block_size) override;
  Status Cache(const BlockKey& key, const Block& block) override;
  Status Prefetch(const BlockKey& key, size_t length) override;

 private:
  Status RemotePut(const BlockKey& key, const Block& block);
  Status RemoteRange(const BlockKey& key, off_t offset, size_t length,
                     IOBuffer* buffer, size_t block_size);
  Status RemoteCache(const BlockKey& key, const Block& block);
  Status RemotePrefetch(const BlockKey& key, size_t length);

  Status InitChannel(const std::string& listen_ip, uint32_t listen_port);
  Status ResetChannel();

  Status CheckHealth() const;
  Status CheckStatus(Status status);

  BthreadMutex mutex_;  // for channel
  const RemoteAccessOption option_;
  const PB_CacheGroupMember member_;
  std::unique_ptr<brpc::Channel> channel_;
  StateMachineUPtr state_machine_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_H_
