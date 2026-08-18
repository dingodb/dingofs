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

#ifndef DINGOFS_CACHE_V2_REMOTE_REMOTE_CACHE_H_
#define DINGOFS_CACHE_V2_REMOTE_REMOTE_CACHE_H_

#include <cstdint>
#include <memory>

#include "cache/v2/block/block_cache.h"
#include "cache/v2/common/mds_client.h"
#include "cache/v2/remote/members.h"
#include "cache/v2/remote/node_group.h"

namespace dingofs {
namespace cache {
namespace v2 {

class RemoteCache final : public BlockCache {
 public:
  explicit RemoteCache(MDSClient* mds_client);
  ~RemoteCache() override;

  RemoteCache(const RemoteCache&) = delete;
  RemoteCache& operator=(const RemoteCache&) = delete;

  Future<> Start() override;
  Future<> Shutdown() override;

  Future<Status> Put(BlockHandle handle, BufferViews body,
                     PutOption option = {}) override;
  Future<Status> Get(BlockHandle handle, uint64_t offset, uint32_t length,
                     char* buffer, GetOption option = {}) override;
  Future<Status> Prefetch(BlockHandle handle,
                          PrefetchOption option = {}) override;
  Future<Status> Delete(BlockHandle handle, DeleteOption option = {}) override;
  Future<CacheStats> GetStats() override;

  static Members GetMembers();

 private:
  Future<> InitRdma();
  Future<> ShutdownRdma();
  void StartSyncer();
  void ShutdownSyncer();
  Future<> WaitForMembersSynced();

  bool running_ = false;
  MDSClient* mds_client_;
  RemoteNodeGroupUPtr nodes_;
  CacheGroupMemberSyncerUPtr syncer_;

  uint64_t hits_ = 0;
  uint64_t misses_ = 0;
};

using RemoteCacheUPtr = std::unique_ptr<RemoteCache>;

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_REMOTE_REMOTE_CACHE_H_
