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

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_BLOCK_CACHE_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_BLOCK_CACHE_H_

#include <butil/iobuf.h>

#include <memory>

#include "base/hash/con_hash.h"
#include "cache/blockcache/cache_store.h"
#include "cache/remotecache/remote_node_group.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {
namespace remotecache {

using dingofs::cache::blockcache::BlockKey;
using dingofs::stub::rpcclient::MdsClient;

class RemoteBlockCache {
 public:
  virtual ~RemoteBlockCache() = default;

  virtual Status Init() = 0;

  virtual void Shutdown() = 0;

  virtual Status Range(const BlockKey& block_key, size_t block_size,
                       off_t offset, size_t length, butil::IOBuf* buffer) = 0;
};

class RemoteBlockCacheImpl : public RemoteBlockCache {
 public:
  RemoteBlockCacheImpl(RemoteBlockCacheOption option,
                       std::shared_ptr<MdsClient> mds_client);

  Status Init() override;

  void Shutdown() override;

  Status Range(const BlockKey& block_key, size_t block_size, off_t offset,
               size_t length, butil::IOBuf* buffer) override;

 private:
  std::atomic<bool> inited_;
  RemoteBlockCacheOption option_;
  std::shared_ptr<MdsClient> mds_client_;
  std::unique_ptr<RemoteNodeGroup> node_group_;
};

}  // namespace remotecache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CLIENT_BLOCK_CACHE_CLIENT_H_
