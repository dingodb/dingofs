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

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_CLIENT_BLOCK_CACHE_CLIENT_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_CLIENT_BLOCK_CACHE_CLIENT_H_

#include <butil/iobuf.h>

#include <memory>

#include "base/hash/con_hash.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/errno.h"
#include "client/cachegroup/base/errno.h"
#include "client/cachegroup/client/cache_group_member_manager.h"
#include "client/common/config.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using ::butil::IOBuf;
using ::dingofs::base::hash::ConHash;
using ::dingofs::client::blockcache::BlockKey;
using ::dingofs::client::blockcache::Errno;
using ::dingofs::client::cachegroup::Errno;
using ::dingofs::client::common::BlockCacheClientOption;
using ::dingofs::client::common::CacheGroupOption;
using ::dingofs::stub::rpcclient::MdsClient;

class RemoteBlockCache {
 public:
  virtual ~RemoteBlockCache() = default;

  virtual bool Init() = 0;

  virtual Errno Range(const BlockKey& block_key, off_t offset, size_t length,
                      IOBuffer* buffer) = 0;
};

class RemoteBlockCacheImpl : public RemoteBlockCache {
 public:
  explicit RemoteBlockCacheImpl(RemoteBlockCacheOption option);

  bool Init() override;

  Errno Range(const BlockKey& block_key, off_t offset, size_t length,
              IOBuffer* buffer) override;

 private:
  BlockCacheClientOption option_;
  std::shared_ptr<MdsClient> mds_client_;
  std::unique_ptr<CacheGroupMemberManager> member_manager_;
};

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CLIENT_BLOCK_CACHE_CLIENT_H_
