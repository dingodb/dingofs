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
 * Created Date: 2025-01-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_SERVICE_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_SERVICE_H_

#include "cache/cachegroup/cache_group_node.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

#define DECLARE_RPC_METHOD(method)                                   \
  void method(google::protobuf::RpcController* controller,           \
              const pb::cache::blockcache::method##Request* request, \
              pb::cache::blockcache::method##Response* response,     \
              google::protobuf::Closure* done)

#define DEFINE_RPC_METHOD(classname, method)                 \
  void classname::CacheGroupNodeServiceImpl::method(         \
      google::protobuf::RpcController* controller,           \
      const pb::cache::blockcache::method##Request* request, \
      pb::cache::blockcache::method##Response* response,     \
      google::protobuf::Closure* done)

using pb::cache::blockcache::BlockCacheService;

class CacheGroupNodeServiceImpl : public BlockCacheService {
 public:
  explicit CacheGroupNodeServiceImpl(std::shared_ptr<CacheGroupNode> node);

  DECLARE_RPC_METHOD(Range);

 private:
  std::shared_ptr<CacheGroupNode> node_;
};

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_SERVICE_H_
