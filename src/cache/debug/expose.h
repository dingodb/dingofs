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
 * Created Date: 2025-07-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_DEBUG_EXPOSE_SERVICE_H_
#define DINGOFS_SRC_CACHE_DEBUG_EXPOSE_SERVICE_H_

#include <brpc/server.h>

#include "cache/common/proto.h"
#include "options/cache/blockcache.h"

namespace dingofs {
namespace cache {

class CacheServiceImpl final : public PBCacheService {
 public:
  void default_method(google::protobuf::RpcController* controller,
                      const pb::cache::DebugCacheRequest* request,
                      pb::cache::DebugCacheResponse* response,
                      google::protobuf::Closure* done) override;
};

Status AddCacheService(brpc::Server* server);

void ExposeDiskCaches(std::vector<DiskCacheOption> options);
void ExposeDiskCacheHealth(uint32_t cache_index, const std::string& health);
void ExposeMDSAddrs(const std::vector<std::string>& addrs);
void ExposeCacheGroupName(const std::string& cache_group);
void ExposeRemoteCacheNodes(const PBCacheGroupMembers& members);
void ExposeRemoteCacheNodeHealth(uint64_t node_id, const std::string& health);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_DEBUG_EXPOSE_SERVICE_H_
