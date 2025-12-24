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

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_SERVICE_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_SERVICE_H_

#include <ostream>

#include "cache/cachegroup/node.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {

class BlockCacheServiceImpl final : public pb::cache::BlockCacheService {
 public:
  explicit BlockCacheServiceImpl(CacheNodeSPtr node);

  void Put(google::protobuf::RpcController* controller,
           const pb::cache::PutRequest* request,
           pb::cache::PutResponse* response,
           google::protobuf::Closure* done) override;

  void Range(google::protobuf::RpcController* controller,
             const pb::cache::RangeRequest* request,
             pb::cache::RangeResponse* response,
             google::protobuf::Closure* done) override;

  void Cache(google::protobuf::RpcController* controller,
             const pb::cache::CacheRequest* request,
             pb::cache::CacheResponse* response,
             google::protobuf::Closure* done) override;

  void Prefetch(google::protobuf::RpcController* controller,
                const pb::cache::PrefetchRequest* request,
                pb::cache::PrefetchResponse* response,
                google::protobuf::Closure* done) override;

  void Ping(google::protobuf::RpcController* controller,
            const pb::cache::PingRequest* request,
            pb::cache::PingResponse* response,
            google::protobuf::Closure* done) override;

 private:
  Status CheckBodySize(size_t expected, size_t real) {
    if (expected != real) {
      LOG(ERROR) << "RPC request body size mismatch, expected=" << expected
                 << ", but got=" << real;
      return Status::InvalidParam("request body size mismatch");
    }
    return Status::OK();
  }

  CacheNodeSPtr node_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_SERVICE_H_
