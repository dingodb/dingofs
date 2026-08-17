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

#ifndef DINGOFS_CACHE_V2_NODE_SERVICE_H_
#define DINGOFS_CACHE_V2_NODE_SERVICE_H_

#include <cstdint>

#include "cache/v2/block/sharded.h"
#include "cache/v2/core/server/proto_service.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace cache {
namespace v2 {

class CacheService : public ProtoService {
 public:
  explicit CacheService(ShardedLocalCache* block_cache);

  Future<> Put(Controller* cntl, const pb::cache::v2::PutRequest* request,
               pb::cache::v2::PutResponse* response);
  Future<> Get(Controller* cntl, const pb::cache::v2::GetRequest* request,
               pb::cache::v2::GetResponse* response);
  Future<> Prefetch(Controller* cntl,
                    const pb::cache::v2::PrefetchRequest* request,
                    pb::cache::v2::PrefetchResponse* response);
  Future<> Ping(Controller* cntl, const pb::cache::v2::PingRequest* request,
                pb::cache::v2::PingResponse* response);
  Future<> GetNodeInfo(Controller* cntl,
                       const pb::cache::v2::GetNodeInfoRequest* request,
                       pb::cache::v2::GetNodeInfoResponse* response);

 private:
  struct AlignedRange {
    uint64_t offset;
    uint32_t length;
  };
  static AlignedRange AlignRequest(uint64_t offset, uint32_t length);

  static Status CheckHandle(const pb::cache::v2::BlockHandle& handle);
  static Status CheckAttachment(const pb::cache::v2::BlockHandle& handle,
                                uint32_t attachment_size);
  static Status CheckRange(const pb::cache::v2::BlockHandle& handle,
                           uint64_t offset, uint32_t length);

  ShardedLocalCache* block_cache_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_NODE_SERVICE_H_
