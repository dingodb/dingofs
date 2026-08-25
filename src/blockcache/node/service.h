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

#ifndef DINGOFS_BLOCKCACHE_NODE_SERVICE_H_
#define DINGOFS_BLOCKCACHE_NODE_SERVICE_H_

#include <cstdint>

#include "blockcache/block/sharded.h"
#include "blockcache/net/brpc/brpc_bridge.h"
#include "blockcache/net/service.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {

class CacheService : public ProtoService {
 public:
  explicit CacheService(ShardedLocalCache* block_cache);

  Future<> Put(Controller* cntl, const pb::blockcache::PutRequest* request,
               pb::blockcache::PutResponse* response);
  Future<> Get(Controller* cntl, const pb::blockcache::GetRequest* request,
               pb::blockcache::GetResponse* response);
  Future<> Prefetch(Controller* cntl,
                    const pb::blockcache::PrefetchRequest* request,
                    pb::blockcache::PrefetchResponse* response);
  Future<> Delete(Controller* cntl,
                  const pb::blockcache::DeleteRequest* request,
                  pb::blockcache::DeleteResponse* response);
  Future<> Ping(Controller* cntl, const pb::blockcache::PingRequest* request,
                pb::blockcache::PingResponse* response);
  Future<> GetNodeInfo(Controller* cntl,
                       const pb::blockcache::GetNodeInfoRequest* request,
                       pb::blockcache::GetNodeInfoResponse* response);

 private:
  struct AlignedRange {
    uint64_t offset;
    uint32_t length;
  };
  static AlignedRange AlignRequest(uint64_t offset, uint32_t length);

  static Status CheckHandle(const pb::blockcache::BlockHandle& handle);
  static Status CheckAttachment(const pb::blockcache::BlockHandle& handle,
                                uint32_t attachment_size);
  static Status CheckRange(const pb::blockcache::BlockHandle& handle,
                           uint64_t offset, uint32_t length);

  ShardedLocalCache* block_cache_;
};

class RawCacheService final : public pb::blockcache::CacheService {
 public:
  using Impl = ::dingofs::blockcache::CacheService;

  RawCacheService(BrpcServer* server, Impl* impl)
      : server_(server), impl_(impl) {}

#define DINGOFS_BRIDGE_METHOD(name)                                        \
  void name(google::protobuf::RpcController* cntl,                         \
            const pb::blockcache::name##Request* request,                  \
            pb::blockcache::name##Response* response,                      \
            google::protobuf::Closure* done) override {                    \
    BridgeToShard(server_, impl_, &Impl::name,                             \
                  static_cast<brpc::Controller*>(cntl), request, response, \
                  done);                                                   \
  }

  DINGOFS_BRIDGE_METHOD(Put)
  DINGOFS_BRIDGE_METHOD(Get)
  DINGOFS_BRIDGE_METHOD(Prefetch)
  DINGOFS_BRIDGE_METHOD(Delete)
  DINGOFS_BRIDGE_METHOD(Ping)
  DINGOFS_BRIDGE_METHOD(GetNodeInfo)

#undef DINGOFS_BRIDGE_METHOD

 private:
  BrpcServer* server_;
  Impl* impl_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NODE_SERVICE_H_
