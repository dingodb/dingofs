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

#ifndef DINGOFS_CACHE_V2_NODE_STUB_H_
#define DINGOFS_CACHE_V2_NODE_STUB_H_

#include <memory>

#include "cache/v2/core/server/stub.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace cache {
namespace v2 {

class CacheStub : public Stub {
 public:
  explicit CacheStub(Channel* channel)
      : Stub(channel, pb::cache::v2::CacheService::descriptor()) {
    CheckComplete();
  }

  Future<Status> Put(Controller* cntl, const pb::cache::v2::PutRequest* request,
                     pb::cache::v2::PutResponse* response) {
    return Call(put_, request, response, cntl);
  }

  Future<Status> Get(Controller* cntl, const pb::cache::v2::GetRequest* request,
                     pb::cache::v2::GetResponse* response) {
    return Call(get_, request, response, cntl);
  }

  Future<Status> Prefetch(Controller* cntl,
                          const pb::cache::v2::PrefetchRequest* request,
                          pb::cache::v2::PrefetchResponse* response) {
    return Call(prefetch_, request, response, cntl);
  }

  Future<Status> Ping(Controller* cntl,
                      const pb::cache::v2::PingRequest* request,
                      pb::cache::v2::PingResponse* response) {
    return Call(ping_, request, response, cntl);
  }

  Future<Status> GetNodeInfo(Controller* cntl,
                             const pb::cache::v2::GetNodeInfoRequest* request,
                             pb::cache::v2::GetNodeInfoResponse* response) {
    return Call(get_node_info_, request, response, cntl);
  }

 private:
  MethodRef<pb::cache::v2::PutRequest, pb::cache::v2::PutResponse> put_ =
      Resolve<pb::cache::v2::PutRequest, pb::cache::v2::PutResponse>("Put");

  MethodRef<pb::cache::v2::GetRequest, pb::cache::v2::GetResponse> get_ =
      Resolve<pb::cache::v2::GetRequest, pb::cache::v2::GetResponse>("Get");

  MethodRef<pb::cache::v2::PrefetchRequest, pb::cache::v2::PrefetchResponse>
      prefetch_ = Resolve<pb::cache::v2::PrefetchRequest,
                          pb::cache::v2::PrefetchResponse>("Prefetch");

  MethodRef<pb::cache::v2::PingRequest, pb::cache::v2::PingResponse> ping_ =
      Resolve<pb::cache::v2::PingRequest, pb::cache::v2::PingResponse>("Ping");

  MethodRef<pb::cache::v2::GetNodeInfoRequest,
            pb::cache::v2::GetNodeInfoResponse>
      get_node_info_ =
          Resolve<pb::cache::v2::GetNodeInfoRequest,
                  pb::cache::v2::GetNodeInfoResponse>("GetNodeInfo");
};

using CacheStubUPtr = std::unique_ptr<CacheStub>;

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_NODE_STUB_H_
