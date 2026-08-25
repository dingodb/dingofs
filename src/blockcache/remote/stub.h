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

#ifndef DINGOFS_BLOCKCACHE_REMOTE_STUB_H_
#define DINGOFS_BLOCKCACHE_REMOTE_STUB_H_

#include <memory>

#include "blockcache/net/stub.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {

class CacheStub : public Stub {
 public:
  explicit CacheStub(Channel* channel)
      : Stub(channel, pb::blockcache::CacheService::descriptor()) {
    CheckComplete();
  }

  Future<Status> Put(Controller* cntl,
                     const pb::blockcache::PutRequest* request,
                     pb::blockcache::PutResponse* response) {
    return Call(put_, request, response, cntl);
  }

  Future<Status> Get(Controller* cntl,
                     const pb::blockcache::GetRequest* request,
                     pb::blockcache::GetResponse* response) {
    return Call(get_, request, response, cntl);
  }

  Future<Status> Prefetch(Controller* cntl,
                          const pb::blockcache::PrefetchRequest* request,
                          pb::blockcache::PrefetchResponse* response) {
    return Call(prefetch_, request, response, cntl);
  }

  Future<Status> Delete(Controller* cntl,
                        const pb::blockcache::DeleteRequest* request,
                        pb::blockcache::DeleteResponse* response) {
    return Call(delete_, request, response, cntl);
  }

  Future<Status> Ping(Controller* cntl,
                      const pb::blockcache::PingRequest* request,
                      pb::blockcache::PingResponse* response) {
    return Call(ping_, request, response, cntl);
  }

  Future<Status> GetNodeInfo(Controller* cntl,
                             const pb::blockcache::GetNodeInfoRequest* request,
                             pb::blockcache::GetNodeInfoResponse* response) {
    return Call(get_node_info_, request, response, cntl);
  }

 private:
  MethodRef<pb::blockcache::PutRequest, pb::blockcache::PutResponse> put_ =
      Resolve<pb::blockcache::PutRequest, pb::blockcache::PutResponse>("Put");

  MethodRef<pb::blockcache::GetRequest, pb::blockcache::GetResponse> get_ =
      Resolve<pb::blockcache::GetRequest, pb::blockcache::GetResponse>("Get");

  MethodRef<pb::blockcache::PrefetchRequest, pb::blockcache::PrefetchResponse>
      prefetch_ = Resolve<pb::blockcache::PrefetchRequest,
                          pb::blockcache::PrefetchResponse>("Prefetch");

  MethodRef<pb::blockcache::DeleteRequest, pb::blockcache::DeleteResponse>
      delete_ = Resolve<pb::blockcache::DeleteRequest,
                        pb::blockcache::DeleteResponse>("Delete");

  MethodRef<pb::blockcache::PingRequest, pb::blockcache::PingResponse> ping_ =
      Resolve<pb::blockcache::PingRequest, pb::blockcache::PingResponse>(
          "Ping");

  MethodRef<pb::blockcache::GetNodeInfoRequest,
            pb::blockcache::GetNodeInfoResponse>
      get_node_info_ =
          Resolve<pb::blockcache::GetNodeInfoRequest,
                  pb::blockcache::GetNodeInfoResponse>("GetNodeInfo");
};

using CacheStubUPtr = std::unique_ptr<CacheStub>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_REMOTE_STUB_H_
