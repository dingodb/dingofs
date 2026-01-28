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
 * Created Date: 2025-01-08
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/service.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <butil/memory/aligned_memory.h>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/cachegroup/service.h"
#include "cache/common/context.h"
#include "cache/common/error.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

template <typename T, typename U, typename... Args>
struct ServiceClosure : public google::protobuf::Closure {
  ServiceClosure(ContextSPtr ctx, google::protobuf::Closure* done,
                 const T* request, U* response, Status& status)
      : ctx(ctx),
        done(done),
        request(request),
        response(response),
        status(status) {}

  ~ServiceClosure() override = default;

  void Run() override {
    std::unique_ptr<ServiceClosure<T, U>> self_guard(this);

    if (response->status() != pb::cache::BlockCacheOk) {
      LOG(ERROR) << "Fail to process rpc request="
                 << request->ShortDebugString()
                 << ", response=" << response->ShortDebugString();
    }
    done->Run();
  }

  ContextSPtr ctx;
  google::protobuf::Closure* done;
  const T* request;
  U* response;
  Status& status;
};

BlockCacheServiceImpl::BlockCacheServiceImpl(CacheNodeSPtr node)
    : node_(CHECK_NOTNULL(node)) {}

void BlockCacheServiceImpl::Put(google::protobuf::RpcController* controller,
                                const pb::cache::PutRequest* request,
                                pb::cache::PutResponse* response,
                                google::protobuf::Closure* done) {
  Status status;
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto ctx = NewContext(cntl->request_id());
  auto* srv_done = new ServiceClosure(ctx, done, request, response, status);
  brpc::ClosureGuard done_guard(srv_done);

  IOBuffer buffer = IOBuffer(cntl->request_attachment().movable());
  Block block(std::move(buffer));
  status = CheckBodySize(request->block_size(), block.buffer.Size());
  if (status.ok()) {
    status = node_->Put(ctx, BlockKey(request->block_key()), block);
  }
  response->set_status(ToPBErr(status));
}

void BlockCacheServiceImpl::Range(google::protobuf::RpcController* controller,
                                  const pb::cache::RangeRequest* request,
                                  pb::cache::RangeResponse* response,
                                  google::protobuf::Closure* done) {
  Status status;
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto ctx = NewContext(cntl->request_id());
  auto* srv_done = new ServiceClosure(ctx, done, request, response, status);
  brpc::ClosureGuard done_guard(srv_done);

  IOBuffer buffer;
  status = node_->Range(ctx, BlockKey(request->block_key()), request->offset(),
                        request->length(), &buffer, request->block_size());
  if (status.ok()) {
    cntl->response_attachment() = buffer.IOBuf().movable();
  }
  response->set_status(ToPBErr(status));
  response->set_cache_hit(ctx->GetCacheHit());
}

void BlockCacheServiceImpl::Cache(google::protobuf::RpcController* controller,
                                  const pb::cache::CacheRequest* request,
                                  pb::cache::CacheResponse* response,
                                  google::protobuf::Closure* done) {
  Status status;
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto ctx = NewContext(cntl->request_id());
  auto* srv_done = new ServiceClosure(ctx, done, request, response, status);
  brpc::ClosureGuard done_guard(srv_done);

  IOBuffer buffer = IOBuffer(cntl->request_attachment().movable());
  status = CheckBodySize(request->block_size(), buffer.Size());
  if (status.ok()) {
    status = node_->AsyncCache(ctx, BlockKey(request->block_key()),
                               Block(std::move(buffer)));
  }
  response->set_status(ToPBErr(status));
}

void BlockCacheServiceImpl::Prefetch(
    google::protobuf::RpcController* controller,
    const pb::cache::PrefetchRequest* request,
    pb::cache::PrefetchResponse* response, google::protobuf::Closure* done) {
  Status status;
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto ctx = NewContext(cntl->request_id());
  auto* srv_done = new ServiceClosure(ctx, done, request, response, status);
  brpc::ClosureGuard done_guard(srv_done);

  status = node_->AsyncPrefetch(ctx, BlockKey(request->block_key()),
                                request->block_size());
  response->set_status(ToPBErr(status));
}

void BlockCacheServiceImpl::Ping(
    google::protobuf::RpcController* /*controller*/,
    const pb::cache::PingRequest* /*request*/,
    pb::cache::PingResponse* /*response*/, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  // do nothing, just reply
}

}  // namespace cache
}  // namespace dingofs
