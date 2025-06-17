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

#include "cache/cachegroup/cache_group_node_service.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include "cache/blockcache/block_cache.h"
#include "cache/cachegroup/service_closure.h"
#include "cache/utils/context.h"

namespace dingofs {
namespace cache {

CacheGroupNodeServiceImpl::CacheGroupNodeServiceImpl(CacheGroupNodeSPtr node)
    : node_(node) {}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Put) {
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto ctx = NewContext(cntl->request_id());
  auto* srv_done = new ServiceClosure(ctx, __func__, done, request, response);
  brpc::ClosureGuard done_guard(srv_done);

  Status status;
  BlockKey key(request->block_key());
  IOBuffer buffer(cntl->request_attachment());

  if (request->block_size() != buffer.Size()) {
    LOG(ERROR) << "Put request block body size mismatch: key = "
               << key.Filename() << ", expected = " << request->block_size()
               << ", actual = " << buffer.Size();
    response->set_status(
        PBErr(Status::InvalidParam("request block body size mismatch")));
    return;
  } else {
    PutOption option;
    option.writeback = true;
    status = node_->Put(ctx, key, Block(buffer), option);
  }
  response->set_status(PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Range) {
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto ctx = NewContext(cntl->request_id());
  auto* srv_done = new ServiceClosure(ctx, __func__, done, request, response);
  brpc::ClosureGuard done_guard(srv_done);

  Status status;
  IOBuffer buffer;
  BlockKey key(request->block_key());
  auto offset = request->offset();
  auto length = request->length();
  auto block_size = request->block_size();

  RangeOption option;
  option.retrive = true;
  option.block_size = block_size;
  status = node_->Range(ctx, key, offset, length, &buffer, option);
  if (status.ok()) {
    cntl->response_attachment().append(buffer.IOBuf());
  }
  response->set_status(PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Cache) {
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto ctx = NewContext(cntl->request_id());
  auto* srv_done = new ServiceClosure(ctx, __func__, done, request, response);
  brpc::ClosureGuard done_guard(srv_done);

  Status status;
  BlockKey key(request->block_key());
  IOBuffer buffer(cntl->request_attachment());

  if (request->block_size() != buffer.Size()) {
    status = Status::InvalidParam("request block body size mismatch");
  } else {
    status = node_->Cache(ctx, key, Block(buffer));
  }
  response->set_status(PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Prefetch) {  // NOLINT
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto ctx = NewContext(cntl->request_id());
  auto* srv_done = new ServiceClosure(ctx, __func__, done, request, response);
  brpc::ClosureGuard done_guard(srv_done);

  Status status;
  BlockKey key(request->block_key());
  auto length = request->block_size();

  status = node_->Prefetch(ctx, key, length);
  response->set_status(PBErr(status));
}

}  // namespace cache
}  // namespace dingofs
