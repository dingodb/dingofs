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
#include "cache/blockcache/cache_store.h"
#include "cache/cachegroup/service_closure.h"
#include "cache/common/const.h"
#include "cache/utils/context.h"
#include "cache/utils/step_timer.h"

namespace dingofs {
namespace cache {

CacheGroupNodeServiceImpl::CacheGroupNodeServiceImpl(CacheGroupNodeSPtr node)
    : node_(CHECK_NOTNULL(node)) {}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Put) {
  Status status;
  StepTimer timer;
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto ctx = NewContext(cntl->request_id());
  auto* srv_done =
      new ServiceClosure(ctx, done, request, response, status, timer);
  brpc::ClosureGuard done_guard(srv_done);

  timer.Start();

  BlockKey key(request->block_key());
  IOBuffer buffer(cntl->request_attachment());
  status = CheckBodySize(request->block_size(), buffer.Size());
  if (status.ok()) {
    NEXT_STEP("node_put");
    PutOption option;
    option.writeback = true;
    status = node_->Put(ctx, key, Block(buffer), option);
  }

  response->set_status(PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Range) {
  Status status;
  StepTimer timer;
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto ctx = NewContext(cntl->request_id());
  auto* srv_done =
      new ServiceClosure(ctx, done, request, response, status, timer);
  brpc::ClosureGuard done_guard(srv_done);

  timer.Start();

  NEXT_STEP("node_range");
  IOBuffer buffer;
  status = node_->Range(
      ctx, BlockKey(request->block_key()), request->offset(), request->length(),
      &buffer,
      RangeOption{.retrive = true, .block_size = request->block_size()});

  if (status.ok()) {
    cntl->response_attachment().append(buffer.IOBuf());
  }
  response->set_status(PBErr(status));
  response->set_cache_hit(ctx->GetCacheHit());
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Cache) {
  Status status;
  StepTimer timer;
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto ctx = NewContext(cntl->request_id());
  auto* srv_done =
      new ServiceClosure(ctx, done, request, response, status, timer);
  brpc::ClosureGuard done_guard(srv_done);

  timer.Start();

  IOBuffer buffer(cntl->request_attachment());
  status = CheckBodySize(request->block_size(), buffer.Size());
  if (status.ok()) {
    NEXT_STEP("node_async_cache");
    node_->AsyncCache(ctx, BlockKey(request->block_key()), Block(buffer),
                      [](Status) {});
  }

  response->set_status(PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Prefetch) {  // NOLINT
  Status status;
  StepTimer timer;
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto ctx = NewContext(cntl->request_id());
  auto* srv_done =
      new ServiceClosure(ctx, done, request, response, status, timer);
  brpc::ClosureGuard done_guard(srv_done);

  timer.Start();

  NEXT_STEP("node_async_prefetch");
  node_->AsyncPrefetch(ctx, BlockKey(request->block_key()),
                       request->block_size(), [](Status) {});

  response->set_status(PBErr(Status::OK()));
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Ping) {  // NOLINT
  brpc::ClosureGuard done_guard(done);
}

Status CacheGroupNodeServiceImpl::CheckBodySize(size_t request_block_size,
                                                size_t body_size) {
  if (request_block_size != body_size) {
    LOG(ERROR) << "Request body size mismatch: expected = "
               << request_block_size << ", actual = " << body_size;
    return Status::InvalidParam("request body size mismatch");
  }
  return Status::OK();
}

}  // namespace cache
}  // namespace dingofs
