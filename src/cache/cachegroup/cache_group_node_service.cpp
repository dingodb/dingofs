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

namespace dingofs {
namespace cache {

CacheGroupNodeServiceImpl::CacheGroupNodeServiceImpl(CacheGroupNodeSPtr node)
    : node_(node) {}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Put) {
  brpc::ClosureGuard done_guard(done);

  auto* cntl = static_cast<brpc::Controller*>(controller);
  BlockKey block_key(request->block_key());
  IOBuffer buffer(cntl->request_attachment());

  Status status;
  if (request->block_size() != buffer.Size()) {
    status = Status::InvalidParam("request block body size mismatch");
  } else {
    status = node_->Put(block_key, Block(buffer));
  }

  response->set_status(PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Range) {
  brpc::ClosureGuard done_guard(done);

  IOBuffer buffer;
  BlockKey block_key(request->block_key());
  auto* cntl = static_cast<brpc::Controller*>(controller);

  auto status = node_->Range(block_key, request->offset(), request->length(),
                             &buffer, request->block_size());
  if (status.ok()) {
    cntl->response_attachment().append(buffer.IOBuf());
  }

  response->set_status(PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Cache) {
  brpc::ClosureGuard done_guard(done);

  auto* cntl = static_cast<brpc::Controller*>(controller);
  BlockKey block_key(request->block_key());
  IOBuffer buffer(cntl->request_attachment());

  Status status;
  if (request->block_size() != buffer.Size()) {
    status = Status::InvalidParam("request block body size mismatch");
  } else {
    status = node_->Cache(block_key, Block(buffer));
  }

  response->set_status(PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Prefetch) {  // NOLINT
  brpc::ClosureGuard done_guard(done);

  BlockKey block_key(request->block_key());

  auto status = node_->Prefetch(block_key, request->block_size());

  response->set_status(PBErr(status));
}

}  // namespace cache
}  // namespace dingofs
