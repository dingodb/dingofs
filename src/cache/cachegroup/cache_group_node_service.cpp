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

#include "cache/utils/access_log.h"
#include "cache/utils/phase_timer.h"

namespace dingofs {
namespace cache {

CacheGroupNodeServiceImpl::CacheGroupNodeServiceImpl(CacheGroupNodeSPtr node)
    : node_(node) {}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Put) {
  Status status;
  PhaseTimer timer;
  auto* cntl = static_cast<brpc::Controller*>(controller);
  BlockKey key(request->block_key());
  IOBuffer buffer(cntl->request_attachment());
  LogGuard log([&]() {
    return absl::StrFormat("[service] put(%s,%zu): %s%s", key.Filename(),
                           buffer.Size(), status.ToString(), timer.ToString());
  });

  if (request->block_size() != buffer.Size()) {
    status = Status::InvalidParam("request block body size mismatch");
  } else {
    timer.NextPhase(Phase::kNodePut);
    status = node_->Put(key, Block(buffer));
  }

  timer.NextPhase(Phase::kSendResponse);
  response->set_status(PBErr(status));
  done->Run();
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Range) {
  Status status;
  PhaseTimer timer;
  IOBuffer buffer;
  BlockKey key(request->block_key());
  auto offset = request->offset();
  auto length = request->length();
  auto* cntl = static_cast<brpc::Controller*>(controller);
  LogGuard log([&]() {
    return absl::StrFormat("[service] range(%s,%lld,%zu): %s%s", key.Filename(),
                           offset, length, status.ToString(), timer.ToString());
  });

  timer.NextPhase(Phase::kNodeRange);
  status = node_->Range(key, offset, length, &buffer, request->block_size());
  if (status.ok()) {
    cntl->response_attachment().append(buffer.IOBuf());
  }

  timer.NextPhase(Phase::kSendResponse);
  response->set_status(PBErr(status));
  done->Run();
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Cache) {
  Status status;
  PhaseTimer timer;
  auto* cntl = static_cast<brpc::Controller*>(controller);
  BlockKey key(request->block_key());
  IOBuffer buffer(cntl->request_attachment());
  LogGuard log([&]() {
    return absl::StrFormat("[service] cache(%s,%zu): %s", key.Filename(),
                           buffer.Size(), status.ToString());
  });

  if (request->block_size() != buffer.Size()) {
    status = Status::InvalidParam("request block body size mismatch");
  } else {
    timer.NextPhase(Phase::kNodeCache);
    status = node_->Cache(key, Block(buffer));
  }

  timer.NextPhase(Phase::kSendResponse);
  response->set_status(PBErr(status));
  done->Run();
}

DEFINE_RPC_METHOD(CacheGroupNodeServiceImpl, Prefetch) {  // NOLINT
  Status status;
  PhaseTimer timer;
  BlockKey key(request->block_key());
  auto length = request->block_size();
  LogGuard log([&]() {
    return absl::StrFormat("[local] refetch(%s,%zu): %s%s", key.Filename(),
                           length, status.ToString(), timer.ToString());
  });

  timer.NextPhase(Phase::kNodePrefetch);
  status = node_->Prefetch(key, length);

  timer.NextPhase(Phase::kSendResponse);
  response->set_status(PBErr(status));
  done->Run();
}

}  // namespace cache
}  // namespace dingofs
