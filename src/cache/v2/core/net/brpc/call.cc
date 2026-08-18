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

#include "cache/v2/core/net/brpc/call.h"

#include "cache/v2/core/net/brpc/request.h"
#include "cache/v2/core/reactor/coroutine.h"

namespace dingofs {
namespace cache {
namespace v2 {

// What one request does, once it is on the shard that owns its key.
Future<Status> Serve(ServerCall* call) {
  // An attachment is always caller bytes: kToServer if any, else kNone.
  const auto attached =
      static_cast<uint32_t>(call->cntl->request_attachment().size());
  BrpcRequest request(call, static_cast<Opcode>(call->request->opcode()),
                      call->request->payload(),
                      attached > 0 ? BodyShape::kToServer : BodyShape::kNone,
                      attached);
  co_await call->transport->context().HandlerOf(call->shard)->Serve(request);
  // Dispatch guarantees an answer; this covers a request whose peer died.
  call->RunDone();
  call->Retire();
  co_return Status::OK();
}

void ServerCall::OnShard(InboxWork* base) {
  (void)Serve(static_cast<ServerCall*>(base));
}

void ServerCall::OnBulkReleasedOnShard(InboxWork* base) {
  static_cast<BodyDoneWork*>(base)->call->Release();
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
