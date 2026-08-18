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

#ifndef DINGOFS_CACHE_V2_CORE_NET_BRPC_CALL_H_
#define DINGOFS_CACHE_V2_CORE_NET_BRPC_CALL_H_

#include <glog/logging.h>

#include "cache/v2/core/net/brpc/brpc_headers.h"
#include "cache/v2/core/net/brpc/transport.h"
#include "cache/v2/core/net/types.h"
#include "cache/v2/core/runtime/shard_inbox.h"
#include "cache/v2/core/runtime/smp.h"
#include "transport.pb.h"

namespace dingofs {
namespace cache {
namespace v2 {

struct ServerCall;

// Carries a finished response body back to the shard that owns it.
struct BodyDoneWork : InboxWork {
  ServerCall* call = nullptr;
};

// One request, from the receiving bthread to the shard that answers it.
struct ServerCall : InboxWork {
  ServerCall(BrpcTransport* t, ::brpc::Controller* c,
             const pb::cache::v2::RpcRequest* req,
             pb::cache::v2::RpcResponse* resp, google::protobuf::Closure* d)
      : transport(t), cntl(c), request(req), response(resp), done(d) {
    run = &ServerCall::OnShard;
    body_work.call = this;
    body_work.run = &ServerCall::OnBulkReleasedOnShard;
  }

  ~ServerCall() {
    // INV-2: a response body must outlive brpc's write from its buffer.
    CHECK(!body_started || body_released)
        << "a response body was abandoned before the transport was done with "
           "it";
  }

  // Runs on the shard the request was routed to: spawns the handler.
  static void OnShard(InboxWork* base);
  static void OnBulkReleasedOnShard(InboxWork* base);

  // Hands the answer to brpc; idempotent.
  void RunDone() {
    if (done == nullptr) {
      return;
    }
    google::protobuf::Closure* closure = done;
    // Everything below belongs to brpc and dies with the closure.
    done = nullptr;
    cntl = nullptr;
    request = nullptr;
    response = nullptr;
    if (transport->reply_on_bthread()) {
      bthread_t tid;
      // Only the closure crosses over: this object may be gone by then.
      if (bthread_start_background(&tid, nullptr, &ServerCall::RunDoneOnBthread,
                                   closure) == 0) {
        return;
      }
      LOG(ERROR) << "Fail to start a bthread for a reply; replying inline";
    }
    closure->Run();
  }

  static void* RunDoneOnBthread(void* arg) {
    static_cast<google::protobuf::Closure*>(arg)->Run();
    return nullptr;
  }

  void Retire() {
    transport->CallFinished();
    delete this;
  }

  // The whole error path in one move: answer `code`, run done, retire.
  void ReplyNow(ReplyCode code) {
    response->set_code(code);
    RunDone();
    Retire();
  }

  // Runs on an unpredictable thread; the buffer is shard-owned, so hop.
  void OnBulkReleased() {
    if (IsOnShard(shard)) {
      Release();
      return;
    }
    if (!PostTo(shard, &body_work)) {
      LOG(ERROR) << "Fail to release a response body to shard " << shard
                 << ": it is stopping";
    }
  }

  void Release() {
    body_released = true;
    body_done.SetValue(Status::OK());
  }

  BrpcTransport* transport;
  ::brpc::Controller* cntl;
  const pb::cache::v2::RpcRequest* request;
  pb::cache::v2::RpcResponse* response;
  google::protobuf::Closure* done;
  unsigned shard = 0;
  BodyDoneWork body_work;
  Promise<Status> body_done;
  bool body_started = false;
  bool body_released = false;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_BRPC_CALL_H_
