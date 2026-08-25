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

#ifndef DINGOFS_BLOCKCACHE_NET_BRPC_BRPC_BRIDGE_H_
#define DINGOFS_BLOCKCACHE_NET_BRPC_BRPC_BRIDGE_H_

#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <butil/iobuf.h>
#include <glog/logging.h>

#include <cstdint>
#include <span>
#include <string>
#include <utility>

#include "blockcache/common/route.h"
#include "blockcache/core/memory/buffer.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/runtime/shard_inbox.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/net/brpc/brpc_server.h"
#include "blockcache/net/controller.h"
#include "blockcache/net/types.h"

namespace dingofs {
namespace blockcache {

struct BrpcCall;

struct AttachmentDoneWork : InboxWork {
  BrpcCall* call = nullptr;
};

struct BrpcCall : InboxWork {
  BrpcCall(BrpcServer* s, ::brpc::Controller* c, google::protobuf::Closure* d)
      : server(s), cntl(c), done(d) {
    attachment_work.call = this;
    attachment_work.run = &BrpcCall::OnAttachmentReleasedOnShard;
  }

  virtual ~BrpcCall() {
    CHECK(!attachment_started || attachment_released)
        << "a response attachment was abandoned before brpc was done with it";
  }
  BrpcCall(const BrpcCall&) = delete;
  BrpcCall& operator=(const BrpcCall&) = delete;

  void RunDone() {
    if (done == nullptr) {
      return;
    }
    google::protobuf::Closure* closure = done;
    done = nullptr;
    cntl = nullptr;
    if (server->reply_on_bthread()) {
      bthread_t tid;
      if (bthread_start_background(&tid, nullptr, &BrpcCall::RunDoneOnBthread,
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
    server->CallFinished();
    delete this;
  }

  void Finish() {
    RunDone();
    Retire();
  }

  void FailNow(int code, const std::string& reason) {
    if (cntl != nullptr) {
      cntl->SetFailed(code, "%s", reason.c_str());
    }
    Finish();
  }

  Future<Status> ReplyWithAttachment(BufferView attachment) {
    attachment_started = true;
    Future<Status> released = attachment_done.GetFuture();
    BrpcCall* self = this;
    cntl->response_attachment().append_user_data_with_meta(
        attachment.data, attachment.size,
        [self](void*) { self->OnAttachmentReleased(); }, 0);
    RunDone();
    return released;
  }

  void OnAttachmentReleased() {
    if (IsOnShard(shard)) {
      Release();
      return;
    }
    if (!PostTo(shard, &attachment_work)) {
      LOG(ERROR) << "Fail to release a response attachment to shard " << shard
                 << ": it is stopping";
    }
  }

  void Release() {
    attachment_released = true;
    attachment_done.SetValue(Status::OK());
  }

  static void OnAttachmentReleasedOnShard(InboxWork* base) {
    static_cast<AttachmentDoneWork*>(base)->call->Release();
  }

  BrpcServer* server;
  ::brpc::Controller* cntl;
  google::protobuf::Closure* done;
  unsigned shard = 0;
  AttachmentDoneWork attachment_work;
  Promise<Status> attachment_done;
  bool attachment_started = false;
  bool attachment_released = false;
};

template <typename Req>
uint64_t RouteKeyOf(const Req& request) {
  if constexpr (requires { request.route_key(); }) {
    return request.route_key();
  } else {
    return 0;
  }
}

template <typename S, typename Req, typename Resp>
struct TypedBrpcCall final : BrpcCall {
  using Method = Future<> (S::*)(Controller*, const Req*, Resp*);

  TypedBrpcCall(BrpcServer* srv, ::brpc::Controller* c,
                google::protobuf::Closure* d, S* s, Method m, const Req* req,
                Resp* resp)
      : BrpcCall(srv, c, d),
        service(s),
        method(m),
        request(req),
        response(resp) {
    run = &TypedBrpcCall::OnShard;
  }

  static void OnShard(InboxWork* base) {
    (void)static_cast<TypedBrpcCall*>(base)->Serve();
  }

  Future<Status> Serve() {
    Controller shard_cntl;
    const butil::IOBuf& attached = cntl->request_attachment();
    const auto attached_size = static_cast<uint32_t>(attached.size());
    if (attached_size > kMaxAttachmentBytes) {
      FailNow(EINVAL, "the request attachment is too large");
      co_return Status::OK();
    }
    if (attached_size > 0) {
      Buffer buffer = Buffer::Alloc(attached_size);
      if (buffer.empty()) {
        FailNow(ENOMEM, "no buffer for the request attachment");
        co_return Status::OK();
      }
      attached.copy_to(buffer.view().data, attached_size);
      shard_cntl.request_attachment() = std::move(buffer);
    }
    co_await (service->*method)(&shard_cntl, request, response);

    if (shard_cntl.Failed()) {
      FailNow(shard_cntl.ErrorCode(), shard_cntl.ErrorText());
      co_return Status::OK();
    }
    if (shard_cntl.response_attachment().empty()) {
      Finish();
      co_return Status::OK();
    }
    co_await ReplyWithAttachment(shard_cntl.response_attachment_view());
    Retire();
    co_return Status::OK();
  }

  S* service;
  Method method;
  const Req* request;
  Resp* response;
};

// Builds the call and parks it on `shard`, where `target->*method` serves it.
// `call->shard` must stay set: releasing a response attachment re-posts to it.
template <typename S, typename Req, typename Resp>
void PostCallToShard(BrpcServer* server, unsigned shard, S* target,
                     Future<> (S::*method)(Controller*, const Req*, Resp*),
                     ::brpc::Controller* cntl, const Req* request,
                     Resp* response, google::protobuf::Closure* done) {
  auto* call = new TypedBrpcCall<S, Req, Resp>(server, cntl, done, target,
                                               method, request, response);
  call->shard = shard;
  server->CallStarted();
  if (PostTo(shard, call)) {
    return;
  }
  LOG(ERROR) << "Fail to post an rpc to a shard: the runtime is stopping";
  call->FailNow(EHOSTDOWN, "the runtime is stopping");
}

// A shared service: one stateless instance serves every shard, so the
// route_key only picks WHERE the call runs.
template <typename S, typename Req, typename Resp>
void BridgeToShard(BrpcServer* server, S* service,
                   Future<> (S::*method)(Controller*, const Req*, Resp*),
                   ::brpc::Controller* cntl, const Req* request, Resp* response,
                   google::protobuf::Closure* done) {
  const unsigned shard = ShardOf(RouteKeyOf(*request), ShardCount());
  PostCallToShard(server, shard, service, method, cntl, request, response,
                  done);
}

// Shard-local objects: `locals[i]` lives on shard i and is touched only
// there, so the route_key picks WHICH instance serves as well as where.
template <typename S, typename Req, typename Resp>
void BridgeToShardLocal(BrpcServer* server, std::span<S* const> locals,
                        Future<> (S::*method)(Controller*, const Req*, Resp*),
                        ::brpc::Controller* cntl, const Req* request,
                        Resp* response, google::protobuf::Closure* done) {
  DCHECK_EQ(locals.size(), size_t{ShardCount()}) << "one instance per shard";
  const unsigned shard = ShardOf(RouteKeyOf(*request), ShardCount());
  PostCallToShard(server, shard, locals[shard], method, cntl, request, response,
                  done);
}

}  // namespace blockcache
}  // namespace dingofs

#endif
