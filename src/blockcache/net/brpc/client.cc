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

#include "blockcache/net/brpc/client.h"

#include <glog/logging.h>

#include <cerrno>
#include <string>
#include <thread>
#include <utility>

#include "blockcache/common/status.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/core/runtime/worker_pool.h"
#include "blockcache/net/brpc/brpc_headers.h"
#include "blockcache/net/brpc/transport.h"
#include "transport.pb.h"

namespace dingofs {
namespace blockcache {

namespace pbv2 = ::dingofs::pb::cache::v2;

// One call in flight: brpc's completion object plus foreign work for the hop.
struct ClientCall : InboxWork, public google::protobuf::Closure {
  explicit ClientCall(BrpcClient* c) : client(c) { run = &ClientCall::OnShard; }

  // brpc, on a bthread: just the hop to the shard.
  void Run() override {
    if (PostTo(client->shard(), this)) {
      return;
    }
    // The shard is gone; nobody can resolve the promise -- just don't leak.
    LOG(ERROR) << "Fail to deliver an rpc reply to shard " << client->shard()
               << ": it is stopping";
    client->CallFinished();
    delete this;
  }

  // The shard that made the call. This transport is the one that pays a
  // memcpy to land the body (rdma writes straight into the destination), so
  // this is where that copy is decided -- and, when it is big enough to be
  // worth the handover, where it leaves for the cpu lane.
  static void OnShard(InboxWork* base) {
    auto* self = static_cast<ClientCall*>(base);
    if (self->cntl.Failed() || self->recv.data == nullptr) {
      self->Finish();
      return;
    }

    const butil::IOBuf& body = self->cntl.response_attachment();
    self->copy_bytes =
        body.size() < self->recv.size ? body.size() : self->recv.size;

    WorkerPool* workers = GetGlobalWorkers();
    if (workers == nullptr || !WorkerPool::ShouldOffload(self->copy_bytes)) {
      body.copy_to(self->recv.data, self->copy_bytes);
      self->Finish();
      return;
    }
    // The call stays counted as in-flight across the detour, so Shutdown()
    // cannot take the client away while the copy is still running.
    workers->CountCopy(self->copy_bytes);
    self->run = &ClientCall::OnCopied;
    workers->Post(self);
  }

  // Cpu worker: the copy, then straight back to the owning shard. brpc is
  // done with the controller by now, so reading its blocks here is ours.
  static void OnCopied(InboxWork* base) {
    auto* self = static_cast<ClientCall*>(base);
    self->cntl.response_attachment().copy_to(self->recv.data, self->copy_bytes);
    self->run = &ClientCall::OnResolve;
    if (PostTo(self->client->shard(), self)) {
      return;
    }
    LOG(ERROR) << "Fail to deliver a copied rpc reply to shard "
               << self->client->shard() << ": it is stopping";
    self->client->CallFinished();
    delete self;
  }

  static void OnResolve(InboxWork* base) {
    static_cast<ClientCall*>(base)->Finish();
  }

  // Answers the caller and retires the call; the last touch of `client`.
  void Finish() {
    client->CallFinished();
    if (cntl.Failed()) {
      // brpc's error numbers are their own domain; they ride in the text.
      promise.SetValue(StatusOr<Reply>(Status::NetError(
          "Fail to complete an rpc (brpc error " +
          std::to_string(cntl.ErrorCode()) + "): " + cntl.ErrorText())));
      delete this;
      return;
    }
    // The reply owns this object and frees it when the caller is done.
    promise.SetValue(
        StatusOr<Reply>(Reply(static_cast<ReplyCode>(response.code()),
                              response.payload(), &ClientCall::Release, this)));
  }

  static void Release(void* call, uint64_t /*unused*/) {
    delete static_cast<ClientCall*>(call);
  }

  BrpcClient* client;
  BufferView recv;  // neutral destination, filled on the owning shard
  size_t copy_bytes = 0;
  ::brpc::Controller cntl;
  pbv2::RpcRequest request;
  pbv2::RpcResponse response;
  Promise<StatusOr<Reply>> promise;
};

BrpcClient::BrpcClient(unsigned shard, const Option& option)
    : shard_(shard), option_(option) {}

BrpcClient::~BrpcClient() { Shutdown(); }

StatusOr<std::unique_ptr<BrpcClient>> BrpcClient::Create(unsigned shard,
                                                         const Option& option) {
  std::unique_ptr<BrpcClient> client(new BrpcClient(shard, option));

  ::brpc::ChannelOptions options;
  options.connect_timeout_ms = option.connect_timeout_ms;
  options.timeout_ms = option.timeout_ms;
  options.max_retry = option.max_retry;
  // "single" + a distinct group: one un-shared socket per shard.
  options.connection_type = "single";
  options.connection_group = option.connection_group;

  client->channel_ = std::make_unique<::brpc::Channel>();
  if (client->channel_->Init(option.server.c_str(), &options) != 0) {
    return Status::NetError("Fail to connect to " + option.server);
  }
  return client;
}

void BrpcClient::Shutdown() {
  DrainInflight(inflight_);
  channel_.reset();
}

Future<StatusOr<Reply>> BrpcClient::Call(Opcode opcode,
                                         std::string_view payload, Body body,
                                         uint64_t route_hint) {
  // The same misuse fails the same way on either transport.
  if (!body.none() && body.empty()) {
    return MakeReadyFuture<StatusOr<Reply>>(
        ToStatus(EINVAL, "send a request: the body is empty"));
  }

  auto* call = new ClientCall(this);
  call->request.set_opcode(opcode);
  call->request.set_payload(payload.data(), payload.size());
  call->request.set_route(route_hint);

  if (body.shape() == BodyShape::kToServer) {
    // Zero copy on the way out: brpc writes from the caller's ranges, in
    // order, while it waits here. Only the way back costs a memcpy.
    for (const BufferView& range : body.source()) {
      call->cntl.request_attachment().append_user_data_with_meta(
          range.data, range.size, [](void*) {}, 0);
    }
  } else if (body.shape() == BodyShape::kToClient) {
    call->recv = body.destination();
  }

  Future<StatusOr<Reply>> reply = call->promise.GetFuture();
  CallStarted();

  pbv2::RpcTransport_Stub stub(channel_.get());
  stub.Call(&call->cntl, &call->request, &call->response, call);
  return reply;
}

}  // namespace blockcache
}  // namespace dingofs
