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

#include "blockcache/net/brpc/transport.h"

#include <glog/logging.h>

#include <string>
#include <thread>
#include <utility>

#include "blockcache/core/runtime/smp.h"
#include "blockcache/net/brpc/brpc_headers.h"
#include "blockcache/net/brpc/call.h"
#include "blockcache/net/router.h"
#include "transport.pb.h"

namespace dingofs {
namespace blockcache {

// Opcodes above the dispatch-table range are rejected, never masked down.
constexpr uint32_t kMaxOpcode = 0xffff;

class TransportServiceImpl : public pb::cache::v2::RpcTransport {
 public:
  explicit TransportServiceImpl(BrpcTransport* owner) : owner_(owner) {}

  void Call(google::protobuf::RpcController* cntl_base,
            const pb::cache::v2::RpcRequest* request,
            pb::cache::v2::RpcResponse* response,
            google::protobuf::Closure* done) override {
    if (request->opcode() > kMaxOpcode) {
      ::brpc::ClosureGuard guard(done);
      response->set_code(kReplyBadOpcode);
      return;
    }
    // No ClosureGuard: `done` now belongs to the shard.
    auto* cntl = static_cast<::brpc::Controller*>(cntl_base);
    auto* call = new ServerCall(owner_, cntl, request, response, done);
    owner_->CallStarted();
    const ServerContext& context = owner_->context();
    // The server-wide routing rule: same shard for a key over either wire.
    const unsigned shard = RouteToShard(
        RequestRoute{.opcode = static_cast<Opcode>(request->opcode()),
                     .route_hint = request->route()},
        context.shard_count());
    // Remembered so a body reply released elsewhere knows where to go back.
    call->shard = shard;
    if (PostTo(shard, call)) {
      return;
    }
    // Only reachable if the runtime went down first; answer, don't hang peer.
    LOG(ERROR) << "Fail to post an rpc to a shard: the runtime is stopping";
    call->ReplyNow(kReplyHandlerError);
  }

 private:
  BrpcTransport* owner_;
};

BrpcTransport::BrpcTransport(Option option) : option_(std::move(option)) {}

BrpcTransport::~BrpcTransport() { Shutdown(); }

Status BrpcTransport::Start(const ServerContext& context) {
  CHECK(!started_) << "BrpcTransport already started";
  context_ = context;

  impl_ = std::make_unique<TransportServiceImpl>(this);
  server_ = std::make_unique<::brpc::Server>();
  if (server_->AddService(impl_.get(), ::brpc::SERVER_DOESNT_OWN_SERVICE) !=
      0) {
    return Status::Internal("Fail to add the rpc transport service");
  }

  butil::EndPoint listen_addr;
  if (butil::str2endpoint(option_.listen_ip.c_str(), option_.listen_port,
                          &listen_addr) != 0) {
    return Status::InvalidParam("Fail to parse the listen address: " +
                                option_.listen_ip);
  }

  ::brpc::ServerOptions options;
  options.idle_timeout_sec = option_.idle_timeout_sec;
  options.max_concurrency = option_.max_concurrency;
  if (server_->Start(listen_addr, &options) != 0) {
    return Status::Internal("Fail to start the brpc server");
  }

  started_ = true;
  return Status::OK();
}

void BrpcTransport::Shutdown() {
  if (!started_) {
    return;
  }
  started_ = false;

  // Shutdown accepting first, then drain; joining before draining deadlocks.
  server_->Stop(0);
  DrainInflight(inflight_);
  server_->Join();
  server_.reset();
  impl_.reset();
}

}  // namespace blockcache
}  // namespace dingofs
