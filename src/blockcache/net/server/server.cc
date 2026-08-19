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

#include "blockcache/net/server/server.h"

#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>

#include "blockcache/core/memory/buffer.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/net/rdma/domain.h"
#include "blockcache/net/rdma/handshake.h"
#include "blockcache/net/rdma/transport.h"

namespace dingofs {
namespace blockcache {

namespace {

std::atomic<bool> g_asked_to_quit{false};

void OnQuitSignal(int /*signo*/) {
  g_asked_to_quit.store(true, std::memory_order_relaxed);
}

// Framework's own verb; reaches the domain via late-bound Serving() slot.
class RdmaHandshakeService final : public Service {
 public:
  RdmaHandshakeService() {
    AddMethod(kOpRdmaHandshake, &RdmaHandshakeService::Handshake);
  }

 private:
  Future<> Handshake(Controller* cntl, const HandshakeMsg* peer,
                     HandshakeMsg* mine) {
    RdmaDomain* domain = RdmaDomain::Serving();
    if (domain == nullptr) {
      cntl->SetFailed("rdma is not serving on this shard");
      co_return;
    }
    const StatusOr<RdmaConnection*> conn = co_await domain->Accept(*peer, mine);
    if (!conn.ok()) {
      cntl->SetFailed(conn.status().ToString());
    }
    co_return;
  }
};

}  // namespace

void ServiceTable::Add(std::unique_ptr<Service> service) {
  service->VerifyBound();
  for (size_t opcode = 0; opcode < service->method_limit(); ++opcode) {
    if (!service->Has(static_cast<Opcode>(opcode))) {
      continue;
    }
    if (methods_.size() <= opcode) {
      methods_.resize(opcode + 1);
    }
    CHECK(methods_[opcode] == nullptr)
        << "two services claim opcode " << opcode;
    methods_[opcode] = service->MethodOf(static_cast<Opcode>(opcode));
  }
  services_.push_back(std::move(service));
}

Future<Status> ServiceTable::Answer(Request& request, ReplyCode code) {
  // A second reply would run brpc's done twice and free a recv slot twice.
  if (request.replied() || !request.alive()) {
    return MakeReadyFuture<Status>(Status::OK());
  }
  return request.Reply(code, {});
}

Future<Status> ServiceTable::Verdict(Request& request, Status status) {
  if (!status.ok()) {
    // The caller only ever sees a reply code; log the reason here.
    LOG_EVERY_N(ERROR, 100) << "Fail to serve opcode " << request.opcode()
                            << ": " << status.ToString();
    return Answer(request, kReplyHandlerError);
  }
  // A method that never replied would hang the peer; answer with an error.
  return request.replied() ? MakeReadyFuture<Status>(Status::OK())
                           : Answer(request, kReplyHandlerError);
}

// `request` outlives every suspension by RequestHandler's contract.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
Future<Status> ServiceTable::Finish(Future<Status> served, Request& request) {
  Status status = co_await std::move(served);
  co_return co_await Verdict(request, std::move(status));
}

Future<Status> ServiceTable::Serve(Request& request) {
  const Opcode opcode = request.opcode();
  if (opcode >= methods_.size() || methods_[opcode] == nullptr) {
    return Answer(request, kReplyBadOpcode);
  }

  Future<Status> served = methods_[opcode](request);
  if (!served.Available()) {
    return Finish(std::move(served), request);
  }
  return Verdict(request, served.Get());
}

Server::Server(ServerOption option) : option_(std::move(option)) {}

Server::~Server() { Shutdown(); }

void Server::AddTransport(std::unique_ptr<ServerTransport> transport) {
  CHECK(!started_) << "AddTransport after Start()";
  transports_.push_back(std::move(transport));
}

Status Server::Start() {
  CHECK(!started_) << "Server started twice";
  CHECK(!factories_.empty()) << "Server has no service";

  LOG(INFO) << "Server is starting...";

  // Failed Start leaves the server as constructed; the runtime stays up, it
  // is the caller's.
  const auto fail = [this](Status status) {
    DropTables();
    return status;
  };

  // Pools first: they make a handler's file buffer and wire buffer the same.
  Status status = BufferPool::InitOnAllShards(option_.buffer_pool_bytes);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start the buffer pool: " << status.ToString();
    return fail(status);
  }

  // Every shard's table exists before any transport accepts.
  status = BuildTables();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start the service tables: " << status.ToString();
    return fail(status);
  }

  RdmaTransport* rdma = nullptr;
  if (RdmaOn()) {
    auto transport = std::make_unique<RdmaTransport>(option_.rdma);
    rdma = transport.get();
    // FIRST: Serving() slots exist before any wire can deliver a handshake.
    transports_.insert(transports_.begin(), std::move(transport));
    own_rdma_ = rdma;
  }

  const ServerContext context{.handlers = handlers_};

  for (size_t i = 0; i < transports_.size(); ++i) {
    status = transports_[i]->Start(context);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to start the " << transports_[i]->name()
                 << " transport: " << status.ToString();
      for (size_t j = i; j > 0; --j) {
        transports_[j - 1]->Shutdown();
      }
      // The rdma transport was this Start's own; a retry would add a second.
      if (rdma != nullptr) {
        transports_.erase(transports_.begin());
        own_rdma_ = nullptr;
      }
      return fail(status);
    }
    LOG(INFO) << "Successfully start the " << transports_[i]->name()
              << " transport";
  }

  started_ = true;

  // Zero-copy link: each shard's buffer pool becomes one MR on its device.
  // Both halves happen on the shard, so pinning the pools costs one round trip
  // for the whole machine rather than two per shard.
  if (rdma != nullptr) {
    status = RunOnAllAndWait([rdma](unsigned shard) -> Future<Status> {
      SlabPool* pool = BufferPool::LocalPool();
      if (pool == nullptr) {
        return MakeReadyFuture<Status>(Status::OK());
      }
      return MakeReadyFuture<Status>(
          rdma->RegisterShardMemory(shard, pool->base(), pool->total_bytes()));
    });
    if (!status.ok()) {
      LOG(ERROR)
          << "Fail to register the shard buffer pool with the rdma device: "
          << status.ToString();
      Shutdown();
      return status;
    }
  }

  LOG(INFO) << "Successfully start Server";
  return Status::OK();
}

void Server::Shutdown() {
  if (!started_) {
    return;
  }

  LOG(INFO) << "Server is shutting down...";

  started_ = false;

  // Transports reverse-order, fully drained, THEN tables: else in-flight UAF.
  for (size_t i = transports_.size(); i > 0; --i) {
    transports_[i - 1]->Shutdown();
  }
  // User transports survive Shutdown; only Start()'s own rdma transport goes.
  if (!transports_.empty() && transports_.front().get() == own_rdma_) {
    transports_.erase(transports_.begin());
  }
  own_rdma_ = nullptr;
  DropTables();
  // The buffer pools and the shards themselves belong to whoever started the
  // runtime; a Server only ever borrows them.

  LOG(INFO) << "Successfully shutdown Server";
}

Status Server::BuildTables() {
  const unsigned shards = ShardCount();
  tables_.resize(shards);
  handlers_.assign(shards, nullptr);

  // Built ON the shard: allocated and later freed by the same core.
  return RunOnAllAndWait([this](unsigned shard) -> Future<Status> {
    auto table = std::make_unique<ServiceTable>();
    for (const Factory& factory : factories_) {
      std::unique_ptr<Service> service = factory();
      service->set_shard(shard);
      table->Add(std::move(service));
    }
    if (RdmaOn()) {
      auto handshake = std::make_unique<RdmaHandshakeService>();
      handshake->set_shard(shard);
      table->Add(std::move(handshake));
    }
    handlers_[shard] = table.get();
    tables_[shard] = std::move(table);
    return MakeReadyFuture<Status>(Status::OK());
  });
}

void Server::DropTables() {
  if (!tables_.empty()) {
    RunOnAllAndWait([this](unsigned shard) -> Future<> {
      tables_[shard].reset();
      return MakeReadyFuture<>();
    });
  }
  tables_.clear();
  handlers_.clear();
}

void Server::RunUntilAskedToQuit() {
  g_asked_to_quit.store(false, std::memory_order_relaxed);
  (void)std::signal(SIGINT, OnQuitSignal);
  (void)std::signal(SIGTERM, OnQuitSignal);

  while (!g_asked_to_quit.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  (void)std::signal(SIGINT, SIG_DFL);
  (void)std::signal(SIGTERM, SIG_DFL);

  LOG(INFO) << "Asked to quit, stopping the server";
  Shutdown();
}

Status Server::Run() {
  Status status = Start();
  if (!status.ok()) {
    return status;
  }
  RunUntilAskedToQuit();
  return Status::OK();
}

}  // namespace blockcache
}  // namespace dingofs
