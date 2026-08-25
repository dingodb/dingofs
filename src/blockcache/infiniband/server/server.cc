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

#include "blockcache/infiniband/server/server.h"

#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "blockcache/common/flag_decls.h"
#include "blockcache/common/status.h"
#include "blockcache/core/memory/buffer.h"
#include "blockcache/core/memory/shard_allocator.h"
#include "blockcache/core/memory/slab_pool.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/connection/connection.h"
#include "blockcache/infiniband/connection/poller.h"
#include "blockcache/net/brpc/brpc_bridge.h"
#include "blockcache/net/brpc/brpc_server.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

static size_t MessagePoolSuperblockCount() {
  const size_t bytes_per_connection =
      2 * size_t{Protocol::MessageBudget()} * size_t{FLAGS_rdma_message_bytes};
  const size_t total_bytes = bytes_per_connection * FLAGS_rdma_max_connections;
  return ((total_bytes + SlabPool::kSuperblockSize - 1) /
          SlabPool::kSuperblockSize) +
         1;
}

class InfinibandServiceImpl final : public pb::blockcache::InfinibandService {
 public:
  InfinibandServiceImpl(BrpcServer* server,
                        std::vector<HandshakeHandler*> handshake_handlers)
      : server_(server), handshake_handlers_(std::move(handshake_handlers)) {}

  void Handshake(google::protobuf::RpcController* cntl,
                 const pb::blockcache::HandshakeRequest* request,
                 pb::blockcache::HandshakeResponse* response,
                 google::protobuf::Closure* done) override {
    BridgeToShardLocal(
        server_, std::span<HandshakeHandler* const>(handshake_handlers_),
        &HandshakeHandler::Handshake, static_cast<::brpc::Controller*>(cntl),
        request, response, done);
  }

 private:
  BrpcServer* server_;
  std::vector<HandshakeHandler*> handshake_handlers_;
};

Server::Server(ServerOption option) : option_(std::move(option)) {}

Server::~Server() { Shutdown(); }

void Server::AddService(Service* service) {
  CHECK(!running_) << "AddService after Start";
  services_.push_back(service);
}

Status Server::Start() {
  CHECK(!running_) << "Server already started";
  CHECK(option_.brpc_server != nullptr) << "brpc_server is required";

  LOG(INFO) << "InfinibandServer is starting...";

  Status status = StartShards();
  if (!status.ok()) {
    ShutdownShards();
    return status;
  }

  std::vector<HandshakeHandler*> handshake_handlers;
  handshake_handlers.reserve(shards_.size());
  for (const ShardContextUPtr& context : shards_) {
    handshake_handlers.push_back(context->handshake_handler.get());
  }
  option_.brpc_server->AddService(std::make_unique<InfinibandServiceImpl>(
      option_.brpc_server, std::move(handshake_handlers)));

  running_ = true;
  LOG(INFO) << "Successfully start InfinibandServer{device="
            << option_.device_name << ", shards=" << ShardCount() << "}";
  return Status::OK();
}

void Server::Shutdown() {
  if (!running_) {
    return;
  }
  running_ = false;

  LOG(INFO) << "InfinibandServer is shutting down...";

  ShutdownShards();

  LOG(INFO) << "Successfully shutdown InfinibandServer";
}

Status Server::StartShards() {
  const unsigned shards = ShardCount();
  shards_.resize(shards);
  return RunOnAllAndWait(
      [this](unsigned shard) -> Future<Status> { return StartShard(shard); });
}

Future<Status> Server::StartShard(unsigned shard) {
  auto context = std::make_unique<ShardContext>();

  // device
  {
    StatusOr<Device*> device = Device::Open(option_.device_name);
    if (!device.ok()) {
      co_return device.status();
    }
    context->device = device.value();
  }

  // memory_registry
  {
    context->memory_registry =
        std::make_unique<MemoryRegistry>(context->device->pd());

    SlabPool* local = blockcache::BufferPool::LocalPool();
    if (local == nullptr) {
      co_return Status::Internal("buffer pool is not initialized");
    }

    StatusOr<const MemoryRegion*> mr =
        context->memory_registry->Register(local->base(), local->total_bytes());
    if (!mr.ok()) {
      co_return mr.status();
    }
  }

  // buffer_pool
  {
    SlabPoolOption option;
    option.superblock_count = MessagePoolSuperblockCount();
    option.numa_node = memory::LocalNumaNode();
    context->buffer_pool = std::make_unique<BufferPool>(option);
    Status status = context->buffer_pool->Init(context->memory_registry.get());
    if (!status.ok()) {
      co_return status;
    }
  }

  // completion_channel
  {
    StatusOr<CompletionChannel> completion_channel =
        CompletionChannel::Create(*context->device);
    if (!completion_channel.ok()) {
      co_return completion_channel.status();
    }
    context->completion_channel = std::make_unique<CompletionChannel>(
        std::move(completion_channel).value());
  }

  // completion_queue
  {
    StatusOr<CompletionQueue> completion_queue =
        CompletionQueue::Create(*context->device, *context->completion_channel);
    if (!completion_queue.ok()) {
      co_return completion_queue.status();
    }
    context->completion_queue =
        std::make_unique<CompletionQueue>(std::move(completion_queue).value());
  }

  // poller
  {
    context->poller = std::make_unique<InfinibandPoller>(
        context->completion_queue.get(), context->completion_channel->fd());
  }

  // service_registry
  {
    context->service_registry = std::make_unique<ServiceRegistry>();
    for (Service* service : services_) {
      context->service_registry->Add(service);
    }
  }

  // listener
  {
    context->listener =
        std::make_unique<Listener>(context->device, context->buffer_pool.get(),
                                   context->completion_queue.get());
  }

  // session_manager
  {
    context->session_manager = std::make_unique<SessionManager>(
        context->device, context->completion_queue.get());
    context->session_manager->Start();
  }

  // handshake_handler
  {
    context->handshake_handler = std::make_unique<HandshakeHandler>(
        context->listener.get(), context->session_manager.get(),
        context->service_registry.get());
  }

  shards_[shard] = std::move(context);
  co_return Status::OK();
}

void Server::ShutdownShards() {
  if (shards_.empty()) {
    return;
  }

  RunOnAllAndWait(
      [this](unsigned shard) -> Future<> { return ShutdownShard(shard); });
  shards_.clear();
}

Future<> Server::ShutdownShard(unsigned shard) {
  ShardContext* context = shards_[shard].get();
  if (context == nullptr) {
    co_return;
  }

  co_await context->session_manager->Shutdown();
  context->handshake_handler.reset();
  context->session_manager.reset();
  context->listener.reset();
  co_await context->poller->Disarm();

  shards_[shard].reset();
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
