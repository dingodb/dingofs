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

#include "blockcache/remote/node.h"

#include <butil/memory/scope_guard.h>
#include <glog/logging.h>

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "blockcache/common/route.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/net/controller.h"

namespace dingofs {
namespace blockcache {

template <typename Method>
struct RpcTraits;

template <typename Request, typename Response>
struct RpcTraits<Future<Status> (CacheStub::*)(Controller*, const Request*,
                                               Response*)> {
  using RequestType = Request;
  using ResponseType = Response;
};

RemoteNode::RemoteNode(CacheGroupMember member)
    : member_(std::move(member)),
      connections_(std::make_unique<NodeConnections>(member_)),
      breaker_(std::make_unique<CircuitBreaker>()) {}

RemoteNode::~RemoteNode() {
  LOG_IF(WARNING, running_) << "RemoteNode destroyed without Shutdown()";
}

Future<Status> RemoteNode::Start() {
  Gate::Holder holder(gate_);
  if (!holder.ok()) {
    co_return Status::CacheDown("remote node is down");
  }
  running_ = true;

  LOG(INFO) << *this << " is starting...";

  const Status status = co_await connections_->Start();
  LOG_IF(INFO, status.ok()) << "Successfully start " << *this;
  co_return status;
}

Future<> RemoteNode::Shutdown() {
  if (!running_) {
    co_return;
  }
  running_ = false;

  LOG(INFO) << *this << " is shutting down...";

  co_await gate_.Close();
  co_await connections_->Shutdown();

  LOG(INFO) << "Successfully shutdown " << *this;
}

Future<Status> RemoteNode::Put(BlockHandle handle, BufferViews block,
                               bool stage) {
  pb::blockcache::PutRequest request;
  handle.ToPb(request.mutable_handle());
  request.set_stage(stage);
  return SendRequest(&CacheStub::Put, RouteHintOf(handle), std::move(request),
                     Attachment{.send = block});
}

Future<Status> RemoteNode::Get(BlockHandle handle, uint64_t offset,
                               uint32_t length, char* buffer) {
  pb::blockcache::GetRequest request;
  handle.ToPb(request.mutable_handle());
  request.set_offset(offset);
  request.set_length(length);
  return SendRequest(&CacheStub::Get, RouteHintOf(handle), std::move(request),
                     Attachment{.receive = BufferView(buffer, length)});
}

Future<Status> RemoteNode::Prefetch(BlockHandle handle) {
  pb::blockcache::PrefetchRequest request;
  handle.ToPb(request.mutable_handle());
  return SendRequest(&CacheStub::Prefetch, RouteHintOf(handle),
                     std::move(request), Attachment{});
}

Future<Status> RemoteNode::Delete(BlockHandle handle) {
  pb::blockcache::DeleteRequest request;
  handle.ToPb(request.mutable_handle());
  return SendRequest(&CacheStub::Delete, RouteHintOf(handle),
                     std::move(request), Attachment{});
}

template <typename Method, typename Request>
Future<Status> RemoteNode::SendRequest(Method method, uint64_t key,
                                       Request request, Attachment attachment) {
  Gate::Holder holder(gate_);
  if (!holder.ok()) {
    co_return Status::CacheDown("remote node is down");
  }

  if (!breaker_->AllowRequest()) {
    co_return Status::CacheUnhealthy("RemoteNode{id=" + member_.id +
                                     "} is isolated");
  }

  Status status;
  BRPC_SCOPE_EXIT { breaker_->OnCallEnd(status); };

  StatusOr<CacheStub*> stub = co_await connections_->Get(key);
  if (!stub.ok()) {
    status = stub.status();
    co_return status;
  }

  if constexpr (requires { request.set_route_key(key); }) {
    request.set_route_key(key);
  }

  Controller cntl;
  if (!attachment.send.empty()) {
    cntl.set_borrowed_request_attachment(attachment.send);
  } else if (attachment.receive.data != nullptr) {
    cntl.set_borrowed_response_attachment(attachment.receive);
  }

  typename RpcTraits<Method>::ResponseType response;
  status = co_await (stub.value()->*method)(&cntl, &request, &response);
  if (status.ok()) {
    status = ToStatus(response.status());
  }
  co_return status;
}

std::ostream& operator<<(std::ostream& os, const RemoteNode& node) {
  const CacheGroupMember& member = node.member();
  os << "RemoteNode{id=" << member.id << " endpoint=" << member.ip << ":"
     << member.port << " shard=" << ThisShardId() << "}";
  return os;
}

}  // namespace blockcache
}  // namespace dingofs
