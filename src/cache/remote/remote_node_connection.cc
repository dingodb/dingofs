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
 * Created Date: 2025-01-21
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remote/remote_node_connection.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <memory>
#include <string>

#include "cache/infiniband/client.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/memory.h"
#include "common/options/cache.h"
#include "dingofs/blockcache.pb.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {

RemoteNodeConnectionUPtr RemoteNodeConnection::New() {
  if (FLAGS_use_rdma) {
    return std::make_unique<RDMAConnection>();
  }
  return std::make_unique<TCPConnection>();
}

std::atomic<uint64_t> TCPConnection::next_id_{0};

TCPConnection::TCPConnection()
    : id_(next_id_.fetch_add(1, std::memory_order_relaxed)) {}

TCPConnection::~TCPConnection() = default;

Status TCPConnection::Connect(const std::string& ip, uint32_t port,
                              uint32_t timeout_ms) {
  bthread::RWLockWrGuard guard(rwlock_);
  if (channel_ != nullptr) {
    return Status::OK();
  }

  butil::EndPoint ep;
  int rc = butil::str2endpoint(ip.c_str(), port, &ep);
  if (rc != 0) {
    LOG(ERROR) << "Fail to str2endpoint(" << ip << ":" << port << ")";
    return Status::Internal("str2endpoint failed");
  }

  brpc::ChannelOptions options;
  options.connect_timeout_ms = timeout_ms;
  options.connection_group = fmt::format("{}:{}:{}", ip, port, id_);
  auto channel = std::make_shared<brpc::Channel>();
  rc = channel->Init(ep, &options);
  if (rc != 0) {
    LOG(ERROR) << "Fail to init channel for address=" << ip << ":" << port;
    return Status::Internal("init channel failed");
  }

  channel_ = std::move(channel);
  LOG(INFO) << "Successfully init brpc channel for " << ip << ":" << port << ":"
            << id_;
  return Status::OK();
}

void TCPConnection::Close() {
  bthread::RWLockWrGuard guard(rwlock_);
  channel_.reset();
}

bool TCPConnection::IsConnected() {
  bthread::RWLockRdGuard guard(rwlock_);
  return channel_ != nullptr;
}

void TCPConnection::Send(const std::string& method,
                         const google::protobuf::Message& raw_request,
                         google::protobuf::Message* raw_response,
                         const IOBuffer* request_attachment,
                         IOBuffer* response_attachment, uint32_t timeout_ms,
                         Result* result) {
  std::shared_ptr<brpc::Channel> channel;
  {
    bthread::RWLockRdGuard guard(rwlock_);
    channel = channel_;
  }
  if (channel == nullptr) {
    result->SetFailed(EIO, "brpc channel is not connected", true);
    return;
  }

  const auto* descriptor =
      pb::cache::BlockCacheService::descriptor()->FindMethodByName(method);
  CHECK(descriptor != nullptr) << "Unknown rpc method=" << method;

  brpc::Controller cntl;
  cntl.set_connection_type(brpc::CONNECTION_TYPE_SINGLE);
  cntl.set_timeout_ms(timeout_ms);
  cntl.ignore_eovercrowded();
  if (request_attachment != nullptr) {
    cntl.request_attachment() =
        const_cast<IOBuffer*>(request_attachment)->IOBuf();
  }

  channel->CallMethod(descriptor, &cntl, &raw_request, raw_response, nullptr);
  if (cntl.Failed()) {
    result->SetFailed(cntl.ErrorCode(), cntl.ErrorText(), /*broken=*/true);
    return;
  }

  if (response_attachment != nullptr) {
    auto& attachment = cntl.response_attachment();
    if (attachment.length() == response_attachment->Size()) {
      attachment.copy_to(response_attachment->Fetch1(), attachment.length());
    } else if (attachment.length() > 0) {
      // A short or oversized body would silently corrupt the pre-allocated
      // read buffer; fail here so the tier cache falls back to storage. An
      // empty attachment is legal: error responses carry no body, the pb
      // status decides.
      LOG(ERROR) << "Response attachment size mismatch: method=" << method
                 << ", expected=" << response_attachment->Size()
                 << ", but got=" << attachment.length();
      result->SetFailed(EIO, "response attachment size mismatch");
    }
  }
}

RDMAConnection::~RDMAConnection() = default;

Status RDMAConnection::Connect(const std::string& ip, uint32_t port,
                               uint32_t /*timeout_ms*/) {
  std::lock_guard<bthread::Mutex> guard(mutex_);
  if (rdma_client_ != nullptr) {
    return Status::OK();
  }

  infiniband::EndPoint ep{FLAGS_cache_rdma_device,
                          static_cast<uint8_t>(FLAGS_cache_rdma_port_num)};
  auto client = infiniband::Client::Create(ep);
  if (client == nullptr) {
    return Status::Internal("create rdma client failed");
  }
  auto status = client->Connect(ip + ":" + std::to_string(port));
  if (!status.ok()) {
    return status;
  }
  rdma_client_ = std::move(client);
  return Status::OK();
}

void RDMAConnection::Close() {
  std::lock_guard<bthread::Mutex> guard(mutex_);
  rdma_client_.reset();
}

bool RDMAConnection::IsConnected() {
  std::lock_guard<bthread::Mutex> guard(mutex_);
  return rdma_client_ != nullptr;
}

void RDMAConnection::Send(const std::string& method,
                          const google::protobuf::Message& raw_request,
                          google::protobuf::Message* raw_response,
                          const IOBuffer* request_attachment,
                          IOBuffer* response_attachment, uint32_t timeout_ms,
                          Result* result) {
  std::shared_ptr<infiniband::Client> client;
  {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    client = rdma_client_;
  }
  if (client == nullptr) {
    result->SetFailed(pb::infiniband::ErrorCode::InternalError,
                      "rdma client is not connected", /*broken=*/true);
    return;
  }

  infiniband::Controller cntl;
  cntl.set_timeout_ms(timeout_ms);
  if (method == "Range") {
    if (response_attachment != nullptr && response_attachment->Size() > 0) {
      auto& region = cntl.write_region();
      region.set_addr(
          reinterpret_cast<uint64_t>(response_attachment->Fetch1()));
      region.set_length(static_cast<uint32_t>(response_attachment->Size()));
      region.set_rkey(
          static_cast<uint32_t>(response_attachment->GetFirstDataMeta()));
    }
  } else if (request_attachment != nullptr && request_attachment->Size() > 0) {
    cntl.request_attachment() = *request_attachment;
    for (const auto& iov : request_attachment->Fetch()) {
      auto* region = cntl.read_regions().Add();
      region->set_addr(reinterpret_cast<uint64_t>(iov.iov_base));
      region->set_length(static_cast<uint32_t>(iov.iov_len));
      region->set_rkey(static_cast<uint32_t>(infiniband::GetRkey(
          FLAGS_cache_rdma_device, iov.iov_base, iov.iov_len)));
    }
  }

  client->Call(&cntl, kServiceName, method, raw_request, raw_response);
  if (cntl.Failed()) {
    result->SetFailed(cntl.ErrorCode(), cntl.ErrorText(),
                      IsConnBroken(cntl.ErrorCode()));
    return;
  }
}

bool RDMAConnection::IsConnBroken(int error_code) {
  using EC = pb::infiniband::ErrorCode;
  // Timeout is a broken connection: the QP has been fenced to error (see
  // ClientSession::MarkBroken), so it must be closed and rebuilt before retry.
  return error_code == EC::QueuePairError || error_code == EC::InternalError ||
         error_code == EC::Timeout;
}

}  // namespace cache
}  // namespace dingofs
