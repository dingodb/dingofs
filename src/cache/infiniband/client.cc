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

/*
 * Project: DingoFS
 * Created Date: 2026-04-22
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/client.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/endpoint.h>
#include <glog/logging.h>

#include <memory>
#include <utility>

#include "cache/infiniband/connection.h"
#include "cache/infiniband/event.h"
#include "cache/infiniband/infiniband.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

Dialer::Dialer(Device* device, Port* port, ProtectDomain* protect_domain)
    : device_(device), port_(port), protect_domain_(protect_domain) {}

DialerUPtr Dialer::Create(const EndPoint& ep) {
  Infiniband::Context context;
  auto status = Infiniband::Init(ep.device_name, ep.port_num, &context);
  if (!status.ok()) {
    return nullptr;
  }

  return std::make_unique<Dialer>(context.device, context.port,
                                  context.protect_domain);
}

ConnectionUPtr Dialer::Dial(const std::string& address) {
  auto completion_queue = CompletionQueue::Create(device_);
  if (nullptr == completion_queue) {
    LOG(ERROR) << "Fail to create completion queue";
    return nullptr;
  }

  auto queue_pair = QueuePair::Create(device_, port_, protect_domain_,
                                      completion_queue.get());
  if (nullptr == queue_pair) {
    LOG(ERROR) << "Fail to create queue pair";
    return nullptr;
  }

  ConnManagementMeta local_cm_meta = queue_pair->GetConnManagementMeta();
  ConnManagementMeta remote_cm_meta;
  auto status = SyncConnManagementMeta(address, local_cm_meta, &remote_cm_meta);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to sync connection management meta: "
               << status.ToString();
    return nullptr;
  }

  status = queue_pair->ModifyQpToInit();
  if (status.ok()) {
    status = queue_pair->ModifyQpToRtr(remote_cm_meta);
    if (status.ok()) {
      status = queue_pair->ModifyQpToRts();
    }
  }

  if (!status.ok()) {
    return nullptr;
  }

  return std::make_unique<Connection>(std::move(queue_pair),
                                      std::move(completion_queue));
}

Status Dialer::SyncConnManagementMeta(const std::string& address,
                                     const ConnManagementMeta& local_cm_meta,
                                     ConnManagementMeta* remote_cm_meta) {
  butil::EndPoint ep;
  if (butil::str2endpoint(address.c_str(), &ep) != 0) {
    LOG(ERROR) << "Fail to parse address=" << address;
    return Status::Internal("str2endpoint failed");
  }

  brpc::Channel channel;
  brpc::ChannelOptions options;
  options.connect_timeout_ms = 1000;
  options.timeout_ms = 3000;
  if (channel.Init(ep, &options) != 0) {
    LOG(ERROR) << "Fail to init channel for address=" << address;
    return Status::Internal("init channel failed");
  }

  pb::infiniband::SyncRequest request;
  SerializeToPb(local_cm_meta, request.mutable_cm_meta());

  brpc::Controller cntl;
  pb::infiniband::SyncResponse response;
  pb::infiniband::InfinibandService_Stub stub(&channel);
  stub.Sync(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    return Status::NetError(cntl.ErrorCode(), cntl.ErrorText());
  }

  return ParseFromPb(response.cm_meta(), remote_cm_meta);
}

Client::Client(DialerUPtr dialer)
    : dialer_(std::move(dialer)), session_(nullptr) {}

Client::~Client() {
  if (session_ != nullptr) {
    session_->Shutdown();
  }
}

ClientUPtr Client::Create(const EndPoint& ep) {
  auto dialer = Dialer::Create(ep);
  if (nullptr == dialer) {
    return nullptr;
  }
  return std::make_unique<Client>(std::move(dialer));
}

Status Client::Connect(const std::string& address) {
  auto conn = dialer_->Dial(address);
  if (nullptr == conn) {
    LOG(ERROR) << "Fail to connect peer=" << address;
    return Status::Internal("dial peer failed");
  }

  int fd = conn->GetFd();
  session_ = std::make_shared<ClientSession>(std::move(conn));
  auto status = session_->Start();
  if (!status.ok()) {
    return status;
  }

  status = GetGlobalEventDispatcher(fd).AddEvent(fd, EventType::kReadEvent,
                                                 session_);
  if (!status.ok()) {
    session_->Shutdown();
    LOG(ERROR) << "Fail to add event to dispatcher";
    return status;
  }
  return Status::OK();
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
