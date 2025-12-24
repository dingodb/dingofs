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

#include "cache/remotecache/peer_connection.h"

#include <fmt/format.h>

namespace dingofs {
namespace cache {

Status PeerConnection::Connect(const std::string& ip, uint32_t port,
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
  channel_ = std::make_unique<brpc::Channel>();
  rc = channel_->Init(ep, &options);
  if (rc != 0) {
    LOG(ERROR) << "Fail to init channel for address=" << ip << ":" << port;
    channel_.reset(nullptr);
    return Status::Internal("init channel failed");
  }

  LOG(INFO) << "Successfully init channel for PeerConnection=" << ip << ":"
            << port << ":" << id_;
  return Status::OK();
}

void PeerConnection::Close() {
  bthread::RWLockWrGuard guard(rwlock_);
  channel_.reset(nullptr);
}

brpc::Channel* PeerConnection::GetChannel() {
  bthread::RWLockRdGuard guard(rwlock_);
  if (channel_ == nullptr) {
    return nullptr;
  }
  return channel_.get();
}

}  // namespace cache
}  // namespace dingofs
