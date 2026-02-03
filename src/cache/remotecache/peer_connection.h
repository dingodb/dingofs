
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
 * Created Date: 2026-01-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_PEER_CONNECTION_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_PEER_CONNECTION_H_

#include <brpc/channel.h>
#include <bthread/rwlock.h>

#include <atomic>
#include <memory>
#include <string>

#include "common/status.h"

namespace dingofs {
namespace cache {

static std::atomic<uint64_t> kConnectionId{0};

class PeerConnection;
using PeerConnectionUPtr = std::unique_ptr<PeerConnection>;

class PeerConnection {
 public:
  PeerConnection()
      : id_(kConnectionId.fetch_add(1, std::memory_order_relaxed)) {}

  static PeerConnectionUPtr New() { return std::make_unique<PeerConnection>(); }

  Status Connect(const std::string& ip, uint32_t port, uint32_t timeout_ms);
  void Close();
  std::shared_ptr<brpc::Channel> GetChannel();

 private:
  uint64_t id_;
  bthread::RWLock rwlock_;
  std::shared_ptr<brpc::Channel> channel_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_PEER_CONNECTION_H_
