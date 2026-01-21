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
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/peer.h"

#include <brpc/channel.h>
#include <bthread/rwlock.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <memory>

#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_int32(connections, 16, "keepalive connection number per peer");

DEFINE_uint32(cache_rpc_connect_timeout_ms, 1000,
              "timeout for rpc channel connect in milliseconds");
DEFINE_validator(cache_rpc_connect_timeout_ms, brpc::PassValidate);

DEFINE_uint32(cache_put_rpc_timeout_ms, 30000,
              "timeout for put rpc request in milliseconds");
DEFINE_validator(cache_put_rpc_timeout_ms, brpc::PassValidate);

DEFINE_uint32(cache_range_rpc_timeout_ms, 30000,
              "timeout for range rpc request in milliseconds");
DEFINE_validator(cache_range_rpc_timeout_ms, brpc::PassValidate);

DEFINE_uint32(cache_rpc_timeout_ms, 30000,
              "timeout for cache rpc request in milliseconds");
DEFINE_validator(cache_rpc_timeout_ms, brpc::PassValidate);

DEFINE_uint32(cache_prefetch_rpc_timeout_ms, 3000,
              "timeout for prefetch rpc request in milliseconds");
DEFINE_validator(cache_prefetch_rpc_timeout_ms, brpc::PassValidate);

DEFINE_uint32(cache_rpc_max_retry_times, 3,
              "maximum retry times for rpc request");
DEFINE_validator(cache_rpc_max_retry_times, brpc::PassValidate);

DEFINE_uint32(cache_rpc_max_timeout_ms, 60000,
              "maximum timeout for rpc request in milliseconds");
DEFINE_validator(cache_rpc_max_timeout_ms, brpc::PassValidate);

Peer::Peer(const std::string& id, const std::string& ip, uint32_t port,
           uint32_t weight)
    : running_(false),
      id_(id),
      ip_(ip),
      port_(port),
      weight_(weight),
      health_checker_(std::make_unique<PeerHealthChecker>(ip, port)) {
  for (int i = 0; i < FLAGS_connections; ++i) {
    connections_.emplace_back(PeerConnection::New());
  }
}

Status Peer::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Peer already started";
    return Status::OK();
  }

  bool has_error = false;
  for (int i = 0; i < FLAGS_connections; ++i) {
    auto status = connections_[i]->Connect(ip_, port_,
                                           FLAGS_cache_rpc_connect_timeout_ms);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to connect to " << *this;
      has_error = true;
    }
  }

  if (has_error) {
    LOG(ERROR) << "Fail to start peer";
    return Status::Internal("start peer failed");
  }

  health_checker_->Start();

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "Peer started";
  return Status::OK();
}

void Peer::Shutdown() {
  for (int i = 0; i < FLAGS_connections; ++i) {
    connections_[i]->Close();
  }
  health_checker_->Shutdown();
}

uint32_t Peer::NextTimeoutMs(const std::string& method, int retry_count) const {
  uint32_t timeout_ms;
  if (method == "Put") {
    timeout_ms = FLAGS_cache_put_rpc_timeout_ms;
  } else if (method == "Range") {
    timeout_ms = FLAGS_cache_range_rpc_timeout_ms;
  } else if (method == "Cache") {
    timeout_ms = FLAGS_cache_rpc_timeout_ms;
  } else if (method == "Prefetch") {
    timeout_ms = FLAGS_cache_prefetch_rpc_timeout_ms;
  } else {
    CHECK(false) << "Unknown rpc method=" << method;
  }

  timeout_ms = timeout_ms * std::pow(2, retry_count);
  return std::min(timeout_ms, FLAGS_cache_rpc_max_timeout_ms);
}

bool Peer::ShouldRetry(const std::string& method, int /*retcode*/) const {
  return method == "Range";
}

void Peer::DoConnect(PeerConnection* conn) const {
  conn->Connect(IP(), Port(), FLAGS_cache_rpc_connect_timeout_ms);
}

bool Peer::Dump(Json::Value& value) const {
  value["id"] = Id();
  value["endpoint"] = fmt::format("{}:{}", IP(), Port());
  value["weight"] = Weight();
  value["connections"] = static_cast<int>(FLAGS_connections);
  value["healthy"] = health_checker_->IsHealthy();
  return true;
}

std::ostream& operator<<(std::ostream& os, const Peer& peer) {
  os << "Peer{id=" << peer.Id() << " ip=" << peer.IP()
     << " port=" << peer.Port() << " conns=" << FLAGS_connections << "}";
  return os;
}

}  // namespace cache
}  // namespace dingofs
