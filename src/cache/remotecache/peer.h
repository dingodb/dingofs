
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
 * Created Date: 2026-01-12
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_PEER_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_PEER_H_

#include <brpc/channel.h>
#include <bthread/mutex.h>
#include <bthread/rwlock.h>
#include <butil/iobuf.h>
#include <butil/memory/scope_guard.h>
#include <json/value.h>

#include <atomic>
#include <memory>
#include <ostream>
#include <string>

#include "cache/common/error.h"
#include "cache/remotecache/peer_connection.h"
#include "cache/remotecache/peer_health_checker.h"
#include "cache/remotecache/request.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {

class Peer {
 public:
  Peer(const std::string& id, const std::string& ip, uint32_t port,
       uint32_t weight);
  Status Start();
  void Shutdown();

  template <typename T, typename U>
  Response<U> SendRequest(const Request<T>& request);

  std::string Id() const { return id_; }
  std::string IP() const { return ip_; }
  uint32_t Port() const { return port_; }
  uint32_t Weight() const { return weight_; }
  bool IsHealthy() { return health_checker_->IsHealthy(); }
  bool Dump(Json::Value& value) const;

 private:
  std::string EndPoint() { return ip_ + ":" + std::to_string(port_); }

  PeerConnection* GetConnection() {
    return connections_[next_conn_index_.fetch_add(1) % FLAGS_connections]
        .get();
  }

  uint32_t NextTimeoutMs(const std::string& method, int retry_count) const;
  bool ShouldRetry(const std::string& method, int retcode) const;
  void DoConnect(PeerConnection* conn) const;

  std::atomic<bool> running_;
  std::string id_;
  std::string ip_;
  uint32_t port_;
  uint32_t weight_;
  std::atomic<int> next_conn_index_{0};
  std::vector<PeerConnectionUPtr> connections_;
  PeerHealthCheckerUPtr health_checker_;
};

using PeerSPtr = std::shared_ptr<Peer>;

std::ostream& operator<<(std::ostream& os, const Peer& peer);

template <typename T, typename U>
Response<U> Peer::SendRequest(const Request<T>& request) {
  const auto* method =
      pb::cache::BlockCacheService::descriptor()->FindMethodByName(
          request.method);
  if (method == nullptr) {
    LOG(FATAL) << "Unknown rpc method=" << request.method;
  }

  Response<U> response;
  BRPC_SCOPE_EXIT {
    auto status = response.status;
    if (status.ok()) {
      health_checker_->IOSuccess();
    } else if (!status.IsNotFound()) {
      health_checker_->IOError();
    }
  };

  butil::Timer timer;
  timer.start();

  for (int retry_count = 0; retry_count < FLAGS_cache_rpc_max_retry_times;
       ++retry_count) {
    auto* conn = GetConnection();
    CHECK_NOTNULL(conn);
    auto* channel = conn->GetChannel();
    if (nullptr == channel) {
      LOG(ERROR) << "PeerConnection is not connected, reconnect " << IP() << ":"
                 << Port();
      DoConnect(conn);
      continue;  // retry anthor one
    }

    brpc::Controller cntl;
    cntl.set_connection_type(brpc::CONNECTION_TYPE_SINGLE);
    cntl.set_timeout_ms(NextTimeoutMs(request.method, retry_count));
    cntl.ignore_eovercrowded();
    if (request.body != nullptr) {
      cntl.request_attachment() = const_cast<IOBuffer*>(request.body)->IOBuf();
    }
    // cntl.set_request_id(ctx->TraceId());

    channel->CallMethod(method, &cntl, &request.raw, &response.raw, nullptr);

    // network error
    if (cntl.Failed()) {
      LOG(ERROR) << "Fail to send " << request << " to " << EndPoint()
                 << ", because network is error: " << cntl.ErrorText()
                 << ", tooks " << std::setprecision(6)
                 << cntl.latency_us() / 1e6 << " seconds";

      if (!ShouldRetry(request.method, cntl.ErrorCode())) {
        response.status = Status::NetError(cntl.ErrorCode(), cntl.ErrorText());
        return response;
      }

      // FIXME: don't reconnect if raise net error?
      conn->Close();
      DoConnect(conn);
      continue;
    }

    // response status is ok
    response.status = ToStatus(response.raw.status());
    if (response.status.ok()) {
      response.body = IOBuffer(cntl.response_attachment().movable());
      return response;
    } else {
      LOG(ERROR) << "Fail to send " << request << " to " << EndPoint()
                 << ", because receive " << response << ", tooks "
                 << std::setprecision(6) << cntl.latency_us() / 1e6
                 << " seconds";
      return response;
    }
  }

  timer.stop();

  LOG(ERROR) << "Fail to send rpc " << request << " to " << EndPoint()
             << ", because exceeded max retry times="
             << FLAGS_cache_rpc_max_retry_times << ", tooks "
             << std::setprecision(6) << timer.u_elapsed(0) / 1e6 << " seconds";

  response.status = Status::Internal("rpc exceed max retry times");
  return response;
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_PEER_H_