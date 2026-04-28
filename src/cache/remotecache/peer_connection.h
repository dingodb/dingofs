
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

#include <google/protobuf/message.h>

#include <memory>
#include <string>

#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

// Result of a single transport-level RPC, decoupled from the concrete
// transport (brpc / RDMA). `failed` means a transport/network error (the app
// status, if any, lives in the response message).
struct TransportResult {
  bool failed{false};
  int error_code{0};
  std::string error_text;
};

// One physical connection to a cache peer. Hides whether the wire is brpc or
// RDMA so that Peer::SendRequest stays transport-agnostic.
class Transport {
 public:
  virtual ~Transport() = default;

  virtual Status Connect(const std::string& ip, uint32_t port,
                         uint32_t timeout_ms) = 0;
  virtual void Close() = 0;
  virtual bool Connected() = 0;

  // Sends one BlockCacheService RPC. For Put/Cache, `req_body` is the block to
  // upload (may be null); for Range, `resp_body` receives the fetched block
  // (may be null). Transport-specific details (brpc attachment vs one-sided
  // RDMA) are hidden here.
  virtual void Execute(const std::string& method,
                       const google::protobuf::Message& raw_request,
                       google::protobuf::Message* raw_response,
                       const IOBuffer* req_body, IOBuffer* resp_body,
                       uint32_t timeout_ms, TransportResult* out) = 0;
};

using TransportUPtr = std::unique_ptr<Transport>;

class PeerConnection;
using PeerConnectionUPtr = std::unique_ptr<PeerConnection>;

class PeerConnection {
 public:
  PeerConnection();

  static PeerConnectionUPtr New() { return std::make_unique<PeerConnection>(); }

  Status Connect(const std::string& ip, uint32_t port, uint32_t timeout_ms) {
    return transport_->Connect(ip, port, timeout_ms);
  }
  void Close() { transport_->Close(); }
  bool Connected() { return transport_->Connected(); }

  void Execute(const std::string& method,
               const google::protobuf::Message& raw_request,
               google::protobuf::Message* raw_response,
               const IOBuffer* req_body, IOBuffer* resp_body,
               uint32_t timeout_ms, TransportResult* out) {
    transport_->Execute(method, raw_request, raw_response, req_body, resp_body,
                        timeout_ms, out);
  }

 private:
  TransportUPtr transport_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_PEER_CONNECTION_H_
