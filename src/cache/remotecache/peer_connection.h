
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
#include <bthread/mutex.h>
#include <bthread/rwlock.h>
#include <google/protobuf/message.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "cache/infiniband/client.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class PeerConnection;
using PeerConnectionUPtr = std::unique_ptr<PeerConnection>;

class PeerConnection {
 public:
  struct Result {
    bool failed{false};
    int error_code{0};
    std::string error_text;
    bool conn_broken{false};

    void SetFailed(int code, const std::string& text, bool broken = false) {
      failed = true;
      error_code = code;
      error_text = text;
      conn_broken = broken;
    }
  };

  static PeerConnectionUPtr New();
  virtual ~PeerConnection() = default;

  virtual Status Connect(const std::string& ip, uint32_t port,
                         uint32_t timeout_ms) = 0;
  virtual void Close() = 0;
  virtual bool IsConnected() = 0;

  virtual void Send(const std::string& method,
                    const google::protobuf::Message& raw_request,
                    google::protobuf::Message* raw_response,
                    const IOBuffer* request_attachment,
                    IOBuffer* response_attachment, uint32_t timeout_ms,
                    Result* result) = 0;
};

class TCPConnection : public PeerConnection {
 public:
  TCPConnection();
  ~TCPConnection() override;

  Status Connect(const std::string& ip, uint32_t port,
                 uint32_t timeout_ms) override;
  void Close() override;
  bool IsConnected() override;
  void Send(const std::string& method,
            const google::protobuf::Message& raw_request,
            google::protobuf::Message* raw_response,
            const IOBuffer* request_attachment, IOBuffer* response_attachment,
            uint32_t timeout_ms, Result* result) override;

 private:
  static std::atomic<uint64_t> next_id_;
  const uint64_t id_;
  bthread::RWLock rwlock_;
  std::shared_ptr<brpc::Channel> channel_;
};

class RDMAConnection : public PeerConnection {
 public:
  ~RDMAConnection() override;

  Status Connect(const std::string& ip, uint32_t port,
                 uint32_t timeout_ms) override;
  void Close() override;
  bool IsConnected() override;
  void Send(const std::string& method,
            const google::protobuf::Message& raw_request,
            google::protobuf::Message* raw_response,
            const IOBuffer* request_attachment, IOBuffer* response_attachment,
            uint32_t timeout_ms, Result* result) override;

 private:
  static constexpr const char* kServiceName =
      "dingofs.pb.cache.BlockCacheService";

  bool IsConnBroken(int error_code);

  bthread::Mutex mutex_;
  std::shared_ptr<infiniband::Client> rdma_client_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_PEER_CONNECTION_H_
