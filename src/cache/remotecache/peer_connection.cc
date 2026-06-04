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

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/mutex.h>
#include <bthread/rwlock.h>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <cerrno>
#include <memory>
#include <string>

#include "cache/infiniband/client.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/controller.h"
#include "cache/remotecache/rdma_region_registry.h"
#include "common/options/cache.h"
#include "dingofs/blockcache.pb.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {

namespace {

constexpr const char* kServiceName = "dingofs.pb.cache.BlockCacheService";

// Distinct connection_group per brpc connection so brpc keeps separate physical
// links (matches the previous one-channel-per-PeerConnection behavior).
std::atomic<uint64_t> g_connection_id{0};

void SetFailed(TransportResult* out, int error_code,
               const std::string& error_text, bool conn_broken = false) {
  out->failed = true;
  out->error_code = error_code;
  out->error_text = error_text;
  out->conn_broken = conn_broken;
}

// Whether an RDMA transport error means the connection/QP is actually broken
// (so the caller must rebuild it) versus a transient failure that must NOT
// trigger a reconnect. Reconnecting tears down + rebuilds the QP, huge-page
// buffer pools and MR registrations; doing that on every RPC timeout turns a
// timeout burst into a reconnect storm that exhausts huge pages.
bool RdmaErrorIsConnBroken(int error_code) {
  using EC = pb::infiniband::ErrorCode;
  // QueuePairError: QP went to error state. InternalError: "rdma client is not
  // connected" / wait failed -- treat as broken. Timeout / NoMem (transient
  // server-busy or buffer shortage) and ProtocolError are NOT connection death.
  return error_code == EC::QueuePairError || error_code == EC::InternalError;
}

}  // namespace

// Transport over brpc (TCP / IPoIB). Block payloads ride as brpc attachments.
class TcpTransport : public Transport {
 public:
  TcpTransport()
      : id_(g_connection_id.fetch_add(1, std::memory_order_relaxed)) {}

  Status Connect(const std::string& ip, uint32_t port,
                 uint32_t timeout_ms) override {
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
    LOG(INFO) << "Successfully init brpc channel for " << ip << ":" << port
              << ":" << id_;
    return Status::OK();
  }

  void Close() override {
    bthread::RWLockWrGuard guard(rwlock_);
    channel_.reset();
  }

  bool Connected() override {
    bthread::RWLockRdGuard guard(rwlock_);
    return channel_ != nullptr;
  }

  void Execute(const std::string& method,
               const google::protobuf::Message& raw_request,
               google::protobuf::Message* raw_response,
               const IOBuffer* req_body, IOBuffer* resp_body,
               uint32_t timeout_ms, TransportResult* out) override {
    std::shared_ptr<brpc::Channel> channel;
    {
      bthread::RWLockRdGuard guard(rwlock_);
      channel = channel_;
    }
    if (channel == nullptr) {
      SetFailed(out, EIO, "brpc channel is not connected",
                /*conn_broken=*/true);
      return;
    }

    const auto* descriptor =
        pb::cache::BlockCacheService::descriptor()->FindMethodByName(method);
    CHECK(descriptor != nullptr) << "Unknown rpc method=" << method;

    brpc::Controller cntl;
    cntl.set_connection_type(brpc::CONNECTION_TYPE_SINGLE);
    cntl.set_timeout_ms(timeout_ms);
    cntl.ignore_eovercrowded();
    if (req_body != nullptr) {
      cntl.request_attachment() = const_cast<IOBuffer*>(req_body)->IOBuf();
    }

    channel->CallMethod(descriptor, &cntl, &raw_request, raw_response, nullptr);
    if (cntl.Failed()) {
      // brpc manages its own physical links, but preserve the prior behavior of
      // re-initing our channel wrapper on a brpc failure.
      SetFailed(out, cntl.ErrorCode(), cntl.ErrorText(), /*conn_broken=*/true);
      return;
    }

    if (resp_body != nullptr) {
      *resp_body = IOBuffer(cntl.response_attachment().movable());
    }
    out->failed = false;
  }

 private:
  uint64_t id_;
  bthread::RWLock rwlock_;
  std::shared_ptr<brpc::Channel> channel_;
};

// Transport over Infiniband/RDMA. Pure mechanics: the cache layer has already
// turned the payloads into registered IOBuffers (meta=rkey), so Execute only
// reads addr/len/rkey and advertises them — a one-sided RDMA read for the
// Put/Cache source, or a write into the pre-registered Range destination.
class RdmaTransport : public Transport {
 public:
  Status Connect(const std::string& ip, uint32_t port,
                 uint32_t /*timeout_ms*/) override {
    // Fast path: already connected (a concurrent reconnect won the race).
    {
      bthread::RWLockRdGuard guard(rwlock_);
      if (client_ != nullptr) {
        return Status::OK();
      }
    }

    // Serialize reconnects. A transport failure makes every concurrent request
    // on this connection see !Connected() and call Connect() -- a thundering
    // herd. Building a client allocates a QP + huge-page buffer pools + MR, so
    // a herd of them exhausts huge pages (SIGBUS). Let exactly one thread
    // rebuild; the rest observe the fresh client under the double-check and
    // return.
    std::lock_guard<bthread::Mutex> connect_guard(connect_mutex_);
    {
      bthread::RWLockRdGuard guard(rwlock_);
      if (client_ != nullptr) {
        return Status::OK();
      }
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
    bthread::RWLockWrGuard guard(rwlock_);
    client_ = std::move(client);
    return Status::OK();
  }

  void Close() override {
    bthread::RWLockWrGuard guard(rwlock_);
    client_.reset();
  }

  bool Connected() override {
    bthread::RWLockRdGuard guard(rwlock_);
    return client_ != nullptr;
  }

  void Execute(const std::string& method,
               const google::protobuf::Message& raw_request,
               google::protobuf::Message* raw_response,
               const IOBuffer* req_body, IOBuffer* resp_body,
               uint32_t /*timeout_ms*/, TransportResult* out) override {
    std::shared_ptr<infiniband::Client> client;
    {
      bthread::RWLockRdGuard guard(rwlock_);
      client = client_;
    }
    if (client == nullptr) {
      SetFailed(out, pb::infiniband::ErrorCode::InternalError,
                "rdma client is not connected", /*conn_broken=*/true);
      return;
    }

    infiniband::Controller cntl;
    if (method == "Range") {
      // resp_body is the pre-registered destination prepared by the cache
      // layer; the server RDMA-writes the block straight into it.
      if (resp_body != nullptr && resp_body->Size() > 0) {
        cntl.SetRequestRdmaRegion(
            resp_body->Fetch1(), static_cast<uint32_t>(resp_body->Size()),
            static_cast<uint32_t>(resp_body->GetFirstDataMeta()));
      }
    } else if (method == "Put" || method == "Cache") {
      // req_body is already a registered source; advertise it for the server's
      // one-sided RDMA read.
      if (req_body != nullptr && req_body->Size() > 0) {
        auto* body = const_cast<IOBuffer*>(req_body);
        if (body->ConstIOBuf().backing_block_num() == 1) {
          // Single registered block (lmcache arena / cache pool): one region,
          // rkey from the block's meta.
          cntl.SetRequestAttachmentRegion(
              body->Fetch1(), static_cast<uint32_t>(body->Size()),
              static_cast<uint32_t>(body->GetFirstDataMeta()));
        } else {
          // Multi-segment registered source (e.g. a FUSE write-mempool block
          // spanning pages): advertise each segment for a scatter RDMA read.
          // RemoteBlockCache only takes this path when every segment resolves a
          // non-zero rkey from the region registry.
          auto& registry = RdmaRegionRegistry::GetInstance();
          for (const auto& iov : body->Fetch()) {
            cntl.AddRequestAttachmentRegion(
                iov.iov_base, static_cast<uint32_t>(iov.iov_len),
                registry.RkeyFor(iov.iov_base, iov.iov_len));
          }
        }
      }
    }
    // Prefetch / Ping carry no attachment.

    client->Call(&cntl, kServiceName, method, raw_request, raw_response);
    if (cntl.Failed()) {
      SetFailed(out, cntl.ErrorCode(), cntl.ErrorText(),
                RdmaErrorIsConnBroken(cntl.ErrorCode()));
      return;
    }
    // For Range the block was RDMA-written into resp_body in place; nothing
    // else to do here.
    out->failed = false;
  }

 private:
  bthread::RWLock rwlock_;        // guards client_
  bthread::Mutex connect_mutex_;  // serializes reconnects (dedup the herd)
  std::shared_ptr<infiniband::Client> client_;
};

PeerConnection::PeerConnection()
    : transport_(FLAGS_use_rdma
                     ? TransportUPtr(std::make_unique<RdmaTransport>())
                     : TransportUPtr(std::make_unique<TcpTransport>())) {}

}  // namespace cache
}  // namespace dingofs
