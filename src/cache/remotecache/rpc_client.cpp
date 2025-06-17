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
 * Created Date: 2025-06-15
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/rpc_client.h"

#include <absl/strings/str_format.h>

#include "cache/common/type.h"
#include "cache/config/tiercache.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(put_rpc_timeout_ms, 3000,
              "RPC timeout for remote put operation in milliseconds");
DEFINE_uint32(range_rpc_timeout_ms, 3000,
              "RPC timeout for remote range operation in milliseconds");
DEFINE_uint32(cache_rpc_timeout_ms, 3000,
              "RPC timeout for remote cache operation in milliseconds");
DEFINE_uint32(prefetch_rpc_timeout_ms, 3000,
              "RPC timeout for remote prefetch operation in milliseconds");
DEFINE_uint32(cache_rpc_max_retry_times, 3,
              "Maximum number of retry times for remote cache rpc operations");

static const std::string kApiPut = "Put";
static const std::string kApiRange = "Range";
static const std::string kApiCache = "Cache";
static const std::string kApiPrefetch = "Prefetch";

using pb::cache::blockcache::BlockCacheErrCode_Name;

RPCClient::RPCClient(const std::string& server_ip, uint32_t server_port)
    : inited_(false),
      server_ip_(server_ip),
      server_port_(server_port),
      channel_(std::make_unique<brpc::Channel>()) {}

Status RPCClient::Init() { return InitChannel(server_ip_, server_port_); }

Status RPCClient::Put(ContextSPtr ctx, const BlockKey& key,
                      const Block& block) {
  brpc::Controller cntl;
  PBPutRequest request;
  PBPutResponse response;

  IOBuffer buffer = block.buffer;
  *request.mutable_block_key() = key.ToPB();
  request.set_block_size(buffer.Size());
  cntl.request_attachment().append(buffer.IOBuf());

  return SendRequest(ctx, kApiPut, cntl, request, response);
}

Status RPCClient::Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                        size_t length, IOBuffer* buffer, size_t block_size) {
  brpc::Controller cntl;
  PBRangeRequest request;
  PBRangeResponse response;

  *request.mutable_block_key() = key.ToPB();
  request.set_offset(offset);
  request.set_length(length);
  request.set_block_size(block_size);

  auto status = SendRequest(ctx, kApiRange, cntl, request, response);
  if (status.ok()) {
    *buffer = IOBuffer(cntl.response_attachment());
  }
  return status;
}

Status RPCClient::Cache(ContextSPtr ctx, const BlockKey& key,
                        const Block& block) {
  brpc::Controller cntl;
  PBCacheRequest request;
  PBCacheResponse response;

  auto buffer = block.buffer;
  *request.mutable_block_key() = key.ToPB();
  request.set_block_size(buffer.Size());
  cntl.request_attachment().append(buffer.IOBuf());

  return SendRequest(ctx, kApiCache, cntl, request, response);
}

Status RPCClient::Prefetch(ContextSPtr ctx, const BlockKey& key,
                           size_t length) {
  brpc::Controller cntl;
  PBPrefetchRequest request;
  PBPrefetchResponse response;

  *request.mutable_block_key() = key.ToPB();
  request.set_block_size(length);

  return SendRequest(ctx, kApiPrefetch, cntl, request, response);
}

Status RPCClient::InitChannel(const std::string& server_ip,
                              uint32_t server_port) {
  butil::EndPoint ep;
  int rc = butil::str2endpoint(server_ip.c_str(), server_port, &ep);
  if (rc != 0) {
    LOG(ERROR) << "str2endpoint(" << server_ip << "," << server_port
               << ") failed: rc = " << rc;
    return Status::Internal("str2endpoint() failed");
  }

  rc = channel_->Init(ep, nullptr);
  if (rc != 0) {
    LOG(INFO) << "Init channel for " << server_ip << ":" << server_port
              << " failed: rc = " << rc;
    return Status::Internal("Init channel failed");
  }

  LOG(INFO) << "Create channel for address (" << server_ip << ":" << server_port
            << ") success.";

  inited_ = true;
  return Status::OK();
}

brpc::Channel* RPCClient::GetChannel() {
  ReadLockGuard lock(rwlock_);
  return inited_ ? channel_.get() : nullptr;
}

Status RPCClient::ResetChannel() {
  WriteLockGuard lock(rwlock_);
  return InitChannel(server_ip_, server_port_);
}

// Only retry for timeout
bool RPCClient::ShouldRetry(int retcode) {
  return retcode == -brpc::ERPCTIMEDOUT || retcode == -ETIMEDOUT;
}

bool RPCClient::ShouldReset(int retcode) {
  return retcode != -brpc::ERPCTIMEDOUT && retcode != -ETIMEDOUT;
}

// TODO: consider retry_count
uint32_t RPCClient::NextTimeoutMs(const std::string& api_name,
                                  int /*retry_count*/) const {
  if (api_name == kApiPut) {
    return FLAGS_put_rpc_timeout_ms;
  } else if (api_name == kApiRange) {
    return FLAGS_range_rpc_timeout_ms;
  } else if (api_name == kApiCache) {
    return FLAGS_cache_rpc_timeout_ms;
  } else if (api_name == kApiPrefetch) {
    return FLAGS_prefetch_rpc_timeout_ms;
  }

  CHECK(false) << "Unknown API name: " << api_name;
  return 0;
}

template <typename Request, typename Response>
Status RPCClient::SendRequest(ContextSPtr ctx, const std::string& api_name,
                              brpc::Controller& cntl, const Request& request,
                              Response& response) {
  const auto* method =
      PBBlockCacheService::descriptor()->FindMethodByName(api_name);

  if (method == nullptr) {
    LOG(FATAL) << "Unknown api name: " << api_name;
  }

  for (int retry_count = 0; retry_count < FLAGS_cache_rpc_max_retry_times;
       ++retry_count) {
    cntl.set_timeout_ms(NextTimeoutMs(api_name, retry_count));
    cntl.set_request_id(ctx->TraceId());

    auto* channel = GetChannel();
    if (channel == nullptr) {
      LOG(ERROR) << absl::StrFormat(
          "[rpc][%s:%d][%s] channel is not inited, retrying...", server_ip_,
          server_port_, api_name);
      ResetChannel();
      continue;
    }

    channel->CallMethod(method, &cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
      LOG(ERROR) << absl::StrFormat(
          "[rpc][%s:%d][%s][%.6lfs] failed: request(%s) cntl_code(%d) "
          "cntl_error(%s)",
          server_ip_, server_port_, api_name, cntl.latency_us() / 1e6,
          request.ShortDebugString(), cntl.ErrorCode(), cntl.ErrorText());

      if (!ShouldRetry(cntl.ErrorCode())) {
        return Status::NetError(cntl.ErrorCode(), cntl.ErrorText());
      } else if (ShouldReset(cntl.ErrorCode())) {
        ResetChannel();
      }
      continue;
    }

    if (response.status() != PBBlockCacheErrCode::BlockCacheOk) {
      LOG(ERROR) << absl::StrFormat(
          "[rpc][%s:%d][%s][%.6lfs] failed: request(%s) response(%s) "
          "status(%s)",
          server_ip_, server_port_, api_name, cntl.latency_us() / 1e6,
          request.ShortDebugString(), response.ShortDebugString(),
          BlockCacheErrCode_Name(response.status()));

      return ToStatus(response.status());
    }
  }

  LOG(ERROR) << absl::StrFormat(
      "[rpc][%s:%d][%s] failed: request(%s) exceed max retry times.",
      server_ip_, server_port_, api_name, request.ShortDebugString());

  return Status::Internal("rpc failed exceed max retry times");
};

}  // namespace cache
}  // namespace dingofs
