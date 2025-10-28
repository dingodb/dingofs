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
#include <brpc/channel.h>
#include <brpc/options.pb.h>
#include <brpc/reloadable_flags.h>
#include <butil/iobuf.h>
#include <butil/time.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <cmath>
#include <cstdint>

#include "cache/blockcache/block_cache.h"
#include "cache/common/type.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(rpc_connect_timeout_ms, 1000,
              "Timeout for rpc channel connect in milliseconds");
DEFINE_validator(rpc_connect_timeout_ms, brpc::PassValidate);

DEFINE_uint32(put_rpc_timeout_ms, 30000,
              "Timeout for put rpc request in milliseconds");
DEFINE_validator(put_rpc_timeout_ms, brpc::PassValidate);

DEFINE_uint32(range_rpc_timeout_ms, 30000,
              "Timeout for range rpc request in milliseconds");
DEFINE_validator(range_rpc_timeout_ms, brpc::PassValidate);

DEFINE_uint32(cache_rpc_timeout_ms, 30000,
              "Timeout for cache rpc request in milliseconds");
DEFINE_validator(cache_rpc_timeout_ms, brpc::PassValidate);

DEFINE_uint32(prefetch_rpc_timeout_ms, 3000,
              "Timeout for prefetch rpc request in milliseconds");
DEFINE_validator(prefetch_rpc_timeout_ms, brpc::PassValidate);

DEFINE_uint32(rpc_max_retry_times, 3, "Maximum retry times for rpc request");
DEFINE_validator(rpc_max_retry_times, brpc::PassValidate);

DEFINE_uint32(rpc_max_timeout_ms, 60000,
              "Maximum timeout for rpc request in milliseconds");
DEFINE_validator(rpc_max_timeout_ms, brpc::PassValidate);

static const std::string kModule = "rpc";

RPCClient::RPCClient(const std::string& server_ip, uint32_t server_port)
    : inited_(false),
      server_ip_(server_ip),
      server_port_(server_port),
      channel_(std::make_unique<brpc::Channel>()) {}

Status RPCClient::Init() { return InitChannel(server_ip_, server_port_); }

Status RPCClient::Put(ContextSPtr ctx, const BlockKey& key,
                      const Block& block) {
  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "put(%s,%zu)", key.Filename(),
                    block.size);
  StepTimerGuard guard(timer);

  pb::cache::PutRequest request;
  pb::cache::PutResponse response;
  IOBuffer buffer = block.buffer;
  *request.mutable_block_key() = key.ToPB();
  request.set_block_size(buffer.Size());

  status = SendRequest(ctx, "Put", request, buffer.ConstIOBuf(), response);
  return status;
}

Status RPCClient::Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                        size_t length, IOBuffer* buffer, RangeOption option) {
  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "%s(%s,%lld,%zu)",
                    option.is_subrequest ? "subrange" : "range", key.Filename(),
                    offset, length);
  StepTimerGuard guard(timer);

  pb::cache::RangeRequest request;
  pb::cache::RangeResponse response;
  butil::IOBuf response_attachment;
  *request.mutable_block_key() = key.ToPB();
  request.set_offset(offset);
  request.set_length(length);
  request.set_block_size(option.block_size);

  status = SendRequest(ctx, "Range", request, response, response_attachment);
  if (status.ok()) {
    *buffer = IOBuffer(response_attachment);
    ctx->SetCacheHit(response.has_cache_hit() ? response.cache_hit() : false);
  }
  return status;
}

Status RPCClient::Cache(ContextSPtr ctx, const BlockKey& key,
                        const Block& block) {
  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "cache(%s,%zu)",
                    key.Filename(), block.size);
  StepTimerGuard guard(timer);

  pb::cache::CacheRequest request;
  pb::cache::CacheResponse response;

  auto buffer = block.buffer;
  *request.mutable_block_key() = key.ToPB();
  request.set_block_size(buffer.Size());

  status = SendRequest(ctx, "Cache", request, buffer.ConstIOBuf(), response);
  return status;
}

Status RPCClient::Prefetch(ContextSPtr ctx, const BlockKey& key,
                           size_t length) {
  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "prefetch(%s,%zu)",
                    key.Filename(), length);
  StepTimerGuard guard(timer);

  pb::cache::PrefetchRequest request;
  pb::cache::PrefetchResponse response;

  *request.mutable_block_key() = key.ToPB();
  request.set_block_size(length);

  status = SendRequest(ctx, "Prefetch", request, response);
  return status;
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

  brpc::ChannelOptions options;
  options.connect_timeout_ms = FLAGS_rpc_connect_timeout_ms;
  options.connection_group = "common";
  rc = channel_->Init(ep, &options);
  if (rc != 0) {
    LOG(ERROR) << "Init channel for " << server_ip << ":" << server_port
               << " failed: rc = " << rc;
    return Status::Internal("init channel failed");
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

// TODO: consider retcode
bool RPCClient::ShouldRetry(const std::string& api_name, int /*retcode*/) {
  return api_name == "Range";
}

bool RPCClient::ShouldReset(int retcode) {
  return retcode != -brpc::ERPCTIMEDOUT && retcode != -ETIMEDOUT;
}

uint32_t RPCClient::NextTimeoutMs(const std::string& api_name,
                                  int retry_count) const {
  uint32_t timeout_ms;
  if (api_name == "Put") {
    timeout_ms = FLAGS_put_rpc_timeout_ms;
  } else if (api_name == "Range") {
    timeout_ms = FLAGS_range_rpc_timeout_ms;
  } else if (api_name == "Cache") {
    timeout_ms = FLAGS_cache_rpc_timeout_ms;
  } else if (api_name == "Prefetch") {
    timeout_ms = FLAGS_prefetch_rpc_timeout_ms;
  } else {
    CHECK(false) << "Unknown API name: " << api_name;
  }

  timeout_ms = timeout_ms * std::pow(2, retry_count);
  return std::min(timeout_ms, FLAGS_rpc_max_timeout_ms);
}

template <typename Request, typename Response>
Status RPCClient::SendRequest(ContextSPtr ctx, const std::string& api_name,
                              const Request& request, Response& response) {
  butil::IOBuf request_attachment, response_attachment;
  return SendRequest(ctx, api_name, request, request_attachment, response,
                     response_attachment);
}

template <typename Request, typename Response>
Status RPCClient::SendRequest(ContextSPtr ctx, const std::string& api_name,
                              const Request& request,
                              const butil::IOBuf& request_attachment,
                              Response& response) {
  butil::IOBuf response_attachment;
  return SendRequest(ctx, api_name, request, request_attachment, response,
                     response_attachment);
}

template <typename Request, typename Response>
Status RPCClient::SendRequest(ContextSPtr ctx, const std::string& api_name,
                              const Request& request, Response& response,
                              butil::IOBuf& response_attachment) {
  butil::IOBuf request_attachment;
  return SendRequest(ctx, api_name, request, request_attachment, response,
                     response_attachment);
}

template <typename Request, typename Response>
Status RPCClient::SendRequest(ContextSPtr ctx, const std::string& api_name,
                              const Request& request,
                              const butil::IOBuf& request_attachment,
                              Response& response,
                              butil::IOBuf& response_attachment) {
  const auto* method =
      pb::cache::BlockCacheService::descriptor()->FindMethodByName(api_name);

  if (method == nullptr) {
    LOG(FATAL) << "Unknown api name: " << api_name;
  }

  auto connection_type = (api_name == "Range") ? brpc::CONNECTION_TYPE_POOLED
                                               : brpc::CONNECTION_TYPE_SINGLE;

  butil::Timer timer;
  timer.start();

  for (int retry_count = 0; retry_count < FLAGS_rpc_max_retry_times;
       ++retry_count) {
    brpc::Controller cntl;
    cntl.set_connection_type(connection_type);
    cntl.set_timeout_ms(NextTimeoutMs(api_name, retry_count));
    cntl.set_request_id(ctx->TraceId());
    cntl.request_attachment() = request_attachment;
    cntl.ignore_eovercrowded();

    auto* channel = GetChannel();
    if (channel == nullptr) {
      LOG(ERROR) << absl::StrFormat(
          "[%s][rpc][%s][%s:%d] channel is not inited, retrying...",
          ctx->StrTraceId(), api_name, server_ip_, server_port_);
      ResetChannel();
      continue;
    }

    channel->CallMethod(method, &cntl, &request, &response, nullptr);

    // network error
    if (cntl.Failed()) {
      LOG(ERROR) << absl::StrFormat(
          "[%s][rpc][%s][%s:%d][%.6lf] failed: request(%s) cntl_code(%d) "
          "cntl_error(%s)",
          ctx->TraceId(), api_name, server_ip_, server_port_,
          cntl.latency_us() / 1e6, request.ShortDebugString(), cntl.ErrorCode(),
          cntl.ErrorText());

      if (!ShouldRetry(api_name, cntl.ErrorCode())) {
        return Status::NetError(cntl.ErrorCode(), cntl.ErrorText());
      }

      ResetChannel();  // FIXME: don't reset channel if raise net error?
      continue;
    }

    // response status is ok
    if (response.status() == pb::cache::BlockCacheOk) {
      response_attachment = cntl.response_attachment();
      return Status::OK();
    } else {
      LOG(ERROR) << absl::StrFormat(
          "[%s][rpc][%s][%s:%d][%.6lf] failed: request(%s) response(%s) "
          "status(%s)",
          ctx->TraceId(), api_name, server_ip_, server_port_,
          cntl.latency_us() / 1e6, request.ShortDebugString(),
          response.ShortDebugString(),
          pb::cache::BlockCacheErrCode_Name(response.status()));

      return ToStatus(response.status());
    }
  }

  timer.stop();

  LOG(ERROR) << absl::StrFormat(
      "[%s][rpc][%s][%s:%d][%.6lf] failed: request(%s) exceed max retry times "
      "(%d).",
      ctx->TraceId(), api_name, server_ip_, server_port_,
      timer.u_elapsed(1.0) / 1e6, request.ShortDebugString(),
      FLAGS_rpc_max_retry_times);

  return Status::Internal("rpc failed exceed max retry times");
};

}  // namespace cache
}  // namespace dingofs
