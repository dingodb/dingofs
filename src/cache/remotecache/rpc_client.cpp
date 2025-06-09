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

#include "cache/config/remote_cache.h"

namespace dingofs {
namespace cache {

RPCClient::RPCClient(const std::string& server_ip, uint32_t server_port,
                     RemoteNodeOption option)
    : server_ip_(server_ip),
      server_port_(server_port),
      option_(option),
      channel_(std::make_unique<brpc::Channel>()) {}

Status RPCClient::Init() { return InitChannel(server_ip_, server_port_); }

Status RPCClient::Put(const BlockKey& key, const Block& block) {
  brpc::Controller cntl;
  PBPutRequest request;
  PBPutResponse response;

  auto buffer = block.buffer;
  *request.mutable_block_key() = key.ToPB();
  request.set_block_size(buffer.Size());
  cntl.request_attachment().append(buffer.IOBuf());
  cntl.set_timeout_ms(option_.remote_put_rpc_timeout_ms);
  PBBlockCacheService_Stub stub(channel_.get());

  stub.Put(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Send block put request to remote failed: "
               << cntl.ErrorText();
    return Status::IoError("put failed");
  } else if (response.status() != PBBlockCacheErrCode::BlockCacheOk) {
    LOG(ERROR) << "Remote put failed: "
               << response.status();  // Log the error code
    return Status::IoError("put failed");
  }

  return Status::OK();
}

Status RPCClient::Range(const BlockKey& key, off_t offset, size_t length,
                        IOBuffer* buffer, size_t block_size) {
  brpc::Controller cntl;
  PBRangeRequest request;
  PBRangeResponse response;

  *request.mutable_block_key() = key.ToPB();
  request.set_offset(offset);
  request.set_length(length);
  request.set_block_size(block_size);
  cntl.set_timeout_ms(option_.remote_range_rpc_timeout_ms);
  PBBlockCacheService_Stub stub(channel_.get());

  stub.Range(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Send block range request to remote failed: "
               << cntl.ErrorText();
    return Status::IoError("range failed");
  }

  auto status = response.status();
  if (status == PBBlockCacheErrCode::BlockCacheOk) {
    *buffer = IOBuffer(cntl.response_attachment());
    return Status::OK();
  }
  return Status::IoError("range failed");
}

Status RPCClient::Cache(const BlockKey& key, const Block& block) {
  brpc::Controller cntl;
  PBCacheRequest request;
  PBCacheResponse response;

  auto buffer = block.buffer;
  *request.mutable_block_key() = key.ToPB();
  request.set_block_size(buffer.Size());
  cntl.request_attachment().append(buffer.IOBuf());
  cntl.set_timeout_ms(option_.remote_cache_rpc_timeout_ms);
  PBBlockCacheService_Stub stub(channel_.get());

  stub.Cache(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Send block cache request to remote failed: "
               << cntl.ErrorText();
    return Status::IoError("cache failed");
  }

  return Status::OK();
}

Status RPCClient::Prefetch(const BlockKey& key, size_t length) {
  brpc::Controller cntl;
  PBPrefetchRequest request;
  PBPrefetchResponse response;

  *request.mutable_block_key() = key.ToPB();
  request.set_block_size(length);
  cntl.set_timeout_ms(option_.remote_cache_rpc_timeout_ms);

  PBBlockCacheService_Stub stub(channel_.get());
  stub.Prefetch(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Send block prefetch request to remote failed: "
               << cntl.ErrorText();
    return Status::IoError("prefetch failed");
  }

  return Status::OK();
}

Status RPCClient::InitChannel(const std::string& server_ip,
                              uint32_t server_port) {
  butil::EndPoint ep;
  int rc = butil::str2endpoint(server_ip.c_str(), server_port, &ep);
  if (rc != 0) {
    LOG(ERROR) << "str2endpoint(" << server_ip << "," << server_port
               << ") failed, rc = " << rc;
    return Status::Internal("str2endpoint() failed");
  }

  rc = channel_->Init(ep, nullptr);
  if (rc != 0) {
    LOG(INFO) << "Init channel for " << server_ip << ":" << server_port
              << " failed, rc = " << rc;
    return Status::Internal("Init channel failed");
  }

  LOG(INFO) << "Create channel for address (" << server_ip << ":" << server_port
            << ") success.";
  return Status::OK();
}

Status RPCClient::ResetChannel() {
  std::lock_guard<BthreadMutex> mutex(mutex_);
  if (channel_->CheckHealth() != 0) {
    return InitChannel(server_ip_, server_port_);
  }
  return Status::OK();
}

}  // namespace cache
}  // namespace dingofs
