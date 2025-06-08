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
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/remote_node.h"

#include "cache/utils/state_machine_impl.h"

namespace dingofs {
namespace cache {

RemoteNodeImpl::RemoteNodeImpl(const PB_CacheGroupMember& member,
                               RemoteAccessOption option)
    : member_(member),
      option_(option),
      channel_(std::make_unique<brpc::Channel>()),
      state_machine_(std::make_unique<StateMachineImpl>()) {}

Status RemoteNodeImpl::Init() {
  return InitChannel(member_.ip(), member_.port());
}

Status RemoteNodeImpl::Put(const BlockKey& key, const Block& block) {
  auto status = CheckHealth();
  if (status.ok()) {
    status = RemotePut(key, block);
  }
  return status;
}

Status RemoteNodeImpl::Range(const BlockKey& key, off_t offset, size_t length,
                             IOBuffer* buffer, size_t block_size) {
  auto status = CheckHealth();
  if (status.ok()) {
    status = RemoteRange(key, offset, length, buffer, block_size);
  }
  return status;
}

Status RemoteNodeImpl::Cache(const BlockKey& key, const Block& block) {
  auto status = CheckHealth();
  if (status.ok()) {
    status = RemoteCache(key, block);
  }
  return status;
}

Status RemoteNodeImpl::Prefetch(const BlockKey& key, size_t length) {
  auto status = CheckHealth();
  if (status.ok()) {
    status = RemotePrefetch(key, length);
  }
  return status;
}

Status RemoteNodeImpl::RemotePut(const BlockKey& key, const Block& block) {
  brpc::Controller cntl;
  PB_PutRequest request;
  PB_PutResponse response;

  auto buffer = block.buffer;
  *request.mutable_block_key() = key.ToPB();
  request.set_block_size(buffer.Size());
  cntl.request_attachment().append(buffer.IOBuf());
  cntl.set_timeout_ms(option_.remote_put_rpc_timeout_ms);
  PB_BlockCacheService_Stub stub(channel_.get());

  stub.Put(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Send block put request to remote failed: "
               << cntl.ErrorText();
    return Status::IoError("put failed");
  }

  return Status::OK();
}

Status RemoteNodeImpl::RemoteRange(const BlockKey& key, off_t offset,
                                   size_t length, IOBuffer* buffer,
                                   size_t block_size) {
  brpc::Controller cntl;
  PB_RangeRequest request;
  PB_RangeResponse response;

  *request.mutable_block_key() = key.ToPB();
  request.set_offset(offset);
  request.set_length(length);
  request.set_block_size(block_size);
  cntl.set_timeout_ms(option_.remote_range_rpc_timeout_ms);
  PB_BlockCacheService_Stub stub(channel_.get());

  stub.Range(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Send block range request to remote failed: "
               << cntl.ErrorText();
    return Status::IoError("range failed");
  }

  auto status = response.status();
  if (status == PB_BlockCacheErrCode::BlockCacheOk) {
    *buffer = IOBuffer(cntl.response_attachment());
    return Status::OK();
  }
  return Status::IoError("range failed");
}

Status RemoteNodeImpl::RemoteCache(const BlockKey& key, const Block& block) {
  brpc::Controller cntl;
  PB_CacheRequest request;
  PB_CacheResponse response;

  auto buffer = block.buffer;
  *request.mutable_block_key() = key.ToPB();
  request.set_block_size(buffer.Size());
  cntl.request_attachment().append(buffer.IOBuf());
  cntl.set_timeout_ms(option_.remote_cache_rpc_timeout_ms);
  PB_BlockCacheService_Stub stub(channel_.get());

  stub.Cache(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Send block cache request to remote failed: "
               << cntl.ErrorText();
    return Status::IoError("cache failed");
  }

  return Status::OK();
}

Status RemoteNodeImpl::RemotePrefetch(const BlockKey& key, size_t length) {
  brpc::Controller cntl;
  PB_PrefetchRequest request;
  PB_PrefetchResponse response;

  *request.mutable_block_key() = key.ToPB();
  request.set_block_size(length);
  cntl.set_timeout_ms(option_.remote_cache_rpc_timeout_ms);

  PB_BlockCacheService_Stub stub(channel_.get());
  stub.Prefetch(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Send block prefetch request to remote failed: "
               << cntl.ErrorText();
    return Status::IoError("prefetch failed");
  }

  return Status::OK();
}

Status RemoteNodeImpl::InitChannel(const std::string& listen_ip,
                                   uint32_t listen_port) {
  butil::EndPoint ep;
  int rc = butil::str2endpoint(listen_ip.c_str(), listen_port, &ep);
  if (rc != 0) {
    LOG(ERROR) << "str2endpoint(" << listen_ip << "," << listen_port
               << ") failed, rc = " << rc;
    return Status::Internal("str2endpoint() failed");
  }

  rc = channel_->Init(ep, nullptr);
  if (rc != 0) {
    LOG(INFO) << "Init channel for " << listen_ip << ":" << listen_port
              << " failed, rc = " << rc;
    return Status::Internal("Init channel failed");
  }

  LOG(INFO) << "Create channel for address (" << listen_ip << ":" << listen_port
            << ") success.";
  return Status::OK();
}

Status RemoteNodeImpl::ResetChannel() {
  std::lock_guard<BthreadMutex> mutex(mutex_);
  if (channel_->CheckHealth() != 0) {
    return InitChannel(member_.ip(), member_.port());
  }
  return Status::OK();
}

Status RemoteNodeImpl::CheckHealth() const {
  if (state_machine_->GetState() == State::kStateNormal) {
    return Status::OK();
  }
  return Status::Internal("remote node is unhealthy");
}

Status RemoteNodeImpl::CheckStatus(Status status) {
  if (status.ok() || status.IsNotFound()) {
    state_machine_->Success();
  } else {
    state_machine_->Error();
  }
  return status;
}

}  // namespace cache
}  // namespace dingofs
