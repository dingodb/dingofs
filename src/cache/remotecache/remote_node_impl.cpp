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

#include "cache/remotecache/remote_node_impl.h"

#include "cache/utils/state_machine_impl.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

RemoteNodeImpl::RemoteNodeImpl(const PBCacheGroupMember& member,
                               RemoteNodeOption option)
    : rpc_(std::make_unique<RPCClient>(member.ip(), member.port(), option)),
      state_machine_(std::make_unique<StateMachineImpl>()) {}

Status RemoteNodeImpl::Init() {
  auto status = rpc_->Init();
  if (!status.ok()) {
    return status;
  }

  if (!state_machine_->Start()) {
    return Status::Internal("state machine start failed");
  }

  return Status::OK();
}

Status RemoteNodeImpl::Destroy() {
  if (!state_machine_->Stop()) {
    return Status::Internal("state machine stop failed");
  }

  return Status::OK();
}

Status RemoteNodeImpl::Put(const BlockKey& key, const Block& block) {
  auto status = CheckHealth();
  if (!status.ok()) {
    return status;
  }

  status = rpc_->Put(key, block);
  return CheckStatus(status);
}

Status RemoteNodeImpl::Range(const BlockKey& key, off_t offset, size_t length,
                             IOBuffer* buffer, size_t block_size) {
  auto status = CheckHealth();
  if (!status.ok()) {
    return status;
  }

  status = rpc_->Range(key, offset, length, buffer, block_size);
  return CheckStatus(status);
}

Status RemoteNodeImpl::Cache(const BlockKey& key, const Block& block) {
  auto status = CheckHealth();
  if (!status.ok()) {
    return status;
  }

  status = CheckStatus(rpc_->Cache(key, block));
  return CheckStatus(status);
}

Status RemoteNodeImpl::Prefetch(const BlockKey& key, size_t length) {
  auto status = CheckHealth();
  if (!status.ok()) {
    return status;
  }

  status = rpc_->Prefetch(key, length);
  return CheckStatus(status);
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
