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

#include "cache/remotecache/remote_node_manager.h"

#include "utils/executor/timer_impl.h"

namespace dingofs {
namespace cache {

using dingofs::pb::mds::cachegroup::CacheGroupErrCode_Name;

RemoteNodeManagerImpl::RemoteNodeManagerImpl(RemoteBlockCacheOption option)
    : running_(false),
      option_(option),
      mds_base_(std::make_shared<stub::rpcclient::MDSBaseClient>()),
      mds_client_(std::make_shared<stub::rpcclient::MdsClientImpl>()),
      timer_(std::make_unique<TimerImpl>()),
      group_(nullptr) {}

Status RemoteNodeManagerImpl::Start() {
  if (!running_.exchange(true)) {
    LOG(INFO) << "Remote node manager starting...";

    auto rc =
        mds_client_->Init(NewMdsOption(option_.mds_addrs), mds_base_.get());
    if (rc != PB_FSStatusCode::OK) {
      return Status::Internal("init mds client failed");
    }

    Status status = RefreshMembers();
    if (!status.ok()) {
      LOG(ERROR) << "Load cache group members failed: " << status.ToString();
      return status;
    }

    CHECK(timer_->Start());
    timer_->Add([this] { BackgroudRefresh(); },
                option_.load_members_interval_ms);

    LOG(INFO) << "Remote node manager started.";
  }

  return Status::OK();
}

void RemoteNodeManagerImpl::Stop() {
  if (running_.exchange(false)) {
    timer_->Stop();
  }
}

void RemoteNodeManagerImpl::BackgroudRefresh() {
  auto status = RefreshMembers();
  if (!status.ok()) {
    LOG(ERROR) << "Refresh cache group members failed: " << status.ToString();
  }
  timer_->Add([this] { BackgroudRefresh(); }, option_.load_members_interval_ms);
}

Status RemoteNodeManagerImpl::RefreshMembers() {
  PB_CacheGroupMembers members;
  Status status = LoadMembers(&members);
  if (!status.ok()) {
    return status;
  } else if (!group_->IsDiff(members)) {
    return Status::OK();
  }

  auto group = std::make_shared<RemoteNodeGroup>(members);
  status = group->Init();
  if (!status.ok()) {
    return status;
  }

  LOG(INFO) << "Refresh cache group (name=" << option_.group_name
            << ") members success.";

  SetGroup(group);
  return Status::OK();
}

Status RemoteNodeManagerImpl::LoadMembers(PB_CacheGroupMembers* members) {
  auto status = mds_client_->LoadCacheGroupMembers(option_.group_name, members);
  if (status != PB_CacheGroupErrCode::CacheGroupOk) {
    LOG(ERROR) << "Load cache group members failed: "
               << CacheGroupErrCode_Name(status);
    return Status::Internal("load cache group member failed");
  }
  return Status::OK();
}

RemoteNodeSPtr RemoteNodeManagerImpl::GetNode(const std::string& key) {
  auto group = GetGroup();
  return group_->GetNode(key);
}

RemoteNodeGorupSPtr RemoteNodeManagerImpl::GetGroup() {
  ReadLockGuard lk(rwlock_);
  return group_;
}

void RemoteNodeManagerImpl::SetGroup(RemoteNodeGorupSPtr group) {
  WriteLockGuard lk(rwlock_);
  group_ = group;
}

}  // namespace cache
}  // namespace dingofs
