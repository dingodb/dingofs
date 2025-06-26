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

#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

using dingofs::pb::mds::cachegroup::CacheGroupErrCode_Name;

RemoteNodeManager::RemoteNodeManager(RemoteBlockCacheOption option,
                                     OnMemberLoad on_member_load)
    : running_(false),
      option_(option),
      on_member_load_(on_member_load),
      mds_base_(std::make_shared<stub::rpcclient::MDSBaseClient>()),
      mds_client_(std::make_shared<stub::rpcclient::MdsClientImpl>()),
      executor_(std::make_unique<BthreadExecutor>()) {}

Status RemoteNodeManager::Start() {
  if (!running_.exchange(true)) {
    LOG(INFO) << "Remote node manager starting...";

    auto rc = mds_client_->Init(option_.mds_option, mds_base_.get());
    if (rc != PBFSStatusCode::OK) {
      return Status::Internal("init mds client failed");
    }

    Status status = RefreshMembers();
    if (!status.ok()) {
      LOG(ERROR) << "Load cache group members failed: " << status.ToString();
      return status;
    }

    CHECK(executor_->Start());
    executor_->Schedule([this] { BackgroudRefresh(); },
                        FLAGS_load_members_interval_ms);

    LOG(INFO) << "Remote node manager started.";
  }

  return Status::OK();
}

void RemoteNodeManager::Stop() {
  if (running_.exchange(false)) {
    executor_->Stop();
  }
}

void RemoteNodeManager::BackgroudRefresh() {
  auto status = RefreshMembers();
  if (!status.ok()) {
    LOG(ERROR) << "Refresh cache group members failed: " << status.ToString();
  }

  executor_->Schedule([this] { BackgroudRefresh(); },
                      FLAGS_load_members_interval_ms);
}

Status RemoteNodeManager::RefreshMembers() {
  PBCacheGroupMembers members;
  Status status = LoadMembers(&members);
  if (status.ok()) {
    status = on_member_load_(members);
  }
  return status;
}

Status RemoteNodeManager::LoadMembers(PBCacheGroupMembers* members) {
  auto status =
      mds_client_->LoadCacheGroupMembers(option_.cache_group, members);
  if (status != PBCacheGroupErrCode::CacheGroupOk) {
    LOG(ERROR) << "Load cache group members failed: "
               << CacheGroupErrCode_Name(status);
    return Status::Internal("load cache group member failed");
  }

  VLOG(9) << "Load cache group members: size = " << members->size();

  return Status::OK();
}

}  // namespace cache
}  // namespace dingofs
