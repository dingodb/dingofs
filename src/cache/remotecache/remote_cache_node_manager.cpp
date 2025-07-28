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

#include "cache/remotecache/remote_cache_node_manager.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "cache/common/macro.h"
#include "cache/debug/expose.h"
#include "options/cache/tiercache.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(load_members_interval_ms, 1000,
              "Interval to load members of the cache group in milliseconds");

using dingofs::pb::mds::cachegroup::CacheGroupErrCode_Name;

RemoteCacheNodeManager::RemoteCacheNodeManager(RemoteBlockCacheOption option,
                                               OnMemberLoad on_member_load)
    : running_(false),
      option_(option),
      on_member_load_(on_member_load),
      mds_base_(std::make_shared<stub::rpcclient::MDSBaseClient>()),
      mds_client_(std::make_shared<stub::rpcclient::MdsClientImpl>()),
      executor_(std::make_unique<BthreadExecutor>()) {}

Status RemoteCacheNodeManager::Start() {
  CHECK_NOTNULL(on_member_load_);
  CHECK_NOTNULL(mds_base_);
  CHECK_NOTNULL(mds_client_);
  CHECK_NOTNULL(executor_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Remote node manager is starting...";

  auto rc = mds_client_->Init(option_.mds_option, mds_base_.get());
  if (rc != PBFSStatusCode::OK) {
    LOG(ERROR) << "Init MDS client failed: rc = " << rc;
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

  ExposeCacheGroupName(option_.cache_group);

  running_ = true;

  LOG(INFO) << "Remote node manager is up.";

  CHECK_RUNNING("Remote node manager");
  return Status::OK();
}

void RemoteCacheNodeManager::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  LOG(INFO) << "Remote node manager is shutting down...";

  executor_->Stop();

  LOG(INFO) << "Remote node manager is down.";

  CHECK_DOWN("Remote node manager");
}

void RemoteCacheNodeManager::BackgroudRefresh() {
  auto status = RefreshMembers();
  if (!status.ok()) {
    LOG(ERROR) << "Refresh cache group members failed: " << status.ToString();
  }

  executor_->Schedule([this] { BackgroudRefresh(); },
                      FLAGS_load_members_interval_ms);
}

Status RemoteCacheNodeManager::RefreshMembers() {
  PBCacheGroupMembers members;
  Status status = LoadMembers(&members);
  if (status.ok()) {
    status = on_member_load_(members);
  }
  return status;
}

Status RemoteCacheNodeManager::LoadMembers(PBCacheGroupMembers* members) {
  auto status =
      mds_client_->LoadCacheGroupMembers(option_.cache_group, members);
  if (status != PBCacheGroupErrCode::CacheGroupOk) {
    LOG(ERROR) << "Load cache group members failed: "
               << CacheGroupErrCode_Name(status);
    return Status::Internal("load cache group member failed");
  }

  VLOG(9) << "Load cache group members success: member count = "
          << members->size();

  return Status::OK();
}

}  // namespace cache
}  // namespace dingofs
