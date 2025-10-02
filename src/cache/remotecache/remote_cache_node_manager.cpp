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

#include <absl/strings/str_join.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>

#include "cache/common/macro.h"
#include "cache/common/mds_client.h"
#include "cache/metric/cache_status.h"
#include "options/cache/option.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(load_members_interval_ms, 1000,
              "Interval to load members of the cache group in milliseconds");

RemoteCacheNodeManager::RemoteCacheNodeManager(OnMemberLoadFn on_member_load_fn)
    : running_(false),
      mds_client_(std::make_unique<MDSClientImpl>(FLAGS_mds_addrs)),
      on_member_load_fn_(on_member_load_fn),
      executor_(std::make_unique<BthreadExecutor>()) {}

Status RemoteCacheNodeManager::Start() {
  CHECK_NOTNULL(mds_client_);
  CHECK_NOTNULL(on_member_load_fn_);
  CHECK_NOTNULL(executor_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Remote node manager is starting...";

  auto status = mds_client_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start mds client failed: " << status.ToString();
    return status;
  }

  status = RefreshMembers();
  if (!status.ok()) {
    LOG(ERROR) << "Load cache group members failed: " << status.ToString();
    return status;
  }

  CHECK(executor_->Start());
  executor_->Schedule([this] { BackgroudRefresh(); },
                      FLAGS_load_members_interval_ms);

  SetStatusPage();

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

  CHECK(executor_->Stop());

  LOG(INFO) << "Remote node manager is down.";

  CHECK_DOWN("Remote node manager");
}

void RemoteCacheNodeManager::BackgroudRefresh() {
  RefreshMembers();
  executor_->Schedule([this] { BackgroudRefresh(); },
                      FLAGS_load_members_interval_ms);
}

Status RemoteCacheNodeManager::RefreshMembers() {
  std::vector<CacheGroupMember> members;
  Status status = LoadMembers(&members);
  if (status.ok()) {
    on_member_load_fn_(members);
    return members.empty() ? Status::NotFound("no member in cache group")
                           : Status::OK();
  }
  return status;
}

Status RemoteCacheNodeManager::LoadMembers(
    std::vector<CacheGroupMember>* members) {
  auto status = mds_client_->ListMembers(FLAGS_cache_group, members);
  if (!status.ok()) {
    LOG(ERROR) << "List cache group members from mds failed: "
               << status.ToString();
    return Status::Internal("load cache group member failed");
  }

  VLOG(9) << "Load cache group members success: member count = "
          << members->size();

  return Status::OK();
}

void RemoteCacheNodeManager::SetStatusPage() const {
  CacheStatus::Update([&](CacheStatus::Root& root) {
    auto& remote_cache = root.remote_cache;
    remote_cache.mds_addrs = FLAGS_mds_addrs;
    remote_cache.cache_group = FLAGS_cache_group;
  });
}

}  // namespace cache
}  // namespace dingofs
