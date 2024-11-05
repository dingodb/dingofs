// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "curvefs/src/client/filesystem/fs_stat_manager.h"

#include <memory>

#include "curvefs/src/client/common/dynamic_config.h"
#include "curvefs/src/client/filesystem/dir_quota.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/common/define.h"
#include "glog/logging.h"

namespace curvefs {
namespace client {
namespace filesystem {

USING_FLAG(fs_usage_flush_interval_second)

void FsStatManager::Start() {
  if (running_.load()) {
    return;
  }

  InitQuota();

  CURVEFS_ERROR rc = LoadFsQuota();

  if (rc != CURVEFS_ERROR::OK) {
    LOG(FATAL) << "LoadFsQuota failed, rc = " << rc;
    return;
  }

  timer_->Add([this] { FlushFsUsage(); },
              FLAGS_fs_usage_flush_interval_second * 1000);

  running_.store(true);
}

void FsStatManager::InitQuota() {
  Quota quota;
  fs_quota_ = std::make_unique<DirQuota>(ROOTINODEID, quota);
}

void FsStatManager::Stop() {
  if (!running_.load()) {
    return;
  }

  DoFlushFsUsage();

  running_.store(false);
}

void FsStatManager::UpdateQuotaUsage(int64_t new_space, int64_t new_inodes) {
  CHECK_NOTNULL(fs_quota_);
  fs_quota_->UpdateUsage(new_space, new_inodes);
}

bool FsStatManager::CheckQuota(int64_t new_space, int64_t new_inodes) {
  CHECK_NOTNULL(fs_quota_);
  bool check = fs_quota_->CheckQuota(new_space, new_inodes);
  if (!check) {
    LOG(WARNING) << "CheckFsQuota failed, fs_id: " << fs_id_
                 << ", new_space: " << new_space
                 << ", new_inodes: " << new_inodes;
  }
  return check;
}

CURVEFS_ERROR FsStatManager::LoadFsQuota() {
  Quota quota;
  MetaStatusCode rc = meta_client_->GetFsQuota(fs_id_, quota);
  if (rc == MetaStatusCode::OK) {
    fs_quota_->Refresh(quota);
    return CURVEFS_ERROR::OK;
  }

  CHECK(rc != MetaStatusCode::NOT_FOUND)
      << "fs quota must be set before mount, fs_id: " << fs_id_;

  return CURVEFS_ERROR::INTERNAL;
}

void FsStatManager::FlushFsUsage() {
  if (!running_.load()) {
    LOG(INFO) << "FlushFsUsage not running";
    return;
  }

  DoFlushFsUsage();

  timer_->Add([this] { FlushFsUsage(); },
              FLAGS_fs_usage_flush_interval_second * 1000);
}

void FsStatManager::DoFlushFsUsage() {
  Usage usage = fs_quota_->GetUsage();
  Quota new_quota;
  MetaStatusCode rc = meta_client_->FlushFsUsage(fs_id_, usage, new_quota);
  if (rc == MetaStatusCode::OK) {
    VLOG(6) << "FlushFsUsage success, new_quota: "
            << new_quota.ShortDebugString();
    fs_quota_->UpdateUsage(-usage.bytes(), -usage.inodes());
    fs_quota_->Refresh(new_quota);
  } else if (rc == MetaStatusCode::NOT_FOUND) {
    VLOG(3) << "FlushFsUsage fs quot not fount, fs_id: " << fs_id_;
    InitQuota();
  } else {
    LOG(WARNING) << "FlushFsUsage failed, fs_id: " << fs_id_ << ", rc: " << rc;
  }
}

Quota FsStatManager::GetQuota() {
  CHECK(running_.load());
  Quota quota = fs_quota_->GetQuota();
  Usage usage = fs_quota_->GetUsage();
  quota.set_usedbytes(quota.usedbytes() + usage.bytes());
  quota.set_usedinodes(quota.usedinodes() + usage.inodes());

  VLOG(6) << "GetQuota, quota: " << quota.ShortDebugString();
  return quota;
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs