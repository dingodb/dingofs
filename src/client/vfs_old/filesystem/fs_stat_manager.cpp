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

#include "client/vfs_old/filesystem/fs_stat_manager.h"

#include <memory>

#include "client/common/dynamic_config.h"
#include "client/vfs_old/inode_wrapper.h"
#include "common/define.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace filesystem {

using filesystem::Ino;
using utils::ReadLockGuard;
using utils::RWLock;
using utils::WriteLockGuard;

using pb::metaserver::MetaStatusCode;
using pb::metaserver::Quota;
using pb::metaserver::Usage;

USING_FLAG(fs_usage_flush_interval_second)

void FsQuota::UpdateUsage(int64_t new_space, int64_t new_inodes) {
  VLOG(6) << "UpdateFsUsage new_space:" << new_space
          << ", new_inodes:" << new_inodes;

  if (new_space != 0) {
    new_space_.fetch_add(new_space, std::memory_order_relaxed);
  }

  if (new_inodes != 0) {
    new_inodes_.fetch_add(new_inodes, std::memory_order_relaxed);
  }
}

bool FsQuota::CheckQuota(int64_t new_space, int64_t new_inodes) {
  Quota quota = GetQuota();

  VLOG(6) << "CheckFsQuota  new_space:" << new_space
          << ", new_inodes:" << new_inodes << ", fs_quota:" << ToString();

  if (quota.maxbytes() > 0 &&
      (new_space + new_space_.load(std::memory_order_relaxed) +
       quota.usedbytes()) > (int64_t)quota.maxbytes()) {
    LOG(INFO) << "CheckFsQuota check space failed, new_space:" << new_space
              << ", fs_quota:" << ToString();
    return false;
  }

  if (quota.maxinodes() > 0 &&
      (new_inodes + new_inodes_.load(std::memory_order_relaxed) +
       quota.usedinodes()) > (int64_t)quota.maxinodes()) {
    LOG(INFO) << "CheckFsQuota check inodes failed, new_inodes:" << new_inodes
              << ", quota:" << ToString();
    return false;
  }

  return true;
}

void FsQuota::Refresh(Quota quota) {
  VLOG(6) << "RefreshFsQuota old fs_quota: " << ToString()
          << ", new quota: " << quota.ShortDebugString();
  WriteLockGuard lk(rwlock_);
  quota_ = quota;
}

Usage FsQuota::GetUsage() {
  Usage usage;
  usage.set_bytes(new_space_.load(std::memory_order_relaxed));
  usage.set_inodes(new_inodes_.load(std::memory_order_relaxed));
  VLOG(6) << "GetFsUsage usage: " << usage.ShortDebugString();
  return usage;
}

Quota FsQuota::GetQuota() {
  ReadLockGuard lk(rwlock_);
  return quota_;
}

std::string FsQuota::ToString() {
  std::ostringstream oss;
  oss << "FsQuota{ino=" << ino_;
  {
    ReadLockGuard lk(rwlock_);
    oss << ", quota=" << quota_.ShortDebugString();
  }
  oss << ", new_space_=" << new_space_.load(std::memory_order_relaxed)
      << ", new_inodes_=" << new_inodes_.load(std::memory_order_relaxed) << "}";

  return oss.str();
}

void FsStatManager::Start() {
  if (running_.load()) {
    return;
  }

  InitQuota();

  DINGOFS_ERROR rc = LoadFsQuota();

  if (rc != DINGOFS_ERROR::OK) {
    LOG(FATAL) << "LoadFsQuota failed, rc = " << rc;
    return;
  }

  timer_->Add([this] { FlushFsUsage(); },
              FLAGS_fs_usage_flush_interval_second * 1000);

  running_.store(true);
}

void FsStatManager::InitQuota() {
  Quota quota;
  fs_quota_ = std::make_unique<FsQuota>(ROOTINODEID, quota);
}

void FsStatManager::Stop() {
  if (!running_.load()) {
    return;
  }

  DoFlushFsUsage();

  running_.store(false);
}

void FsStatManager::UpdateFsQuotaUsage(int64_t new_space, int64_t new_inodes) {
  CHECK_NOTNULL(fs_quota_);
  VLOG(3) << "UpdateFsQuotaUsage, new_space: " << new_space
          << ", new_inodes: " << new_inodes
          << ", cur fs_quota: " << fs_quota_->ToString();
  fs_quota_->UpdateUsage(new_space, new_inodes);
}

bool FsStatManager::CheckFsQuota(int64_t new_space, int64_t new_inodes) {
  CHECK_NOTNULL(fs_quota_);
  bool check = fs_quota_->CheckQuota(new_space, new_inodes);
  if (!check) {
    LOG(WARNING) << "CheckFsQuota failed, fs_id: " << fs_id_
                 << ", new_space: " << new_space
                 << ", new_inodes: " << new_inodes;
  }
  return check;
}

DINGOFS_ERROR FsStatManager::LoadFsQuota() {
  Quota quota;
  MetaStatusCode rc = meta_client_->GetFsQuota(fs_id_, quota);
  if (rc == MetaStatusCode::OK) {
    fs_quota_->Refresh(quota);
    return DINGOFS_ERROR::OK;
  }

  CHECK(rc != MetaStatusCode::NOT_FOUND)
      << "fs quota must be set before mount, fs_id: " << fs_id_;

  return DINGOFS_ERROR::INTERNAL;
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
    VLOG(3) << "FlushFsUsage success, usage: " << usage.ShortDebugString()
            << ", new_quota: " << new_quota.ShortDebugString()
            << ", cur fs_quota: " << fs_quota_->ToString();
    UpdateFsQuotaUsage(-usage.bytes(), -usage.inodes());
    fs_quota_->Refresh(new_quota);
  } else if (rc == MetaStatusCode::NOT_FOUND) {
    LOG(INFO) << "FlushFsUsage fs quot not fount, fs_id: " << fs_id_;
    InitQuota();
  } else {
    LOG(WARNING) << "FlushFsUsage failed, fs_id: " << fs_id_ << ", rc: " << rc;
  }
}

Quota FsStatManager::GetFsQuota() {
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
}  // namespace dingofs