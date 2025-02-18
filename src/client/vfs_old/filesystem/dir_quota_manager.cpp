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

#include "client/vfs_old/filesystem/dir_quota_manager.h"

#include <atomic>
#include <cstdint>
#include <unordered_map>

#include "client/vfs_old/common/dynamic_config.h"
#include "client/vfs_old/inode_wrapper.h"
#include "common/define.h"
#include "utils/concurrent/concurrent.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace filesystem {

using utils::ReadLockGuard;
using utils::RWLock;
using utils::WriteLockGuard;

using pb::metaserver::MetaStatusCode;
using pb::metaserver::Quota;
using pb::metaserver::Usage;

USING_FLAG(flush_quota_interval_second);
USING_FLAG(load_quota_interval_second);

void DirQuota::UpdateUsage(int64_t new_space, int64_t new_inodes) {
  VLOG(6) << "UpdateUsage dir inodeId=" << ino_ << " new_space:" << new_space
          << ", new_inodes:" << new_inodes;
  if (new_space != 0) {
    new_space_.fetch_add(new_space, std::memory_order_relaxed);
  }

  if (new_inodes != 0) {
    new_inodes_.fetch_add(new_inodes, std::memory_order_relaxed);
  }
}

void DirQuota::FlushedUsage(int64_t new_space, int64_t new_inodes) {
  VLOG(6) << "FlushedUsage dir inodeId=" << ino_ << " new_space:" << new_space
          << ", new_inodes:" << new_inodes << ", dir_quota:" << ToString();
  if (new_space != 0) {
    new_space_.fetch_sub(new_space, std::memory_order_relaxed);
  }

  if (new_inodes != 0) {
    new_inodes_.fetch_sub(new_inodes, std::memory_order_relaxed);
  }

  WriteLockGuard lk(rwlock_);
  quota_.set_usedbytes(quota_.usedbytes() + new_space);
  quota_.set_usedinodes(quota_.usedinodes() + new_inodes);
}

bool DirQuota::CheckQuota(int64_t new_space, int64_t new_inodes) {
  Quota quota = GetQuota();

  VLOG(6) << "CheckQuota dir inodeId=" << ino_ << " new_space:" << new_space
          << ", new_inodes:" << new_inodes << ", dir_quota:" << ToString();

  if (new_space > 0 && quota.maxbytes() > 0 &&
      (new_space + new_space_.load(std::memory_order_relaxed) +
       quota.usedbytes()) > (int64_t)quota.maxbytes()) {
    LOG(INFO) << "CheckQuota check space failed, new_space:" << new_space
              << ", quota:" << ToString();
    return false;
  }

  if (new_inodes > 0 && quota.maxinodes() > 0 &&
      (new_inodes + new_inodes_.load(std::memory_order_relaxed) +
       quota.usedinodes()) > (int64_t)quota.maxinodes()) {
    LOG(INFO) << "CheckQuota check inode failed, new_inodes:" << new_inodes
              << ", quota:" << ToString();
    return false;
  }

  return true;
}

void DirQuota::Refresh(Quota quota) {
  VLOG(6) << "Refresh dir quota, old dir_quota: " << ToString()
          << ", new quota: " << quota.ShortDebugString();
  WriteLockGuard lk(rwlock_);
  quota_ = quota;
}

Usage DirQuota::GetUsage() {
  Usage usage;
  usage.set_bytes(new_space_.load(std::memory_order_relaxed));
  usage.set_inodes(new_inodes_.load(std::memory_order_relaxed));
  VLOG(6) << "GetUsage dir inodeId=" << ino_
          << ", usage: " << usage.ShortDebugString();
  return usage;
}

Quota DirQuota::GetQuota() {
  ReadLockGuard lk(rwlock_);
  return quota_;
}

std::string DirQuota::ToString() {
  std::ostringstream oss;
  oss << "DirQuota{ino=" << ino_;
  {
    ReadLockGuard lk(rwlock_);
    oss << ", quota=" << quota_.ShortDebugString();
  }
  oss << ", new_space_=" << new_space_.load(std::memory_order_relaxed)
      << ", new_inodes_=" << new_inodes_.load(std::memory_order_relaxed) << "}";

  return oss.str();
}

void DirQuotaManager::Start() {
  if (running_.load()) {
    return;
  }

  DINGOFS_ERROR rc = DoLoadQuotas();
  if (rc != DINGOFS_ERROR::OK) {
    LOG(FATAL) << "QuotaManager load quota failed, rc = " << rc;
    return;
  }

  timer_->Add([this] { FlushQuotas(); },
              FLAGS_flush_quota_interval_second * 1000);
  timer_->Add([this] { LoadQuotas(); },
              FLAGS_load_quota_interval_second * 1000);

  running_.store(true);
}

void DirQuotaManager::Stop() {
  if (!running_.load()) {
    return;
  }

  DoFlushQuotas();

  running_.store(false);
}

void DirQuotaManager::UpdateDirQuotaUsage(Ino ino, int64_t new_space,
                                          int64_t new_inodes) {
  Ino inode = ino;
  while (true) {
    // NOTE: now we should not enable recyble
    if (inode == RECYCLEINODEID) {
      LOG(ERROR) << "UpdateDirUsage failed, ino = " << inode
                 << " is RECYCLEINODEID";
      return;
    }

    std::shared_ptr<DirQuota> dir_quota;
    auto rc = GetDirQuota(inode, dir_quota);

    if (rc == DINGOFS_ERROR::OK) {
      dir_quota->UpdateUsage(new_space, new_inodes);
    }

    if (inode == ROOTINODEID) {
      break;
    }

    // recursively update its parent
    Ino parent;
    rc = dir_parent_watcher_->GetParent(inode, parent);
    if (rc != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "UpdateDirUsage failed, ino = " << inode
                 << " get parent failed, rc = " << rc;
      return;
    }

    inode = parent;
  }
}

bool DirQuotaManager::CheckDirQuota(Ino ino, int64_t space, int64_t inodes) {
  Ino inode = ino;

  while (true) {
    // NOTE: now we should not enable recycle
    if (inode == RECYCLEINODEID) {
      LOG(ERROR) << "CheckDirQuota failed, inodeId=" << ino
                 << ", inodeId=" << inode << " is RECYCLEINODEID";
      return false;
    }

    std::shared_ptr<DirQuota> dir_quota;
    auto rc = GetDirQuota(inode, dir_quota);

    if (rc == DINGOFS_ERROR::OK) {
      if (!dir_quota->CheckQuota(space, inodes)) {
        LOG(WARNING) << "CheckDirQuota failed, inodeId=" << ino
                     << ", check fail becase quota inodeId=" << inode;
        return false;
      }
    }

    if (inode == ROOTINODEID) {
      break;
    }

    // if inode no quota or quota is enough, check its parent
    Ino parent;
    rc = dir_parent_watcher_->GetParent(inode, parent);
    if (rc != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "CheckDirQuota failed, inodeId=" << ino
                 << " because get inodeId=" << inode
                 << " parent failed, rc = " << rc;
      return false;
    }

    inode = parent;
  }

  return true;
}

DINGOFS_ERROR DirQuotaManager::GetDirQuota(
    Ino ino, std::shared_ptr<DirQuota>& dir_quota) {
  ReadLockGuard lk(rwock_);
  auto iter = quotas_.find(ino);
  if (iter == quotas_.end()) {
    VLOG(11) << "GetDirQuota failed, ino = " << ino << " not found in quotas_";
    return DINGOFS_ERROR::NOTEXIST;
  }

  dir_quota = iter->second;
  CHECK_NOTNULL(dir_quota);
  return DINGOFS_ERROR::OK;
}

void DirQuotaManager::FlushQuotas() {
  if (!running_.load()) {
    LOG(INFO) << "FlushQuotas is skipped, DirQuotaManager is not running";
    return;
  }

  DoFlushQuotas();

  timer_->Add([this] { FlushQuotas(); },
              FLAGS_flush_quota_interval_second * 1000);
}

void DirQuotaManager::DoFlushQuotas() {
  std::unordered_map<uint64_t, Usage> dir_usages;
  {
    ReadLockGuard lk(rwock_);
    for (const auto& [ino, dir_quota] : quotas_) {
      Usage usage = dir_quota->GetUsage();
      if (usage.bytes() == 0 && usage.inodes() == 0) {
        VLOG(3) << "DoFlushQuota skip ino: " << ino
                << " usage is 0, fs_id: " << fs_id_;
        continue;
      } else {
        dir_usages[ino] = usage;
      }
    }
  }

  if (dir_usages.empty()) {
    VLOG(3) << "DoFlushQuota is skipped dir_usages is empty, fs_id: " << fs_id_;
    return;
  }

  {
    // TODO: refact this, let metaserver reponse with new quota
    // Reader is flush and fuse, Writer is load, so this only influence load
    ReadLockGuard lk(rwock_);
    auto rc = meta_client_->FlushDirUsages(fs_id_, dir_usages);

    if (rc == MetaStatusCode::OK) {
      LOG(INFO) << "DoFlushQuotas success, fs_id: " << fs_id_;
      for (const auto& [ino, usage] : dir_usages) {
        auto iter = quotas_.find(ino);
        if (iter != quotas_.end()) {
          iter->second->FlushedUsage(usage.bytes(), usage.inodes());
        } else {
          LOG(INFO) << "FlushDirUsages success, but ino: " << ino
                    << " quota is removed  from quotas_";
        }
      }
    } else {
      LOG(WARNING) << "FlushFsUsage failed, fs_id: " << fs_id_
                   << ", rc: " << rc;
    }
  }
}

void DirQuotaManager::LoadQuotas() {
  if (!running_.load()) {
    LOG(INFO) << "LoadQuotas is skipped, DirQuotaManager is not running";
    return;
  }

  DoLoadQuotas();

  timer_->Add([this] { LoadQuotas(); },
              FLAGS_load_quota_interval_second * 1000);
}

DINGOFS_ERROR DirQuotaManager::DoLoadQuotas() {
  std::unordered_map<uint64_t, Quota> loaded_dir_quotas;
  auto rc = meta_client_->LoadDirQuotas(fs_id_, loaded_dir_quotas);
  if (rc != MetaStatusCode::OK) {
    LOG(ERROR) << "LoadDirQuotas failed, fs_id: " << fs_id_ << ", rc: " << rc;
    return DINGOFS_ERROR::INTERNAL;
  } else {
    LOG(INFO) << "DoLoadQuotas success, fs_id: " << fs_id_;
    WriteLockGuard lk(rwock_);
    // Remove quotas that are not in loaded_dir_quotas
    for (auto iter = quotas_.begin(); iter != quotas_.end();) {
      if (loaded_dir_quotas.find(iter->first) == loaded_dir_quotas.end()) {
        iter = quotas_.erase(iter);
      } else {
        ++iter;
      }
    }

    // Update or add quotas from loaded_dir_quotas
    for (const auto& [ino, quota] : loaded_dir_quotas) {
      auto iter = quotas_.find(ino);
      if (iter != quotas_.end()) {
        iter->second->Refresh(quota);
      } else {
        auto dir_quota = std::make_shared<DirQuota>(ino, quota);
        quotas_[ino] = dir_quota;
        VLOG(6) << "Add dir quota, new dir_quota: " << dir_quota->ToString();
      }
    }

    return DINGOFS_ERROR::OK;
  }
}

bool DirQuotaManager::NearestDirQuota(Ino ino, Ino& out_quota_ino) {
  Ino inode = ino;
  while (true) {
    // NOTE: now we should not enable recyble
    if (inode == RECYCLEINODEID) {
      LOG(ERROR) << "HasDirQuota failed, ino = " << inode
                 << " is RECYCLEINODEID";
      return false;
    }

    std::shared_ptr<DirQuota> dir_quota;
    auto rc = GetDirQuota(inode, dir_quota);

    if (rc == DINGOFS_ERROR::OK) {
      out_quota_ino = inode;
      return true;
    }

    if (inode == ROOTINODEID) {
      break;
    }

    Ino parent;
    rc = dir_parent_watcher_->GetParent(inode, parent);
    if (rc != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "HasDirQuota failed, ino = " << inode
                 << " get parent failed, rc = " << rc;
      return true;
    }

    inode = parent;
  }

  return false;
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs