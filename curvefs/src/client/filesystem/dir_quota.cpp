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

#include "curvefs/src/client/filesystem/dir_quota.h"

#include <atomic>
#include <cstdint>
#include <sstream>

namespace curvefs {
namespace client {
namespace filesystem {

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

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

bool DirQuota::CheckQuota(int64_t new_space, int64_t new_inodes) {
  Quota quota = GetQuota();

  VLOG(6) << "CheckQuota dir inodeId=" << ino_ << " new_space:" << new_space
          << ", new_inodes:" << new_inodes << ", dir_quota:" << ToString();

  if (quota.maxbytes() > 0 &&
      (new_space + new_space_.load(std::memory_order_relaxed) +
       quota.usedbytes()) > (int64_t)quota.maxbytes()) {
    LOG(INFO) << "CheckQuota check space failed, new_space:" << new_space
              << ", quota:" << ToString();
    return false;
  }

  if (quota.maxinodes() > 0 &&
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

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs