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

#include "client/vfs_old/filesystem/dir_parent_watcher.h"

#include "dingofs/metaserver.pb.h"
#include "client/vfs_old/inode_wrapper.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace filesystem {

using utils::ReadLockGuard;
using utils::RWLock;
using utils::WriteLockGuard;

using pb::metaserver::InodeAttr;

void DirParentWatcherImpl::Remeber(Ino ino, Ino parent) {
  VLOG(3) << "DirParentWatcherImpl remeber ino: " << ino
          << " parent: " << parent;
  WriteLockGuard lk(rwlock_);
  dir_parent_[ino] = parent;
}

void DirParentWatcherImpl::Forget(Ino ino) {
  VLOG(3) << "DirParentWatcherImpl forget ino: " << ino;
  WriteLockGuard lk(rwlock_);
  dir_parent_.erase(ino);
}

DINGOFS_ERROR DirParentWatcherImpl::GetParent(Ino ino, Ino& parent) {
  {
    ReadLockGuard lk(rwlock_);

    auto it = dir_parent_.find(ino);
    if (it != dir_parent_.end()) {
      parent = it->second;
      return DINGOFS_ERROR::OK;
    }
  }

  // NOTE: which case will get here?
  LOG(INFO) << "DirParentWatcherImpl get parent from cache failed, ino: "
            << ino;

  InodeAttr attr;
  auto rc = inode_cache_manager_->GetInodeAttr(ino, &attr);
  if (rc != DINGOFS_ERROR::OK) {
    return rc;
  }

  if (attr.parent_size() == 0) {
    LOG(WARNING)
        << "Failed DirParentWatcherImpl from inode_cache_manager_, ino: " << ino
        << " attr: " << attr.ShortDebugString();
    return DINGOFS_ERROR::NOTEXIST;
  } else {
    parent = attr.parent(0);
    Remeber(ino, attr.parent(0));
  }

  return DINGOFS_ERROR::OK;
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs
