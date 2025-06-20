
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

#ifndef DINGOFS_SRC_FILE_SYSTEM_DIR_PARENT_WATCHER_H_
#define DINGOFS_SRC_FILE_SYSTEM_DIR_PARENT_WATCHER_H_

#include <unordered_map>

#include "client/vfs_legacy/filesystem/meta.h"
#include "client/vfs_legacy/inode_cache_manager.h"
#include "client/vfs_legacy/inode_wrapper.h"
#include "utils/concurrent/concurrent.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace filesystem {

class DirParentWatcher {
 public:
  DirParentWatcher() = default;

  virtual ~DirParentWatcher() = default;

  virtual void Remeber(Ino ino, Ino parent) = 0;

  virtual void Forget(Ino ino) = 0;

  virtual DINGOFS_ERROR GetParent(Ino ino, Ino& parent) = 0;
};

class DirParentWatcherImpl : public DirParentWatcher {
 public:
  DirParentWatcherImpl(std::shared_ptr<InodeCacheManager> inode_cache_manager)
      : inode_cache_manager_(std::move(inode_cache_manager)) {}

  ~DirParentWatcherImpl() override = default;

  void Remeber(Ino ino, Ino parent) override;

  void Forget(Ino ino) override;

  DINGOFS_ERROR GetParent(Ino ino, Ino& parent) override;

 private:
  std::shared_ptr<InodeCacheManager> inode_cache_manager_;
  utils::RWLock rwlock_;
  std::unordered_map<Ino, Ino> dir_parent_;
};

class DirParentWatcherGuard {
 public:
  DirParentWatcherGuard(std::shared_ptr<DirParentWatcher> wacther,
                        const pb::metaserver::InodeAttr& attr)
      : watcher_(std::move(wacther)), attr_(attr) {}

  ~DirParentWatcherGuard() {
    if (attr_.type() == pb::metaserver::FsFileType::TYPE_DIRECTORY) {
      CHECK_GT(attr_.parent_size(), 0);
      watcher_->Remeber(attr_.inodeid(), attr_.parent(0));
    }
  }

 private:
  std::shared_ptr<DirParentWatcher> watcher_;
  const pb::metaserver::InodeAttr& attr_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_FILE_SYSTEM_DIR_PARENT_WATCHER_H_