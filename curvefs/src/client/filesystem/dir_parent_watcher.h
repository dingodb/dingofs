
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

#ifndef CURVEFS_SRC_FILE_SYSTEM_DIR_PARENT_WATCHER_H_
#define CURVEFS_SRC_FILE_SYSTEM_DIR_PARENT_WATCHER_H_

#include <unordered_map>

#include "curvefs/src/client/filesystem/meta.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "glog/logging.h"
#include "src/common/concurrent/concurrent.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curve::common::RWLock;

class DirParentWatcher {
 public:
  DirParentWatcher() = default;

  virtual ~DirParentWatcher() = default;

  virtual void Remeber(Ino ino, Ino parent) = 0;

  virtual void Forget(Ino ino) = 0;

  virtual CURVEFS_ERROR GetParent(Ino ino, Ino& parent) = 0;
};

class DirParentWatcherImpl : public DirParentWatcher {
 public:
  DirParentWatcherImpl(std::shared_ptr<InodeCacheManager> inode_cache_manager)
      : inode_cache_manager_(std::move(inode_cache_manager)) {}

  ~DirParentWatcherImpl() override = default;

  void Remeber(Ino ino, Ino parent) override;

  void Forget(Ino ino) override;

  CURVEFS_ERROR GetParent(Ino ino, Ino& parent) override;

 private:
  std::shared_ptr<InodeCacheManager> inode_cache_manager_;
  RWLock rwlock_;
  std::unordered_map<Ino, Ino> dir_parent_;
};

class DirParentWatcherGuard {
 public:
  DirParentWatcherGuard(std::shared_ptr<DirParentWatcher> wacther,
                        const InodeAttr& attr)
      : watcher_(std::move(wacther)), attr_(attr) {}

  ~DirParentWatcherGuard() {
    if (attr_.type() == FsFileType::TYPE_DIRECTORY) {
      CHECK_GT(attr_.parent_size(), 0);
      watcher_->Remeber(attr_.inodeid(), attr_.parent(0));
    }
  }

 private:
  std::shared_ptr<DirParentWatcher> watcher_;
  const InodeAttr& attr_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_FILE_SYSTEM_DIR_PARENT_WATCHER_H_