/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Curve
 * Created Date: 2023-03-09
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEM_ATTR_WATCHER_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEM_ATTR_WATCHER_H_

#include <memory>

#include "client/common/config.h"
#include "client/filesystem/dir_cache.h"
#include "client/filesystem/meta.h"
#include "client/filesystem/openfile.h"
#include "utils/lru_cache.h"

namespace dingofs {
namespace client {
namespace filesystem {

class AttrWatcher {
 public:
  using LRUType = utils::LRUCache<Ino, base::time::TimeSpec>;

  AttrWatcher(common::AttrWatcherOption option,
              std::shared_ptr<OpenFiles> openFiles,
              std::shared_ptr<DirCache> dirCache);

  void RemeberMtime(const pb::metaserver::InodeAttr& attr);

  bool GetMtime(Ino ino, base::time::TimeSpec* time);

  void UpdateDirEntryAttr(Ino ino, const pb::metaserver::InodeAttr& attr);

  void UpdateDirEntryLength(Ino ino, const pb::metaserver::InodeAttr& open);

 private:
  friend class AttrWatcherGuard;

  utils::RWLock rwlock_;
  std::shared_ptr<LRUType> modifiedAt_;
  std::shared_ptr<OpenFiles> openFiles_;
  std::shared_ptr<DirCache> dirCache_;
};

enum class ReplyType { ATTR, ONLY_LENGTH };

/*
 * each attribute reply to kernel, the watcher will:
 *  before reply:
 *    1) set attibute length if the corresponding file is opened
 *  after reply:
 *    1) remeber attribute modified time.
 *    2) write back attribute to dir entry cache if |writeBack| is true,
 *       because the dir-entry attribute maybe stale.
 */
struct AttrWatcherGuard {
 public:
  AttrWatcherGuard(std::shared_ptr<AttrWatcher> watcher,
                   pb::metaserver::InodeAttr* attr, ReplyType type,
                   bool writeBack);

  ~AttrWatcherGuard();

 private:
  std::shared_ptr<AttrWatcher> watcher;
  pb::metaserver::InodeAttr* attr;
  ReplyType type;
  bool writeBack;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEM_ATTR_WATCHER_H_
