/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-29
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEM_ENTRY_WATCHER_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEM_ENTRY_WATCHER_H_

#include <string>
#include <vector>

#include "client/vfs_old/filesystem/meta.h"
#include "utils/lru_cache.h"

namespace dingofs {
namespace client {
namespace filesystem {

// remeber regular file's ino which will use nocto flush plolicy
class EntryWatcher {
 public:
  using LRUType = utils::LRUCache<Ino, bool>;

  EntryWatcher(const std::string& nocto_suffix);

  void Remeber(const pb::metaserver::InodeAttr& attr,
               const std::string& filename);

  void Forget(Ino ino);

  bool ShouldWriteback(Ino ino);

 private:
  utils::RWLock rwlock_;
  std::unique_ptr<LRUType> nocto_;
  std::vector<std::string> suffixs_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEM_ENTRY_WATCHER_H_
