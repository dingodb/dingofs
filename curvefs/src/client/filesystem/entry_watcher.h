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

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_ENTRY_WATCHER_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_ENTRY_WATCHER_H_

#include <string>
#include <vector>

#include "curvefs/src/client/filesystem/meta.h"
#include "src/common/lru_cache.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curve::common::LRUCache;
using ::curve::common::RWLock;

// remeber regular file's ino which will use nocto flush plolicy
class EntryWatcher {
 public:
  using LRUType = LRUCache<Ino, bool>;

 public:
  EntryWatcher(const std::string& nocto_suffix);

  void Remeber(const InodeAttr& attr, const std::string& filename);

  void Forget(Ino ino);

  bool ShouldWriteback(Ino ino);

 private:
  RWLock rwlock_;
  std::unique_ptr<LRUType> nocto_;
  std::vector<std::string> suffixs_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_ENTRY_WATCHER_H_
