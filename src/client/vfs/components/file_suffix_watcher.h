/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CLIENT_VFS_COMPONENTS_FILE_SUFFIX_WATCHER_H_
#define DINGOFS_CLIENT_VFS_COMPONENTS_FILE_SUFFIX_WATCHER_H_

#include <shared_mutex>
#include <unordered_set>

#include "client/meta/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

class FileSuffixWatcher {
 public:
  FileSuffixWatcher(const std::string& writeback_suffix);

  void Remeber(const Attr& attr, const std::string& name);

  void Forget(Ino ino);

  bool ShouldWriteback(Ino ino);

 private:
  std::shared_mutex rw_lock_;
  std::unordered_set<Ino> writeback_ino_;
  std::vector<std::string> suffixs_;
};
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_COMPONENTS_FILE_SUFFIX_WATCHER_H_