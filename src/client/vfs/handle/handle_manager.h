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

#ifndef DINGOFS_CLIENT_VFS_HANDLE_MANAGER_H
#define DINGOFS_CLIENT_VFS_HANDLE_MANAGER_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "client/vfs/data/file.h"
#include "client/vfs/handle/dir_iterator.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

struct Handle {
  Ino ino;
  uint64_t fh;
  // file ralted
  int32_t flags;
  std::unique_ptr<File> file;

  // dir related
  std::unique_ptr<DirIterator> dir_iterator;
};

using HandlePtr = std::shared_ptr<Handle>;

class HandleManager {
 public:
  HandleManager() = default;
  ~HandleManager() = default;

  HandlePtr NewHandle();

  HandlePtr FindHandler(uint64_t fh);

  void ReleaseHandler(uint64_t fh);

 private:
  std::mutex mutex_;
  uint64_t next_fh_{1};
  std::unordered_map<uint64_t, HandlePtr> handles_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_HANDLE_MANAGER_H
