/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CLIENT_VFS_DATA_READER_READER_REGISTRY_H_
#define DINGOFS_CLIENT_VFS_DATA_READER_READER_REGISTRY_H_

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

class FileReader;

// Non-owning per-inode index of open FileReaders. HandleResources remains the
// owner of each reader. Registry snapshots pin readers with their intrusive
// refcount and never call FileReader methods while holding mutex_.
class ReaderRegistry {
 public:
  void Register(FileReader* reader);
  void Unregister(FileReader* reader);

  void InvalidateByIno(Ino ino, int64_t offset, int64_t size);

  size_t Size() const;

 private:
  mutable std::mutex mutex_;
  std::unordered_map<Ino, std::unordered_set<FileReader*>> readers_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_READER_REGISTRY_H_
