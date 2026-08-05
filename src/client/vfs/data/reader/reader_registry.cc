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

#include "client/vfs/data/reader/reader_registry.h"

#include <glog/logging.h>

#include <vector>

#include "client/vfs/data/reader/file_reader.h"

namespace dingofs {
namespace client {
namespace vfs {

void ReaderRegistry::Register(FileReader* reader) {
  CHECK_NOTNULL(reader);
  const Ino ino = reader->GetIno();
  std::lock_guard<std::mutex> lock(mutex_);
  CHECK(readers_[ino].insert(reader).second)
      << "FileReader registered more than once, ino: " << ino;
}

void ReaderRegistry::Unregister(FileReader* reader) {
  CHECK_NOTNULL(reader);
  const Ino ino = reader->GetIno();
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = readers_.find(ino);
  CHECK(it != readers_.end()) << "FileReader inode is not registered: " << ino;
  CHECK_EQ(it->second.erase(reader), 1)
      << "FileReader is not registered for inode: " << ino;
  if (it->second.empty()) {
    readers_.erase(it);
  }
}

void ReaderRegistry::InvalidateByIno(Ino ino, int64_t offset, int64_t size) {
  std::vector<FileReader*> readers;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = readers_.find(ino);
    if (it == readers_.end()) {
      return;
    }
    readers.reserve(it->second.size());
    for (auto* reader : it->second) {
      reader->AcquireRef();
      readers.push_back(reader);
    }
  }

  for (auto* reader : readers) {
    reader->Invalidate(offset, size);
    reader->ReleaseRef();
  }
}

size_t ReaderRegistry::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t size = 0;
  for (const auto& entry : readers_) {
    size += entry.second.size();
  }
  return size;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
