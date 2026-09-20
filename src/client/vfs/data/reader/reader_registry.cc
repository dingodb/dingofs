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

#include "absl/hash/hash.h"
#include "client/vfs/data/reader/file_reader.h"
#include "common/sync_point.h"

namespace dingofs {
namespace client {
namespace vfs {

ReaderRegistryShard& ReaderRegistry::GetShard(Ino ino) {
  return shards_[absl::HashOf(ino) & (kShardCount - 1)];
}

void ReaderRegistry::Register(FileReader* reader) {
  CHECK_NOTNULL(reader);
  GetShard(reader->GetIno()).Register(reader);
}

void ReaderRegistry::Unregister(FileReader* reader) {
  CHECK_NOTNULL(reader);
  GetShard(reader->GetIno()).Unregister(reader);
}

void ReaderRegistry::InvalidateByIno(Ino ino, int64_t offset, int64_t size) {
  auto readers = GetShard(ino).Snapshot(ino);
  TEST_SYNC_POINT_CALLBACK("ReaderRegistry::InvalidateByIno:after_snapshot",
                           this);

  for (auto* reader : readers) {
    reader->Invalidate(offset, size);
    reader->ReleaseRef();
  }
}

std::vector<FileReader*> ReaderRegistry::SnapshotShard(size_t shard_index) {
  CHECK_LT(shard_index, kShardCount);
  return shards_[shard_index].SnapshotAll();
}

size_t ReaderRegistry::Size() const {
  size_t size = 0;
  for (const auto& shard : shards_) {
    size += shard.Size();
  }
  return size;
}

void ReaderRegistryShard::Register(FileReader* reader) {
  const Ino ino = reader->GetIno();
  std::lock_guard<std::mutex> lock(mutex_);
  CHECK(readers_[ino].insert(reader).second)
      << "FileReader registered more than once, ino: " << ino;
  ++reader_count_;
}

void ReaderRegistryShard::Unregister(FileReader* reader) {
  const Ino ino = reader->GetIno();
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = readers_.find(ino);
  CHECK(it != readers_.end()) << "FileReader inode is not registered: " << ino;
  CHECK_EQ(it->second.erase(reader), 1)
      << "FileReader is not registered for inode: " << ino;
  --reader_count_;
  if (it->second.empty()) readers_.erase(it);
}

std::vector<FileReader*> ReaderRegistryShard::Snapshot(Ino ino) {
  std::vector<FileReader*> readers;
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = readers_.find(ino);
  if (it == readers_.end()) return readers;
  readers.reserve(it->second.size());
  for (auto* reader : it->second) {
    reader->AcquireRef();
    readers.push_back(reader);
  }
  return readers;
}

std::vector<FileReader*> ReaderRegistryShard::SnapshotAll() {
  std::vector<FileReader*> readers;
  std::lock_guard<std::mutex> lock(mutex_);
  readers.reserve(reader_count_);
  for (const auto& [ino, reader_set] : readers_) {
    for (FileReader* reader : reader_set) {
      reader->AcquireRef();
      readers.push_back(reader);
    }
  }
  return readers;
}

size_t ReaderRegistryShard::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return reader_count_;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
