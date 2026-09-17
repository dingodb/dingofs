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

#include <array>
#include <cstddef>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

class FileReader;
class ReaderRegistryShard;

// Non-owning per-inode index of open FileReaders. HandleResources remains the
// owner of each reader. Snapshots pin readers under their inode's shard lock;
// Invalidate and pin release run outside that lock. The owner drains callers
// before destroying the registry.
// One inode partition: a non-owning reader index with its own lock. The
// registry owns routing; the reader's owner keeps it alive from before
// Register until after Unregister, while the shard owns snapshot pins.
class alignas(64) ReaderRegistryShard {
 public:
  void Register(FileReader* reader);
  void Unregister(FileReader* reader);
  // Every returned reader owns one pin. Invalidate and ReleaseRef happen
  // after this function releases the lock, including after removal.
  std::vector<FileReader*> Snapshot(Ino ino);
  // Background single-shard snapshot across all inodes of this shard: every
  // returned reader owns one reference pin taken under the shard lock. The
  // caller releases refs outside any registry lock. Closed readers may
  // appear; their maintenance entry re-checks the closing flag.
  std::vector<FileReader*> SnapshotAll();
  size_t Size() const;

 private:
  mutable std::mutex mutex_;
  size_t reader_count_{0};
  std::unordered_map<Ino, std::unordered_set<FileReader*>> readers_;
};

class ReaderRegistry {
 public:
  // Shard count for background single-shard scans (see SnapshotShard).
  static constexpr size_t kShardCount = 64;

  void Register(FileReader* reader);
  void Unregister(FileReader* reader);

  void InvalidateByIno(Ino ino, int64_t offset, int64_t size);

  // Background single-shard snapshot (shard_index < kShardCount): pins every
  // reader of that shard with a reference under the shard's lock only. The
  // caller owns releasing the refs outside any registry lock. A round is not
  // a consistent whole-table snapshot; readers registered after the shard was
  // visited are seen on the next round.
  std::vector<FileReader*> SnapshotShard(size_t shard_index);

  // Best-effort number of registered readers, not inode entries.
  size_t Size() const;

 private:
  ReaderRegistryShard& GetShard(Ino ino);

  std::array<ReaderRegistryShard, kShardCount> shards_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_READER_REGISTRY_H_
