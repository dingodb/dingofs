// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_FILE_SESSION_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_FILE_SESSION_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "client/vfs/metasystem/mds/chunk.h"
#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "utils/concurrent/concurrent.h"
#include "utils/shards.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

class FileSession;
using FileSessionSPtr = std::shared_ptr<FileSession>;

class FileSessionMap;

class FileSession {
 public:
  FileSession(Ino ino, ChunkSetSPtr& chunk_set)
      : ino_(ino), chunk_set_(chunk_set) {}
  ~FileSession() = default;

  static FileSessionSPtr New(Ino ino, ChunkSetSPtr chunk_set) {
    return std::make_shared<FileSession>(ino, chunk_set);
  }

  Ino GetIno() const { return ino_; }
  std::string GetSessionID(uint64_t fh);

  ChunkSetSPtr& GetChunkSet() { return chunk_set_; }

  void AddSession(uint64_t fh, const std::string& session_id);
  uint32_t DeleteSession(uint64_t fh);

  // output json format string
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  friend class FileSessionMap;

  uint32_t IncRef() { return ref_count_.fetch_add(1) + 1; }
  uint32_t DecRef() { return ref_count_.fetch_sub(1) - 1; }

  Ino ino_;

  std::atomic<uint32_t> ref_count_{0};

  utils::RWLock lock_;

  // fh -> session_id
  absl::flat_hash_map<uint64_t, std::string> session_id_map_;

  ChunkSetSPtr chunk_set_;
};

// used by open file
class FileSessionMap {
 public:
  FileSessionMap(ChunkCache& chunk_cache) : chunk_cache_(chunk_cache) {}
  ~FileSessionMap() = default;

  FileSessionSPtr Put(Ino ino, uint64_t fh, const std::string& session_id);
  void Delete(Ino ino, uint64_t fh);

  std::string GetSessionID(Ino ino, uint64_t fh);
  FileSessionSPtr GetSession(Ino ino);
  std::vector<FileSessionSPtr> GetAllSession();

  // output json format string
  bool Dump(Ino ino, Json::Value& value);
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  void Put(FileSessionSPtr);

  ChunkCache& chunk_cache_;

  using Map = absl::btree_map<Ino, FileSessionSPtr>;

  constexpr static size_t kShardNum = 32;
  utils::Shards<Map, kShardNum> shard_map_;
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_FILE_SESSION_H_