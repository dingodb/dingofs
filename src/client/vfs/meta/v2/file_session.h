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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_FILE_SESSION_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_FILE_SESSION_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "client/meta/vfs_meta.h"
#include "json/value.h"
#include "mdsv2/common/type.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class FileSession;
using FileSessionSPtr = std::shared_ptr<FileSession>;

class FileSession {
 public:
  struct Chunk;
  using ChunkSPtr = std::shared_ptr<Chunk>;

  struct Chunk {
    uint64_t index{0};
    uint64_t chunk_size{0};
    uint64_t block_size{0};

    std::vector<Slice> slices;

    uint64_t version{0};
    uint64_t last_compaction_time_ms{0};

    static ChunkSPtr From(const mdsv2::ChunkEntry& chunk);
    static mdsv2::ChunkEntry To(ChunkSPtr chunk);
  };

  FileSession() = default;
  ~FileSession() = default;

  static FileSessionSPtr New() { return std::make_shared<FileSession>(); }

  void SetSessionId(const std::string& session_id) { session_id_ = session_id; }
  const std::string& GetSessionID() const { return session_id_; }

  void AddChunk(ChunkSPtr chunk);
  void AppendSlice(int64_t index, const std::vector<Slice>& slices);

  ChunkSPtr GetChunk(int64_t index);
  std::vector<ChunkSPtr> GetAllChunk();

 private:
  std::string session_id_;

  utils::RWLock lock_;
  // index -> chunk
  std::map<int64_t, ChunkSPtr> chunk_map_;
};

// used by open file
class FileSessionMap {
 public:
  FileSessionMap() = default;
  ~FileSessionMap() = default;

  bool Put(uint64_t fh, FileSessionSPtr file_session);
  void Delete(uint64_t fh);

  std::string GetSessionID(uint64_t fh);
  FileSessionSPtr GetSession(uint64_t fh);

  // output json format string
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  utils::RWLock lock_;
  // fh -> FileSession
  std::map<uint64_t, FileSessionSPtr> file_session_map_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_FILE_SESSION_H_