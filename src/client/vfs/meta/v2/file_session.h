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
#include "mdsv2/filesystem/fs_info.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

struct ChunkMutation;
using ChunkMutationSPtr = std::shared_ptr<ChunkMutation>;

using mdsv2::ChunkEntry;

class ChunkMutation {
 public:
  ChunkMutation(Ino ino, uint64_t fh, uint64_t index, uint64_t chunk_size,
                uint64_t block_size)
      : ino_(ino),
        fh_(fh),
        index_(index),
        chunk_size_(chunk_size),
        block_size_(block_size) {}
  ChunkMutation(Ino ino, uint64_t fh, const ChunkEntry& chunk)
      : ino_(ino), fh_(fh) {
    UpdateChunkIf(chunk);
  }

  static ChunkMutationSPtr New(Ino ino, uint64_t fh, uint64_t index,
                               uint64_t chunk_size, uint64_t block_size) {
    return std::make_shared<ChunkMutation>(ino, fh, index, chunk_size,
                                           block_size);
  }
  static ChunkMutationSPtr New(Ino ino, uint64_t fh, const ChunkEntry& chunk) {
    return std::make_shared<ChunkMutation>(ino, fh, chunk);
  }

  int64_t GetIndex() const { return index_; }
  bool HasChunk() const { return version_ > 0; }
  ChunkEntry GetChunkEntry() const;

  void UpdateChunkIf(const ChunkEntry& chunk);

  std::vector<Slice> GetAllSlice();
  std::vector<Slice> GetDeltaSlice();
  void AppendSlice(const std::vector<Slice>& slices);
  void DeleteDeltaSlice(const std::vector<uint64_t>& delete_slice_ids);

  // output json format string
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  utils::RWLock lock_;

  Ino ino_;
  uint64_t fh_{0};
  uint64_t index_{0};
  uint64_t chunk_size_{0};
  uint64_t block_size_{0};

  std::vector<Slice> slices_;

  uint64_t version_{0};
  uint64_t last_compaction_time_ms_{0};

  std::vector<Slice> delta_slices_;
};

class FileSession;
using FileSessionSPtr = std::shared_ptr<FileSession>;

class FileSession {
 public:
  FileSession(Ino ino, uint64_t fh, const std::string& session_id,
              mdsv2::FsInfoPtr fs_info, const std::vector<ChunkEntry>& chunks);
  ~FileSession() = default;

  static FileSessionSPtr New(Ino ino, uint64_t fh,
                             const std::string& session_id,
                             mdsv2::FsInfoPtr fs_info,
                             const std::vector<ChunkEntry>& chunks) {
    return std::make_shared<FileSession>(ino, fh, session_id, fs_info, chunks);
  }

  Ino GetIno() const { return ino_; }
  uint64_t GetFh() const { return fh_; }
  const std::string& GetSessionID() const { return session_id_; }

  void UpsertChunkMutation(const ChunkEntry& chunk);
  void AppendSlice(int64_t index, const std::vector<Slice>& slices);
  void DeleteChunkMutation(int64_t index);

  ChunkMutationSPtr GetChunkMutation(int64_t index);
  std::vector<ChunkMutationSPtr> GetAllChunkMutation();

 private:
  friend class FileSessionMap;
  void AddChunkMutation(ChunkMutationSPtr chunk_mutation);

  Ino ino_;
  uint64_t fh_;
  const std::string session_id_;

  mdsv2::FsInfoPtr fs_info_;

  utils::RWLock lock_;

  // index -> chunk
  std::map<int64_t, ChunkMutationSPtr> chunk_mutation_map_;
};

// used by open file
class FileSessionMap {
 public:
  FileSessionMap(mdsv2::FsInfoPtr fs_info) : fs_info_(fs_info) {}
  ~FileSessionMap() = default;

  struct Key {
    uint64_t ino{0};
    uint64_t fh{0};

    bool operator<(const Key& other) const {
      if (ino != other.ino) {
        return ino < other.ino;
      }
      return fh < other.fh;
    }
  };

  bool Put(const Key& key, FileSessionSPtr file_session);
  void Delete(const Key& key);

  std::string GetSessionID(const Key& key);
  FileSessionSPtr GetSession(const Key& key);

  // output json format string
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  mdsv2::FsInfoPtr fs_info_;

  utils::RWLock lock_;
  // Key(ino, fh) -> FileSession
  std::map<Key, FileSessionSPtr> file_session_map_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_FILE_SESSION_H_