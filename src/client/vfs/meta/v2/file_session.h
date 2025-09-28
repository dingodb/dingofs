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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "client/meta/vfs_meta.h"
#include "json/value.h"
#include "mds/common/type.h"
#include "mds/filesystem/fs_info.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

struct ChunkMutation;
using ChunkMutationSPtr = std::shared_ptr<ChunkMutation>;

class FileSession;
using FileSessionSPtr = std::shared_ptr<FileSession>;

class FileSessionMap;

using mds::ChunkEntry;

class ChunkMutation {
 public:
  ChunkMutation() = default;
  ChunkMutation(Ino ino, uint64_t index, uint64_t chunk_size,
                uint64_t block_size)
      : ino_(ino),
        index_(index),
        chunk_size_(chunk_size),
        block_size_(block_size) {}
  ChunkMutation(Ino ino, const ChunkEntry& chunk) : ino_(ino) {
    UpdateChunkIf(chunk);
  }

  static ChunkMutationSPtr New(Ino ino, uint64_t index, uint64_t chunk_size,
                               uint64_t block_size) {
    return std::make_shared<ChunkMutation>(ino, index, chunk_size, block_size);
  }
  static ChunkMutationSPtr New(Ino ino, const ChunkEntry& chunk) {
    return std::make_shared<ChunkMutation>(ino, chunk);
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
  friend class FileSession;

  static ChunkMutationSPtr New() { return std::make_shared<ChunkMutation>(); }

  utils::RWLock lock_;

  Ino ino_;
  uint64_t index_{0};
  uint64_t chunk_size_{0};
  uint64_t block_size_{0};

  std::vector<Slice> slices_;

  uint64_t version_{0};
  uint64_t last_compaction_time_ms_{0};

  std::vector<Slice> delta_slices_;
};

class WriteMemo {
 public:
  WriteMemo() = default;
  ~WriteMemo() = default;

  void AddRange(uint64_t offset, uint64_t size);
  uint64_t GetLength();
  uint64_t LastTimeNs() const { return last_time_ns_; }

  // output json format string
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  struct Range {
    uint64_t start{0};
    uint64_t end{0};  // [start, end)
  };

  std::vector<Range> ranges_;
  uint64_t last_time_ns_{0};
};

class FileSession {
 public:
  FileSession(mds::FsInfoSPtr fs_info) : fs_info_(fs_info) {}
  FileSession(mds::FsInfoSPtr fs_info, Ino ino, uint64_t fh,
              const std::string& session_id);
  ~FileSession() = default;

  static FileSessionSPtr New(mds::FsInfoSPtr fs_info, Ino ino, uint64_t fh,
                             const std::string& session_id) {
    return std::make_shared<FileSession>(fs_info, ino, fh, session_id);
  }

  Ino GetIno() const { return ino_; }
  std::string GetSessionID(uint64_t fh);

  uint32_t IncRef() { return ref_count_.fetch_add(1) + 1; }
  uint32_t DecRef() { return ref_count_.fetch_sub(1) - 1; }

  void AddSession(uint64_t fh, const std::string& session_id);
  void DeleteSession(uint64_t fh);

  void AddWriteMemo(uint64_t offset, uint64_t size);
  uint64_t GetLength();
  uint64_t GetLastTimeNs();

  void UpsertChunk(uint64_t fh, const std::vector<ChunkEntry>& chunks);
  void DeleteChunkMutation(int64_t index);

  ChunkMutationSPtr GetChunkMutation(int64_t index);

  // output json format string
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  friend class FileSessionMap;

  static FileSessionSPtr New(mds::FsInfoSPtr fs_info) {
    return std::make_shared<FileSession>(fs_info);
  }

  mds::FsInfoSPtr fs_info_;
  Ino ino_;

  std::atomic<uint32_t> ref_count_{0};

  utils::RWLock lock_;

  // fh -> session_id
  std::map<uint64_t, std::string> session_id_map_;

  WriteMemo write_memo_;

  // index -> chunk
  std::map<int64_t, ChunkMutationSPtr> chunk_mutation_map_;
};

// used by open file
class FileSessionMap {
 public:
  FileSessionMap(mds::FsInfoSPtr fs_info) : fs_info_(fs_info) {}
  ~FileSessionMap() = default;

  FileSessionSPtr Put(Ino ino, uint64_t fh, const std::string& session_id);
  void Delete(Ino ino, uint64_t fh);

  std::string GetSessionID(Ino ino, uint64_t fh);
  FileSessionSPtr GetSession(Ino ino);

  // output json format string
  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  mds::FsInfoSPtr fs_info_;

  utils::RWLock lock_;
  // ino -> FileSession
  std::map<Ino, FileSessionSPtr> file_session_map_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_FILE_SESSION_H_