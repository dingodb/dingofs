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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MEMORY_FILESYSTEM_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MEMORY_FILESYSTEM_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "client/vfs/metasystem/meta_system.h"
#include "client/vfs/vfs_meta.h"
#include "common/trace/context.h"
#include "dingofs/mds.pb.h"
#include "json/value.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace memory {

// for ReadDir/ReadDirPlus
class ReadDirStateMemo {
 public:
  ReadDirStateMemo();
  ~ReadDirStateMemo();

  struct State {
    std::string last_name;
    bool is_end{false};
  };

  uint64_t NewState();
  bool GetState(uint64_t fh, State& state);
  void UpdateState(uint64_t fh, const State& state);
  void DeleteState(uint64_t fh);

 private:
  std::atomic<uint64_t> id_generator_{1000};
  uint64_t GenID() { return id_generator_.fetch_add(1); }

  bthread_mutex_t mutex_;
  // fh: last dir/file name
  std::map<uint64_t, State> state_map_;
};

// for open file
class OpenFileMemo {
 public:
  OpenFileMemo();
  ~OpenFileMemo();

  struct State {
    uint32_t ref_count{0};
  };

  bool IsOpened(uint64_t ino);
  void Open(uint64_t ino);
  void Close(uint64_t ino);

 private:
  bthread_mutex_t mutex_;
  // ino: open file
  std::map<uint64_t, State> file_map_;
};

// for store file data
class DataStorage {
 public:
  DataStorage();
  ~DataStorage();

  struct DataBuffer {
    std::string data;
  };
  using DataBufferPtr = std::shared_ptr<DataBuffer>;

  Status Read(uint64_t ino, off_t off, size_t size, char* buf, size_t& rsize);
  Status Write(uint64_t ino, off_t off, const char* buf, size_t size);

  bool GetLength(uint64_t ino, size_t& length);

 private:
  DataBufferPtr GetDataBuffer(uint64_t ino);

  bthread_mutex_t mutex_;
  std::map<uint64_t, DataBufferPtr> data_map_;
};

class FileChunkMap {
 public:
  FileChunkMap();
  ~FileChunkMap();

  Status NewSliceId(uint64_t* id);

  Status Read(uint64_t ino, uint64_t index, std::vector<Slice>* slices);
  Status Write(uint64_t ino, uint64_t index, const std::vector<Slice>& slices);

 private:
  struct Chunk {
    // chunk index -> slices
    std::map<uint64_t, std::vector<Slice>> slices;
  };

  bthread_mutex_t mutex_;

  std::atomic<uint64_t> slice_id_generator_{1000};

  // ino -> chunk
  std::map<uint64_t, Chunk> chunk_map_;
};

class MemoryMetaSystem;

class DirIterator;
using DirIteratorSPtr = std::shared_ptr<DirIterator>;

class DirIterator {
 public:
  DirIterator(MemoryMetaSystem* system, Ino ino)
      : dumy_system_(system), ino_(ino) {}

  ~DirIterator();

  static DirIteratorSPtr New(MemoryMetaSystem* system, Ino ino) {
    return std::make_shared<DirIterator>(system, ino);
  }

  Status Seek();

  bool Valid();

  DirEntry GetValue(bool with_attr);

  void Next();

  void SetDirEntries(std::vector<DirEntry>&& dir_entries);

 private:
  Ino ino_{0};
  uint64_t offset_{0};

  std::vector<DirEntry> dir_entries_;
  MemoryMetaSystem* dumy_system_{nullptr};
};

class DirIteratorManager {
 public:
  DirIteratorManager();
  ~DirIteratorManager();

  void Put(uint64_t fh, DirIteratorSPtr dir_iterator);
  DirIteratorSPtr Get(uint64_t fh);
  void Delete(uint64_t fh);

 private:
  bthread_mutex_t mutex_;
  // fh -> DirIteratorSPtr
  std::map<uint64_t, DirIteratorSPtr> dir_iterator_map_;
};

class MemoryMetaSystem : public vfs::MetaSystem {
 public:
  MemoryMetaSystem();
  ~MemoryMetaSystem() override;

  using PBInode = pb::mds::Inode;
  using PBDentry = pb::mds::Dentry;

  struct Dentry {
    PBDentry dentry;
    std::map<std::string, PBDentry> children;
  };

  struct ReadDirResult {
    PBDentry dentry;
    PBInode inode;
  };

  Status Init(bool upgrade) override;
  void UnInit(bool upgrade) override;

  bool Dump(ContextSPtr ctx, Json::Value& value) override;
  bool Dump(const DumpOption& options, Json::Value& value) override;
  bool Load(ContextSPtr ctx, const Json::Value& value) override;

  pb::mds::FsInfo GetFsInfo() { return fs_info_; }

  Status Lookup(ContextSPtr ctx, Ino parent, const std::string& name,
                Attr* attr) override;

  Status MkNod(ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, uint64_t rdev,
               Attr* attr) override;

  Status Open(ContextSPtr ctx, Ino ino, int flags, uint64_t fh) override;

  Status Create(ContextSPtr ctx, Ino parent, const std::string& name,
                uint32_t uid, uint32_t gid, uint32_t mode, int flags,
                Attr* attr, uint64_t fh) override;

  Status Close(ContextSPtr ctx, Ino ino, uint64_t fh) override;

  Status ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
                   std::vector<Slice>* slices, uint64_t& version) override;
  Status NewSliceId(ContextSPtr ctx, Ino ino, uint64_t* id) override;
  Status WriteSlice(ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
                    const std::vector<Slice>& slices) override;
  Status AsyncWriteSlice(ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
                         const std::vector<Slice>& slices,
                         DoneClosure done) override;
  Status Write(ContextSPtr ctx, Ino ino, uint64_t offset, uint64_t size,
               uint64_t fh) override;

  Status MkDir(ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, Attr* attr) override;

  Status RmDir(ContextSPtr ctx, Ino parent, const std::string& name) override;

  Status OpenDir(ContextSPtr ctx, Ino ino, uint64_t fh) override;

  Status ReadDir(ContextSPtr ctx, Ino ino, uint64_t fh, uint64_t offset,
                 bool with_attr, ReadDirHandler handler) override;

  Status ReleaseDir(ContextSPtr ctx, Ino ino, uint64_t fh) override;

  Status Link(ContextSPtr ctx, Ino ino, Ino new_parent,
              const std::string& new_name, Attr* attr) override;
  Status Unlink(ContextSPtr ctx, Ino parent, const std::string& name) override;
  Status Symlink(ContextSPtr ctx, Ino parent, const std::string& name,
                 uint32_t uid, uint32_t gid, const std::string& link,
                 Attr* att) override;
  Status ReadLink(ContextSPtr ctx, Ino ino, std::string* link) override;

  Status GetAttr(ContextSPtr ctx, Ino ino, Attr* attr) override;
  Status SetAttr(ContextSPtr ctx, Ino ino, int set, const Attr& in_attr,
                 Attr* out_attr) override;
  Status GetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                  std::string* value) override;
  Status SetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                  const std::string& value, int flags) override;
  Status RemoveXattr(ContextSPtr ctx, Ino ino,
                     const std::string& name) override;
  Status ListXattr(ContextSPtr ctx, Ino ino,
                   std::vector<std::string>* xattrs) override;

  Status Rename(ContextSPtr ctx, Ino old_parent, const std::string& old_name,
                Ino new_parent, const std::string& new_name) override;

  Status StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) override;

  Status GetFsInfo(ContextSPtr ctx, FsInfo* fs_info) override;

  bool GetDescription(Json::Value& value) override;

 private:
  friend class DirIterator;

  pb::mds::FsInfo fs_info_;

  std::atomic<uint64_t> ino_generator_{1000};
  uint64_t GenIno() { return ino_generator_++; }

  void AddDentry(const Dentry& dentry);
  void AddChildDentry(uint64_t parent_ino, const PBDentry& pb_dentry);

  void DeleteDentry(uint64_t parent_ino);
  void DeleteDentry(const std::string& name);
  void DeleteChildDentry(uint64_t parent_ino, const std::string& name);

  bool GetDentry(uint64_t parent_ino, Dentry& dentry);
  bool GetChildDentry(uint64_t parent_ino, const std::string& name,
                      PBDentry& dentry);
  bool GetAllChildDentry(uint64_t parent_ino,
                         std::vector<DirEntry>& dir_entries);

  bool IsEmptyDentry(const Dentry& dentry);

  void AddInode(const PBInode& inode);
  void DeleteInode(uint64_t ino);
  bool GetInode(uint64_t ino, PBInode& inode);

  void UpdateInode(const PBInode& inode,
                   const std::vector<std::string>& fields);
  void IncInodeNlink(uint64_t ino);
  void DecOrDeleteInodeNlink(uint64_t ino);
  void UpdateXAttr(uint64_t ino, const std::string& name,
                   const std::string& value);
  void RemoveXAttr(uint64_t ino, const std::string& name);
  void UpdateInodeLength(uint64_t ino, size_t length);

  bthread_mutex_t mutex_;
  // name: ino
  std::map<std::string, uint64_t> name_ino_map_;
  // parent_ino: dentry
  std::map<uint64_t, Dentry> dentry_map_;
  // ino: inode
  std::map<uint64_t, PBInode> inode_map_;

  // for read dir
  ReadDirStateMemo read_dir_state_memo_;

  // for open file
  OpenFileMemo open_file_memo_;

  DirIteratorManager dir_iterator_manager_;

  // for store file data
  DataStorage data_storage_;

  FileChunkMap file_chunk_map_;
};

}  // namespace memory
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MEMORY_FILESYSTEM_H_
