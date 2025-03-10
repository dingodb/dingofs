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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_DUMMY_FILESYSTEM_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_DUMMY_FILESYSTEM_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "client/vfs/dir_iterator.h"
#include "client/vfs/meta/meta_system.h"
#include "client/vfs/vfs_meta.h"
#include "dingofs/mdsv2.pb.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace dummy {

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

class DummyFileSystem;

class DummyDirIterator : public vfs::DirIterator {
 public:
  DummyDirIterator(DummyFileSystem* system, Ino ino)
      : dumy_system_(system), ino_(ino) {}

  ~DummyDirIterator() override;

  Status Seek() override;

  bool Valid() override;

  DirEntry GetValue(bool with_attr) override;

  void Next() override;

  void SetDirEntries(std::vector<DirEntry>&& dir_entries);

 private:
  Ino ino_{0};
  uint64_t offset_{0};

  std::vector<DirEntry> dir_entries_;
  DummyFileSystem* dumy_system_{nullptr};
};

class DummyFileSystem : public vfs::MetaSystem {
 public:
  DummyFileSystem();
  ~DummyFileSystem() override;

  using PBInode = pb::mdsv2::Inode;
  using PBDentry = pb::mdsv2::Dentry;

  struct Dentry {
    PBDentry dentry;
    std::map<std::string, PBDentry> children;
  };

  struct ReadDirResult {
    PBDentry dentry;
    PBInode inode;
  };

  Status Init() override;
  void UnInit() override;

  pb::mdsv2::FsInfo GetFsInfo() { return fs_info_; }

  Status Lookup(Ino parent, const std::string& name, Attr* attr) override;

  Status MkNod(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, uint64_t rdev, Attr* attr) override;

  Status Open(Ino ino, int flags) override;

  Status Create(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
                uint32_t mode, int flags, Attr* attr) override;

  Status Close(Ino ino) override;

  Status ReadSlice(Ino ino, uint64_t index,
                   std::vector<Slice>* slices) override;
  Status NewSliceId(uint64_t* id) override;
  Status WriteSlice(Ino ino, uint64_t index,
                    const std::vector<Slice>& slices) override;

  Status MkDir(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, Attr* attr) override;

  Status RmDir(Ino parent, const std::string& name) override;

  Status OpenDir(Ino ino) override;

  // NOTE: caller own dir and the DirHandler should be deleted by caller
  DirIterator* NewDirIterator(Ino ino) override;

  Status Link(Ino ino, Ino new_parent, const std::string& new_name,
              Attr* attr) override;
  Status Unlink(Ino parent, const std::string& name) override;
  Status Symlink(Ino parent, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& link, Attr* att) override;
  Status ReadLink(Ino ino, std::string* link) override;

  Status GetAttr(Ino ino, Attr* attr) override;
  Status SetAttr(Ino ino, int set, const Attr& in_attr,
                 Attr* out_attr) override;
  Status GetXattr(Ino ino, const std::string& name,
                  std::string* value) override;
  Status SetXattr(Ino ino, const std::string& name, const std::string& value,
                  int flags) override;
  Status ListXattr(Ino ino, std::vector<std::string>* xattrs) override;

  Status Rename(Ino old_parent, const std::string& old_name, Ino new_parent,
                const std::string& new_name) override;

  Status StatFs(Ino ino, FsStat* fs_stat) override;

 private:
  friend class DummyDirIterator;

  pb::mdsv2::FsInfo fs_info_;

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

  // for store file data
  DataStorage data_storage_;

  FileChunkMap file_chunk_map_;
};

}  // namespace dummy
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_DUMMY_FILESYSTEM_H_
