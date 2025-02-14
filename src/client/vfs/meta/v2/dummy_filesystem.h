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

#include "client/filesystem/error.h"
#include "client/filesystem/meta.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"

namespace dingofs {
namespace client {
namespace filesystem {

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

class DummyFileSystem {
 public:
  DummyFileSystem();
  ~DummyFileSystem();

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

  using ReadDirHandler = std::function<bool(const std::string&, uint64_t)>;
  using ReadDirPlusHandler =
      std::function<bool(const std::string&, const PBInode&)>;

  bool Init();
  void UnInit();

  pb::mdsv2::FsInfo GetFsInfo() { return fs_info_; }

  Status Lookup(uint64_t parent_ino, const std::string& name,
                EntryOut& entry_out);

  Status MkNod(uint64_t parent_ino, const std::string& name, uint32_t uid,
               uint32_t gid, mode_t mode, dev_t rdev, EntryOut& entry_out);
  Status Open(uint64_t ino);
  Status Release(uint64_t ino);
  Status Read(uint64_t ino, off_t off, size_t size, char* buf, size_t& rsize);
  Status Write(uint64_t ino, off_t off, const char* buf, size_t size,
               size_t& wsize);
  Status Flush(uint64_t ino);
  Status Fsync(uint64_t ino, int data_sync);

  Status MkDir(uint64_t parent_ino, const std::string& name, uint32_t uid,
               uint32_t gid, mode_t mode, dev_t rdev, EntryOut& entry_out);
  Status RmDir(uint64_t parent_ino, const std::string& name);
  Status OpenDir(uint64_t ino, uint64_t& fh);
  Status ReadDir(uint64_t fh, uint64_t ino, ReadDirHandler handler);
  Status ReadDirPlus(uint64_t fh, uint64_t ino, ReadDirPlusHandler handler);
  Status ReleaseDir(uint64_t ino, uint64_t fh);

  Status Link(uint64_t ino, uint64_t new_parent_ino,
              const std::string& new_name, EntryOut& entry_out);
  Status UnLink(uint64_t parent_ino, const std::string& name);
  Status Symlink(uint64_t parent_ino, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& symlink, EntryOut& entry_out);
  Status ReadLink(uint64_t ino, std::string& symlink);

  Status GetAttr(uint64_t ino, AttrOut& attr_out);
  Status SetAttr(uint64_t ino, struct stat* attr, int to_set,
                 AttrOut& attr_out);
  Status GetXAttr(uint64_t ino, const std::string& name, std::string& value);
  Status SetXAttr(uint64_t ino, const std::string& name,
                  const std::string& value);
  Status ListXAttr(uint64_t ino, size_t size, std::string& out_names);

  Status Rename(uint64_t parent_ino, const std::string& name,
                uint64_t new_parent_ino, const std::string& new_name);

 private:
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
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_DUMMY_FILESYSTEM_H_
