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

#ifndef DINGOFS_MDV2_FILESYSTEM_H_
#define DINGOFS_MDV2_FILESYSTEM_H_

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/file.h"
#include "mdsv2/filesystem/id_generator.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/storage/storage.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {

namespace mdsv2 {

class FileSystem;
using FileSystemPtr = std::shared_ptr<FileSystem>;

class FileSystemSet;
using FileSystemSetPtr = std::shared_ptr<FileSystemSet>;

struct EntryOut {
  EntryOut() = default;

  explicit EntryOut(const pb::mdsv2::Inode& inode) : inode(inode) {}

  std::string name;
  pb::mdsv2::Inode inode;
};

class FileSystem {
 public:
  FileSystem(const pb::mdsv2::FsInfo& fs_info, IdGeneratorPtr id_generator, KVStoragePtr kv_storage)
      : fs_info_(fs_info), id_generator_(std::move(id_generator)), kv_storage_(kv_storage){};
  ~FileSystem() = default;

  static FileSystemPtr New(const pb::mdsv2::FsInfo& fs_info, IdGeneratorPtr id_generator, KVStoragePtr kv_storage) {
    return std::make_shared<FileSystem>(fs_info, std::move(id_generator), kv_storage);
  }

  uint32_t FsId() const { return fs_info_.fs_id(); }
  std::string FsName() const { return fs_info_.fs_name(); }
  pb::mdsv2::FsInfo FsInfo() const { return fs_info_; }

  // create root directory
  Status CreateRoot();

  // lookup dentry
  Status Lookup(uint64_t parent_ino, const std::string& name, EntryOut& entry_out);

  // file
  struct MkNodParam {
    std::string name;
    uint32_t flag{0};
    uint32_t uid{0};
    uint32_t gid{0};
    uint32_t mode{0};
    uint64_t parent_ino{0};
    uint64_t rdev{0};
  };
  Status MkNod(const MkNodParam& param, EntryOut& entry_out);
  Status Open(uint64_t ino);
  Status Release(uint64_t ino);

  // directory
  struct MkDirParam {
    std::string name;
    uint32_t flag{0};
    uint32_t uid{0};
    uint32_t gid{0};
    uint32_t mode{0};
    uint64_t parent_ino{0};
    uint64_t rdev{0};
  };
  Status MkDir(const MkDirParam& param, EntryOut& entry_out);
  Status RmDir(uint64_t parent_ino, const std::string& name);
  Status ReadDir(uint64_t ino, const std::string& last_name, uint limit, bool with_attr,
                 std::vector<EntryOut>& entry_outs);

  // create hard link
  Status Link(uint64_t ino, uint64_t new_parent_ino, const std::string& new_name, EntryOut& entry_out);
  // delete link
  Status UnLink(uint64_t parent_ino, const std::string& name);
  // create symbolic link
  Status Symlink(const std::string& symlink, uint64_t new_parent_ino, const std::string& new_name, uint32_t uid,
                 uint32_t gid, EntryOut& entry_out);
  // read symbolic link
  Status ReadLink(uint64_t ino, std::string& link);

  // attr
  struct SetAttrParam {
    uint32_t to_set{0};
    pb::mdsv2::Inode inode;
  };

  Status SetAttr(uint64_t ino, const SetAttrParam& param, EntryOut& entry_out);
  Status GetAttr(uint64_t ino, EntryOut& entry_out);

  // xattr
  Status GetXAttr(uint64_t ino, Inode::XAttrMap& xattr);
  Status GetXAttr(uint64_t ino, const std::string& name, std::string& value);
  Status SetXAttr(uint64_t ino, const std::map<std::string, std::string>& xattr);

  // update file data chunk
  Status UpdateS3Chunk();

  // rename
  Status Rename(uint64_t old_parent_ino, const std::string& old_name, uint64_t new_parent_ino,
                const std::string& new_name);

  // slice
  Status WriteSlice(uint64_t ino, uint64_t chunk_index, const pb::mdsv2::SliceList& slice_list);
  Status ReadSlice(uint64_t ino, uint64_t chunk_index, pb::mdsv2::SliceList& out_slice_list);

  OpenFiles& GetOpenFiles() { return open_files_; }
  DentryCache& GetDentryCache() { return dentry_cache_; }
  InodeCache& GetInodeCache() { return inode_cache_; }

 private:
  friend class DebugServiceImpl;

  // generate ino
  Status GenDirIno(int64_t& ino);
  Status GenFileIno(int64_t& ino);

  // get dentry
  Status GetDentrySet(uint64_t parent_ino, DentrySetPtr& out_dentry_set);
  DentrySetPtr GetDentrySetFromCache(uint64_t parent_ino);
  Status GetDentrySetFromStore(uint64_t parent_ino, DentrySetPtr& out_dentry_set);

  // get inode
  Status GetInode(uint64_t ino, InodePtr& out_inode);
  InodePtr GetInodeFromCache(uint64_t ino);
  Status GetInodeFromStore(uint64_t ino, InodePtr& out_inode);

  Status GetInodeFromDentry(const Dentry& dentry, DentrySetPtr& dentry_set, InodePtr& out_inode);

  // thorough delete inode
  Status DestoryInode(uint32_t fs_id, uint64_t ino);

  // filesystem info
  pb::mdsv2::FsInfo fs_info_;

  // generate inode id
  IdGeneratorPtr id_generator_;

  // persistence store dentry/inode
  KVStoragePtr kv_storage_;

  // for open/read/write/close file
  OpenFiles open_files_;

  // organize dentry directory tree
  DentryCache dentry_cache_;

  // organize inode
  InodeCache inode_cache_;
};

// manage all filesystem
class FileSystemSet {
 public:
  FileSystemSet(CoordinatorClientPtr coordinator_client, IdGeneratorPtr id_generator, KVStoragePtr kv_storage,
                MDSMetaMapPtr mds_meta_map);
  ~FileSystemSet();

  static FileSystemSetPtr New(CoordinatorClientPtr coordinator_client, IdGeneratorPtr id_generator,
                              KVStoragePtr kv_storage, MDSMetaMapPtr mds_meta_map) {
    return std::make_shared<FileSystemSet>(coordinator_client, std::move(id_generator), kv_storage, mds_meta_map);
  }

  bool Init();

  struct CreateFsParam {
    int64_t mds_id;
    std::string fs_name;
    uint64_t block_size;
    pb::mdsv2::FsType fs_type;
    pb::mdsv2::FsExtra fs_extra;
    bool enable_sum_in_dir;
    std::string owner;
    uint64_t capacity;
    uint32_t recycle_time_hour;
    pb::mdsv2::PartitionType partition_type;
  };

  Status CreateFs(const CreateFsParam& param, int64_t& fs_id);
  Status MountFs(const std::string& fs_name, const pb::mdsv2::MountPoint& mount_point);
  Status UmountFs(const std::string& fs_name, const pb::mdsv2::MountPoint& mount_point);
  Status DeleteFs(const std::string& fs_name);
  Status GetFsInfo(const std::string& fs_name, pb::mdsv2::FsInfo& fs_info);

  Status AllocSliceId(uint32_t slice_num, std::vector<uint64_t>& slice_ids);

  FileSystemPtr GetFileSystem(uint32_t fs_id);
  std::vector<FileSystemPtr> GetAllFileSystem();

  // load already exist filesystem
  bool LoadFileSystems();

 private:
  Status GenFsId(int64_t& fs_id);
  pb::mdsv2::FsInfo GenFsInfo(int64_t fs_id, const CreateFsParam& param);

  Status CreateFsTable();
  bool IsExistFsTable();

  bool AddFileSystem(FileSystemPtr fs);
  void DeleteFileSystem(uint32_t fs_id);

  CoordinatorClientPtr coordinator_client_;

  // for fs id
  IdGeneratorPtr id_generator_;
  // for slice id
  IdGeneratorPtr slice_id_generator_;

  KVStoragePtr kv_storage_;

  MDSMetaMapPtr mds_meta_map_;

  // protect fs_map_
  utils::RWLock lock_;
  // key: fs_id
  std::map<uint32_t, FileSystemPtr> fs_map_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_H_