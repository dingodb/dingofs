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
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/id_generator.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {

namespace mdsv2 {

class FileSystem;
using FileSystemPtr = std::shared_ptr<FileSystem>;

class FileSystemSet;
using FileSystemSetPtr = std::shared_ptr<FileSystemSet>;

class FileSystem {
 public:
  FileSystem(IdGeneratorPtr id_generator, KVStoragePtr kv_storage)
      : id_generator_(id_generator), kv_storage_(kv_storage){};
  ~FileSystem() = default;

  static FileSystemPtr New(IdGeneratorPtr id_generator, KVStoragePtr kv_storage) {
    return std::make_shared<FileSystem>(id_generator, kv_storage);
  }

  bool Init();

  // dentry/inode
  Status MkNod(const pb::mdsv2::MkNodRequest* request);
  Status MkDir(const pb::mdsv2::MkDirRequest* request);
  Status RmDir(const pb::mdsv2::RmDirRequest* request);

  Status Link(uint64_t parent_ino, const std::string& name, uint64_t ino);
  Status UnLink(uint64_t parent_ino, const std::string& name);
  Status Symlink(const pb::mdsv2::SymlinkRequest* request);
  Status ReadLink(uint64_t ino, std::string& link);

  DentryPtr GetDentry(uint64_t ino);
  DentryPtr GetDentry(const std::string& name);
  std::vector<DentryPtr> GetDentries(uint64_t ino, const std::string& last_name, uint32_t limit, bool is_only_dir);

  Status GetInode(uint64_t parent_ino, const std::string& name, InodePtr& out_inode);
  Status GetInode(uint64_t ino, InodePtr& out_inode);
  InodePtr GetInodeFromCache(uint64_t parent_ino, const std::string& name);
  InodePtr GetInodeFromCache(uint64_t ino);

  struct UpdateInodeParam {
    std::vector<std::string> update_fields;

    uint64_t ino{0};
    uint32_t fs_id{0};
    uint64_t length{0};
    uint64_t ctime{0};
    uint64_t mtime{0};
    uint64_t atime{0};
    uint32_t uid{0};
    uint32_t gid{0};
    uint32_t mode{0};
  };
  Status UpdateInode(const UpdateInodeParam& param, InodePtr& out_inode);

  Status GetXAttr(uint64_t ino, Inode::XAttrMap& xattr);
  Status GetXAttr(uint64_t ino, const std::string& name, std::string& value);
  Status SetXAttr(uint64_t ino, const std::map<std::string, std::string>& xattr);

  Status UpdateS3Chunk();

 private:
  Status GenIno(int64_t& ino);

  pb::mdsv2::FsInfo fs_info_;

  IdGeneratorPtr id_generator_;
  KVStoragePtr kv_storage_;

  DentryMap dentry_map_;
  InodeMap inode_map_;
};

// manage all filesystem
class FileSystemSet {
 public:
  FileSystemSet(IdGeneratorPtr id_generator, KVStoragePtr kv_storage);
  ~FileSystemSet();

  static FileSystemSetPtr New(IdGeneratorPtr id_generator, KVStoragePtr kv_storage) {
    return std::make_shared<FileSystemSet>(id_generator, kv_storage);
  }

  bool Init();

  struct CreateFsParam {
    std::string fs_name;
    uint64_t block_size;
    pb::mdsv2::FsType fs_type;
    pb::mdsv2::FsDetail fs_detail;
    bool enable_sum_in_dir;
    std::string owner;
    uint64_t capacity;
    uint32_t recycle_time_hour;
  };

  Status CreateFs(const CreateFsParam& param, int64_t& fs_id);
  Status MountFs(const std::string& fs_name, const pb::mdsv2::MountPoint& mount_point);
  Status UmountFs(const std::string& fs_name, const pb::mdsv2::MountPoint& mount_point);
  Status DeleteFs(const std::string& fs_name);
  Status GetFsInfo(const std::string& fs_name, pb::mdsv2::FsInfo& fs_info);

  FileSystemPtr GetFileSystem(uint32_t fs_id);

 private:
  Status GenFsId(int64_t& fs_id);
  static std::string GenFsValue(int64_t fs_id, const CreateFsParam& param);

  Status CreateFsTable();
  bool IsExistFsTable();

  IdGeneratorPtr id_generator_;
  KVStoragePtr kv_storage_;

  bthread_mutex_t mutex_;
  // key: fs_id
  std::map<uint32_t, FileSystemPtr> fs_map_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_H_