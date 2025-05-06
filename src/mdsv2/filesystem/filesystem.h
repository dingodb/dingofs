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
#include "mdsv2/common/context.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/file_session.h"
#include "mdsv2/filesystem/fs_info.h"
#include "mdsv2/filesystem/id_generator.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/filesystem/mutation_processor.h"
#include "mdsv2/filesystem/partition.h"
#include "mdsv2/filesystem/renamer.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/storage/storage.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mdsv2 {

class FileSystem;
using FileSystemSPtr = std::shared_ptr<FileSystem>;

class FileSystemSet;
using FileSystemSetSPtr = std::shared_ptr<FileSystemSet>;

struct EntryOut {
  EntryOut() = default;

  explicit EntryOut(const pb::mdsv2::Inode& inode) : inode(inode) {}

  std::string name;
  pb::mdsv2::Inode inode;
};

class FileSystem : public std::enable_shared_from_this<FileSystem> {
 public:
  FileSystem(int64_t self_mds_id, FsInfoUPtr fs_info, IdGeneratorUPtr id_generator, KVStorageSPtr kv_storage,
             RenamerPtr renamer, MutationProcessorSPtr mutation_processor, MDSMetaMapSPtr mds_meta_map);
  ~FileSystem() = default;

  static FileSystemSPtr New(int64_t self_mds_id, FsInfoUPtr fs_info, IdGeneratorUPtr id_generator,
                            KVStorageSPtr kv_storage, RenamerPtr renamer, MutationProcessorSPtr mutation_processor,
                            MDSMetaMapSPtr mds_meta_map) {
    return std::make_shared<FileSystem>(self_mds_id, std::move(fs_info), std::move(id_generator), kv_storage, renamer,
                                        mutation_processor, mds_meta_map);
  }

  FileSystemSPtr GetSelfPtr();

  uint32_t FsId() const { return fs_id_; }
  std::string FsName() const { return fs_info_->GetName(); }

  uint64_t Epoch() const;

  pb::mdsv2::FsInfo FsInfo() const { return fs_info_->Get(); }

  pb::mdsv2::PartitionType PartitionType() const;
  bool IsMonoPartition() const;
  bool IsParentHashPartition() const;

  bool CanServe() const { return can_serve_; };

  // create root directory
  Status CreateRoot();

  // lookup dentry
  Status Lookup(Context& ctx, uint64_t parent_ino, const std::string& name, EntryOut& entry_out);

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
  Status MkNod(Context& ctx, const MkNodParam& param, EntryOut& entry_out);
  Status Open(Context& ctx, uint64_t ino, std::string& session_id);
  Status Release(Context& ctx, uint64_t ino, const std::string& session_id);

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
  Status MkDir(Context& ctx, const MkDirParam& param, EntryOut& entry_out);
  Status RmDir(Context& ctx, uint64_t parent_ino, const std::string& name);
  Status ReadDir(Context& ctx, uint64_t ino, const std::string& last_name, uint limit, bool with_attr,
                 std::vector<EntryOut>& entry_outs);

  // create hard link
  Status Link(Context& ctx, uint64_t ino, uint64_t new_parent_ino, const std::string& new_name, EntryOut& entry_out);
  // delete link
  Status UnLink(Context& ctx, uint64_t parent_ino, const std::string& name);
  // create symbolic link
  Status Symlink(Context& ctx, const std::string& symlink, uint64_t new_parent_ino, const std::string& new_name,
                 uint32_t uid, uint32_t gid, EntryOut& entry_out);
  // read symbolic link
  Status ReadLink(Context& ctx, uint64_t ino, std::string& link);

  // attr
  struct SetAttrParam {
    uint32_t to_set{0};
    pb::mdsv2::Inode inode;
  };

  Status SetAttr(Context& ctx, uint64_t ino, const SetAttrParam& param, EntryOut& entry_out);
  Status GetAttr(Context& ctx, uint64_t ino, EntryOut& entry_out);

  // xattr
  Status GetXAttr(Context& ctx, uint64_t ino, Inode::XAttrMap& xattr);
  Status GetXAttr(Context& ctx, uint64_t ino, const std::string& name, std::string& value);
  Status SetXAttr(Context& ctx, uint64_t ino, const std::map<std::string, std::string>& xattrs);

  // rename
  Status Rename(Context& ctx, uint64_t old_parent_ino, const std::string& old_name, uint64_t new_parent_ino,
                const std::string& new_name, uint64_t& old_parent_version, uint64_t& new_parent_version);
  Status RenameWithRetry(Context& ctx, uint64_t old_parent_ino, const std::string& old_name, uint64_t new_parent_ino,
                         const std::string& new_name, uint64_t& old_parent_version, uint64_t& new_parent_version);
  Status CommitRename(Context& ctx, uint64_t old_parent_ino, const std::string& old_name, uint64_t new_parent_ino,
                      const std::string& new_name, uint64_t& old_parent_version, uint64_t& new_parent_version);

  // slice
  Status WriteSlice(Context& ctx, uint64_t ino, uint64_t chunk_index, const std::vector<pb::mdsv2::Slice>& slices);
  Status ReadSlice(Context& ctx, uint64_t ino, uint64_t chunk_index, std::vector<pb::mdsv2::Slice>& slices);

  // compact
  Status CompactChunk(Context& ctx, uint64_t ino, uint64_t chunk_index,
                      std::vector<pb::mdsv2::TrashSlice>& trash_slices);
  Status CompactFile(Context& ctx, uint64_t ino, std::vector<pb::mdsv2::TrashSlice>& trash_slices);
  Status CompactAll(Context& ctx, uint64_t& checked_count, uint64_t& compacted_count);
  Status CleanTrashFileData(Context& ctx, uint64_t ino);

  // dentry/inode
  Status GetDentry(Context& ctx, uint64_t parent, const std::string& name, Dentry& dentry);
  Status ListDentry(Context& ctx, uint64_t parent, const std::string& last_name, uint32_t limit, bool is_only_dir,
                    std::vector<Dentry>& dentries);
  Status GetInode(Context& ctx, uint64_t ino, EntryOut& entry_out);
  Status BatchGetInode(Context& ctx, const std::vector<uint64_t>& inoes, std::vector<EntryOut>& out_entries);
  Status BatchGetXAttr(Context& ctx, const std::vector<uint64_t>& inoes, std::vector<pb::mdsv2::XAttr>& out_xattrs);

  Status RefreshInode(const std::vector<uint64_t>& inoes);

  Status RefreshFsInfo();
  Status RefreshFsInfo(const std::string& name);
  void RefreshFsInfo(const pb::mdsv2::FsInfo& fs_info);

  Status UpdatePartitionPolicy(uint64_t mds_id);
  Status UpdatePartitionPolicy(const std::map<uint64_t, pb::mdsv2::HashPartition::BucketSet>& distributions);

  PartitionCache& GetPartitionCache() { return partition_cache_; }
  InodeCache& GetInodeCache() { return inode_cache_; }

 private:
  friend class DebugServiceImpl;
  friend class FsStatServiceImpl;

  // generate ino
  Status GenDirIno(int64_t& ino);
  Status GenFileIno(int64_t& ino);
  bool CanServe(int64_t self_mds_id);

  // get partition
  Status GetPartition(Context& ctx, uint64_t parent, PartitionPtr& out_partition);
  Status GetPartition(Context& ctx, uint64_t version, uint64_t parent, PartitionPtr& out_partition);
  PartitionPtr GetPartitionFromCache(uint64_t parent_ino);
  std::map<uint64_t, PartitionPtr> GetAllPartitionsFromCache();
  Status GetPartitionFromStore(uint64_t parent_ino, const std::string& reason, PartitionPtr& out_partition);

  // get dentry
  Status GetDentryFromStore(uint64_t parent, const std::string& name, Dentry& dentry);
  Status ListDentryFromStore(uint64_t parent, const std::string& last_name, uint32_t limit, bool is_only_dir,
                             std::vector<Dentry>& dentries);

  // get inode
  Status GetInode(Context& ctx, Dentry& dentry, PartitionPtr partition, InodeSPtr& out_inode);
  Status GetInode(Context& ctx, uint64_t version, Dentry& dentry, PartitionPtr partition, InodeSPtr& out_inode);
  Status GetInode(Context& ctx, uint64_t ino, InodeSPtr& out_inode);
  Status GetInode(Context& ctx, uint64_t version, uint64_t ino, InodeSPtr& out_inode);
  InodeSPtr GetInodeFromCache(uint64_t ino);
  std::map<uint64_t, InodeSPtr> GetAllInodesFromCache();
  Status GetInodeFromStore(uint64_t ino, const std::string& reason, InodeSPtr& out_inode);
  Status BatchGetInodeFromStore(std::vector<uint64_t> inoes, std::vector<InodeSPtr>& out_inodes);

  // thorough delete inode
  Status DestoryInode(uint32_t fs_id, uint64_t ino);

  // part fail, clean/rollback
  Status CleanUpInode(InodeSPtr inode);
  Status CleanUpDentry(Dentry& dentry);
  Status RollbackFileNlink(uint32_t fs_id, uint64_t ino, int delta);

  uint64_t GetMdsIdByIno(uint64_t ino);

  std::vector<pb::mdsv2::TrashSlice> GenTrashSlices(uint64_t ino, uint64_t file_length,
                                                    const pb::mdsv2::Chunk& chunk) const;
  std::vector<pb::mdsv2::TrashSlice> DoCompactChunk(uint64_t ino, uint64_t file_length, pb::mdsv2::Chunk& chunk);
  std::vector<pb::mdsv2::TrashSlice> DoCompactChunk(uint64_t ino, uint64_t file_length, pb::mdsv2::Chunk& chunk,
                                                    Txn* txn);

  void SendRefreshInode(uint64_t mds_id, uint32_t fs_id, const std::vector<uint64_t>& inoes);

  uint64_t self_mds_id_;

  // filesystem info
  FsInfoUPtr fs_info_;
  const uint32_t fs_id_;

  bool can_serve_{false};

  // generate inode id
  IdGeneratorUPtr id_generator_;

  // persistence store dentry/inode
  KVStorageSPtr kv_storage_;

  // for open/read/write/close file
  FileSessionManagerUPtr file_session_manager_;

  // organize dentry directory tree
  PartitionCache partition_cache_;

  // organize inode
  InodeCache inode_cache_;

  // mds meta map
  MDSMetaMapSPtr mds_meta_map_;

  RenamerPtr renamer_;

  // muation merger
  MutationProcessorSPtr mutation_processor_;
};

// manage all filesystem
class FileSystemSet {
 public:
  FileSystemSet(CoordinatorClientSPtr coordinator_client, IdGeneratorUPtr fs_id_generator,
                IdGeneratorUPtr slice_id_generator, KVStorageSPtr kv_storage, MDSMeta self_mds_meta,
                MDSMetaMapSPtr mds_meta_map, RenamerPtr renamer, MutationProcessorSPtr mutation_processor);
  ~FileSystemSet();

  static FileSystemSetSPtr New(CoordinatorClientSPtr coordinator_client, IdGeneratorUPtr fs_id_generator,
                               IdGeneratorUPtr slice_id_generator, KVStorageSPtr kv_storage, MDSMeta self_mds_meta,
                               MDSMetaMapSPtr mds_meta_map, RenamerPtr renamer,
                               MutationProcessorSPtr mutation_processor) {
    return std::make_shared<FileSystemSet>(coordinator_client, std::move(fs_id_generator),
                                           std::move(slice_id_generator), kv_storage, self_mds_meta, mds_meta_map,
                                           renamer, mutation_processor);
  }

  bool Init();

  struct CreateFsParam {
    int64_t mds_id;
    std::string fs_name;
    uint64_t block_size;
    uint64_t chunk_size;
    pb::mdsv2::FsType fs_type;
    pb::mdsv2::FsExtra fs_extra;
    bool enable_sum_in_dir;
    std::string owner;
    uint64_t capacity;
    uint32_t recycle_time_hour;
    pb::mdsv2::PartitionType partition_type;
  };

  Status CreateFs(const CreateFsParam& param, pb::mdsv2::FsInfo& fs_info);
  Status MountFs(const std::string& fs_name, const pb::mdsv2::MountPoint& mount_point);
  Status UmountFs(const std::string& fs_name, const pb::mdsv2::MountPoint& mount_point);
  Status DeleteFs(const std::string& fs_name, bool is_force);
  Status UpdateFsInfo(Context& ctx, const std::string& fs_name, const pb::mdsv2::FsInfo& fs_info);
  Status GetFsInfo(Context& ctx, const std::string& fs_name, pb::mdsv2::FsInfo& fs_info);
  Status GetAllFsInfo(Context& ctx, std::vector<pb::mdsv2::FsInfo>& fs_infoes);
  Status RefreshFsInfo(const std::string& fs_name);
  Status RefreshFsInfo(uint32_t fs_id);

  Status AllocSliceId(uint32_t slice_num, std::vector<uint64_t>& slice_ids);

  FileSystemSPtr GetFileSystem(uint32_t fs_id);
  FileSystemSPtr GetFileSystem(const std::string& fs_name);
  uint32_t GetFsId(const std::string& fs_name);
  std::vector<FileSystemSPtr> GetAllFileSystem();

  // load already exist filesystem
  bool LoadFileSystems();

 private:
  Status GenFsId(int64_t& fs_id);
  pb::mdsv2::FsInfo GenFsInfo(int64_t fs_id, const CreateFsParam& param);

  Status CreateFsTable();
  bool IsExistFsTable();

  bool AddFileSystem(FileSystemSPtr fs, bool is_force = false);
  void DeleteFileSystem(uint32_t fs_id);

  CoordinatorClientSPtr coordinator_client_;

  // for fs id
  IdGeneratorUPtr id_generator_;
  // for slice id
  IdGeneratorUPtr slice_id_generator_;

  KVStorageSPtr kv_storage_;

  RenamerPtr renamer_;

  MutationProcessorSPtr mutation_processor_;

  MDSMeta self_mds_meta_;
  MDSMetaMapSPtr mds_meta_map_;

  // protect fs_map_
  utils::RWLock lock_;
  // key: fs_id
  std::map<uint32_t, FileSystemSPtr> fs_map_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_H_