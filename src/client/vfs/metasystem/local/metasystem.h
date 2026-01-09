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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_LOCAL_H_
#define DINGOFS_SRC_CLIENT_VFS_META_LOCAL_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "client/vfs/metasystem/local/dir_iterator.h"
#include "client/vfs/metasystem/local/id_generator.h"
#include "client/vfs/metasystem/mds/inode_cache.h"
#include "client/vfs/metasystem/meta_system.h"
#include "client/vfs/vfs_meta.h"
#include "common/trace/context.h"
#include "dingofs/mds.pb.h"
#include "json/value.h"
#include "leveldb/db.h"
#include "mds/common/crontab.h"
#include "mds/common/type.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace local {

struct KeyValue {
  enum class OpType : uint8_t {
    kPut = 0,
    kDelete = 1,
  };

  static std::string OpTypeName(OpType op_type) {
    switch (op_type) {
      case OpType::kPut:
        return "Put";
      case OpType::kDelete:
        return "Delete";
      default:
        return "Unknown";
    }
  }

  OpType opt_type{OpType::kPut};
  std::string key;
  std::string value;
};

// for open file
class OpenFileMemo {
 public:
  OpenFileMemo();
  ~OpenFileMemo();

  struct State {
    uint32_t ref_count{0};
  };

  bool IsOpened(Ino ino);
  void Open(Ino ino);
  void Close(Ino ino, bool& is_erased);

  bool Dump(Json::Value& value);
  bool Load(const Json::Value& value);

 private:
  bthread_mutex_t mutex_;
  // ino: open file
  std::map<Ino, State> file_map_;
};

class LocalMetaSystem : public vfs::MetaSystem {
 public:
  LocalMetaSystem(const std::string& db_path, const std::string& fs_name,
                  const std::string& storage_info);
  ~LocalMetaSystem() override = default;

  LocalMetaSystem& operator=(const LocalMetaSystem&) = delete;
  LocalMetaSystem(const LocalMetaSystem&) = delete;

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
  void Stop(bool upgrade) override;

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

  Status Flush(ContextSPtr ctx, Ino ino, uint64_t fh) override;

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
                 Attr* attr) override;
  Status ReadLink(ContextSPtr ctx, Ino ino, std::string* link) override;

  Status GetAttr(ContextSPtr ctx, Ino ino, Attr* attr) override;
  Status SetAttr(ContextSPtr ctx, Ino ino, int to_set, const Attr& in_attr,
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
  void SetFsStorageInfo(mds::FsInfoEntry& fs_info,
                        const std::string& storage_info);

  mds::FsInfoEntry GenFsInfo();

  Status InitFsInfo();
  Status InitFsQuota();
  Status InitRoot();

  bool InitIdGenerators();

  bool InitCrontab();

  Ino GenDirIno();
  Ino GenFileIno();

  // prior to get inode from cache, fetch from leveldb if not exist
  Status GetAttrEntry(Ino ino, mds::AttrEntry& attr_entry);
  Status GetDentries(Ino parent, std::vector<mds::DentryEntry>& dentries);

  Status CheckDirEmpty(Ino ino, bool& is_empty);

  Status CleanChunk(Ino ino);
  void CleanDelfile();
  Status DoCleanDelfile();

  // inode cache operations
  void PutInodeToCache(const mds::AttrEntry& attr_entry);
  void DeleteInodeFromCache(Ino ino);
  meta::InodeSPtr GetInodeFromCache(Ino ino);

  // quota operations
  void UpdateFsUsage(int64_t byte_delta, int64_t inode_delta,
                     const std::string& reason);
  mds::QuotaEntry GetFsQuota();

  // leveldb operations
  bool OpenLevelDB(const std::string& db_path);
  void CloseLevelDB();

  Status Get(const std::string& key, std::string& value);
  Status Put(std::vector<KeyValue>& kvs);

  const std::string fs_name_;
  const std::string db_path_;
  const std::string storage_info_;
  mds::FsInfoEntry fs_info_;

  // for generating inode id
  IdGeneratorUPtr ino_generator_;
  // for generating slice id
  IdGeneratorUPtr slice_id_generator_;

  // for open file
  OpenFileMemo open_file_memo_;
  // for read dir
  DirIteratorManager dir_iterator_manager_;
  // for cache inode
  meta::InodeCacheUPtr inode_cache_;

  utils::RWLock fs_quota_lock_;
  // for fs quota
  mds::QuotaEntry fs_quota_;

  // Crontab config
  std::vector<mds::CrontabConfig> crontab_configs_;
  // This is manage crontab, like heartbeat.
  mds::CrontabManager crontab_manager_;

  utils::RWLock lock_;
  // for store data
  leveldb::DB* db_{nullptr};
};

}  // namespace local
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_LOCAL_H_
