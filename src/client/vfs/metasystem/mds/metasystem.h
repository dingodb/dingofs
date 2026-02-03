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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "client/vfs/common/client_id.h"
#include "client/vfs/compaction/compactor.h"
#include "client/vfs/metasystem/mds/batch_processor.h"
#include "client/vfs/metasystem/mds/chunk.h"
#include "client/vfs/metasystem/mds/chunk_memo.h"
#include "client/vfs/metasystem/mds/compact.h"
#include "client/vfs/metasystem/mds/dir_iterator.h"
#include "client/vfs/metasystem/mds/executor.h"
#include "client/vfs/metasystem/mds/file_session.h"
#include "client/vfs/metasystem/mds/id_cache.h"
#include "client/vfs/metasystem/mds/inode_cache.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "client/vfs/metasystem/mds/modify_time_memo.h"
#include "client/vfs/metasystem/mds/tiny_file_data.h"
#include "client/vfs/metasystem/meta_system.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "common/trace/trace_manager.h"
#include "glog/logging.h"
#include "json/value.h"
#include "mds/common/crontab.h"
#include "mds/common/type.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

using mds::AttrEntry;

class MDSMetaSystem;
using MDSMetaSystemPtr = std::shared_ptr<MDSMetaSystem>;
using MDSMetaSystemUPtr = std::unique_ptr<MDSMetaSystem>;

class MDSMetaSystem : public vfs::MetaSystem {
 public:
  MDSMetaSystem(mds::FsInfoEntry fs_info_entry, const ClientId& client_id,
                RPC&& rpc, TraceManager& trace_manager, Compactor& compactor);
  ~MDSMetaSystem() override;

  static MDSMetaSystemUPtr New(mds::FsInfoEntry fs_info_entry,
                               const ClientId& client_id, RPC&& rpc,
                               TraceManager& trace_manager,
                               Compactor& compactor) {
    return std::make_unique<MDSMetaSystem>(
        fs_info_entry, client_id, std::move(rpc), trace_manager, compactor);
  }

  static MDSMetaSystemUPtr Build(const std::string& fs_name,
                                 const std::string& mds_addrs,
                                 const ClientId& client_id,
                                 TraceManager& trace_manager,
                                 Compactor& compactor);

  Status Init(bool upgrade) override;

  void Stop(bool upgrade) override;

  bool GetSummary(Json::Value& value) override;
  // dump state for upgrade
  bool Dump(ContextSPtr ctx, Json::Value& value) override;

  // dump state for show
  bool Dump(const DumpOption& options, Json::Value& value) override;

  bool Load(ContextSPtr ctx, const Json::Value& value) override;

  mds::FsInfoEntry GetFsInfo() { return fs_info_.Get(); }

  Status GetFsInfo(ContextSPtr ctx, FsInfo* fs_info) override;

  Status StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) override;

  Status Lookup(ContextSPtr ctx, Ino parent, const std::string& name,
                Attr* attr) override;

  Status Create(ContextSPtr ctx, Ino parent, const std::string& name,
                uint32_t uid, uint32_t gid, uint32_t mode, int flags,
                Attr* attr, uint64_t fh) override;

  Status MkNod(ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, uint64_t rdev,
               Attr* attr) override;

  Status Open(ContextSPtr ctx, Ino ino, int flags, uint64_t fh) override;

  Status Flush(ContextSPtr ctx, Ino ino, uint64_t fh) override;

  Status Close(ContextSPtr ctx, Ino ino, uint64_t fh) override;

  Status ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
                   std::vector<Slice>* slices, uint64_t& version) override;

  Status NewSliceId(ContextSPtr ctx, Ino ino, uint64_t* id) override;

  Status WriteSlice(ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
                    const std::vector<Slice>& slices) override;
  Status Write(ContextSPtr ctx, Ino ino, const char* buf, uint64_t offset,
               uint64_t size, uint64_t fh) override;

  Status Read(ContextSPtr ctx, Ino ino, uint64_t fh, uint64_t offset,
              uint64_t size, vfs::DataBuffer& data_buffer,
              uint64_t& out_rsize) override;

  Status MkDir(ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, Attr* attr) override;
  Status RmDir(ContextSPtr ctx, Ino parent, const std::string& name) override;

  Status OpenDir(ContextSPtr ctx, Ino ino, uint64_t fh,
                 bool& need_cache) override;

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
  Status SetAttr(ContextSPtr ctx, Ino ino, int set, const Attr& attr,
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

  Status Compact(ContextSPtr ctx, Ino ino, uint32_t chunk_index,
                 bool is_async) override;

  bool GetDescription(Json::Value& value) override;

 private:
  friend class OpenTask;

  bool SetRandomEndpoint();
  bool SetEndpoints();
  bool MountFs();
  bool UnmountFs();

  void Heartbeat();
  void CleanExpiredModifyTimeMemo();
  void CleanExpiredChunkMemo();
  void CleanExpiredChunkCache();
  void CleanExpiredInodeCache();
  void CleanExpiredTinyFileDataCache();

  bool InitCrontab();

  // inode cache
  InodeSPtr PutInodeToCache(const AttrEntry& attr_entry) {
    return inode_cache_.Put(attr_entry.ino(), attr_entry);
  }
  void DeleteInodeFromCache(Ino ino) { inode_cache_.Delete(ino); }
  InodeSPtr GetInodeFromCache(Ino ino) { return inode_cache_.Get(ino); }
  InodeSPtr GetInode(FileSessionSPtr& file_session) {
    auto inode = file_session->GetInode();
    if (inode != nullptr) return inode;

    return inode_cache_.Get(file_session->GetIno());
  }

  // chunk cache
  Status FlushFile(ContextSPtr ctx, InodeSPtr inode, ChunkSetSPtr& chunk_set);
  void LaunchWriteSlice(ContextSPtr& ctx, ChunkSetSPtr chunk_set,
                        CommitTaskSPtr task);
  // async flush batch slices of single file
  void AsyncFlushSlice(ContextSPtr& ctx, ChunkSetSPtr chunk_set, bool is_force,
                       bool is_wait);
  // flush slices of single file
  Status FlushSlice(ContextSPtr ctx, Ino ino);
  // flush slices of all files
  void FlushAllSlice();

  Status CorrectAttr(ContextSPtr ctx, uint64_t time_ns, Attr& attr,
                     bool& is_amend, const std::string& caller);
  bool CorrectAttrLength(Attr& attr, const std::string& caller);

  bool IsPrefetchChunk(Ino ino);
  bool IsPrefetchTinyFileData(Ino ino);
  Status DoOpen(ContextSPtr ctx, Ino ino, int flags, uint64_t fh,
                const std::string& session_id, FileSessionSPtr file_session);
  void AsyncOpen(ContextSPtr ctx, Ino ino, int flags, uint64_t fh,
                 const std::string& session_id, FileSessionSPtr file_session);

  void AsyncClose(ContextSPtr ctx, Ino ino, uint64_t fh,
                  const std::string& session_id);

  // batch operation
  Status RunOperation(OperationSPtr operation);

  void AssertStop() {
    CHECK(!stopped_.load(std::memory_order_relaxed)) << "metasystem is stopped";
  }

  const std::string name_;
  const ClientId client_id_;

  mds::FsInfo fs_info_;

  Executor executor_;

  MDSClient mds_client_;

  ModifyTimeMemo modify_time_memo_;

  ChunkCache chunk_cache_;

  ChunkMemo chunk_memo_;

  FileSessionMap file_session_map_;

  DirIteratorManager dir_iterator_manager_;

  IdCache id_cache_;
  InodeCache inode_cache_;

  TinyFileDataCache tiny_file_data_cache_;

  // Crontab config
  std::vector<mds::CrontabConfig> crontab_configs_;
  // This is manage crontab, like heartbeat.
  mds::CrontabManager crontab_manager_;

  BatchProcessor batch_processor_;

  CompactProcessor compact_processor_;

  Compactor& compactor_;

  std::atomic<bool> stopped_{false};
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_H_
