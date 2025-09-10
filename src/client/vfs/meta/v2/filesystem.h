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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_FILESYSTEM_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_FILESYSTEM_H_

#include <cstdint>
#include <memory>
#include <string>

#include "client/meta/vfs_meta.h"
#include "client/vfs/meta/meta_system.h"
#include "client/vfs/meta/v2/client_id.h"
#include "client/vfs/meta/v2/dir_iterator.h"
#include "client/vfs/meta/v2/file_session.h"
#include "client/vfs/meta/v2/id_cache.h"
#include "client/vfs/meta/v2/inode_cache.h"
#include "client/vfs/meta/v2/mds_client.h"
#include "client/vfs/meta/v2/mds_discovery.h"
#include "client/vfs/meta/v2/write_slice_processor.h"
#include "common/status.h"
#include "json/value.h"
#include "mdsv2/common/crontab.h"
#include "mdsv2/common/type.h"
#include "trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

using mdsv2::AttrEntry;

class MDSV2FileSystem;
using MDSV2FileSystemPtr = std::shared_ptr<MDSV2FileSystem>;
using MDSV2FileSystemUPtr = std::unique_ptr<MDSV2FileSystem>;

class MDSV2FileSystem : public vfs::MetaSystem {
 public:
  MDSV2FileSystem(mdsv2::FsInfoSPtr fs_info, const ClientId& client_id,
                  MDSDiscoverySPtr mds_discovery, InodeCacheSPtr inode_cache,
                  MDSClientSPtr mds_client);
  ~MDSV2FileSystem() override;

  static MDSV2FileSystemUPtr New(mdsv2::FsInfoSPtr fs_info,
                                 const ClientId& client_id,
                                 MDSDiscoverySPtr mds_discovery,
                                 InodeCacheSPtr inode_cache,
                                 MDSClientSPtr mds_client) {
    return std::make_unique<MDSV2FileSystem>(fs_info, client_id, mds_discovery,
                                             inode_cache, mds_client);
  }

  static MDSV2FileSystemUPtr Build(const std::string& fs_name,
                                   const std::string& mds_addrs,
                                   const std::string& mountpoint,
                                   uint32_t port);

  Status Init() override;

  void UnInit() override;

  bool Dump(ContextSPtr ctx, Json::Value& value) override;

  bool Load(ContextSPtr ctx, const Json::Value& value) override;

  mdsv2::FsInfoEntry GetFsInfo() { return fs_info_->Get(); }

  Status GetFsInfo(ContextSPtr ctx, FsInfo* fs_info) override;

  Status StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) override;

  Status Lookup(ContextSPtr ctx, Ino parent, const std::string& name,
                Attr* out_attr) override;

  Status Create(ContextSPtr ctx, Ino parent, const std::string& name,
                uint32_t uid, uint32_t gid, uint32_t mode, int flags,
                Attr* attr, uint64_t fh) override;

  Status MkNod(ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, uint64_t rdev,
               Attr* attr) override;

  Status Open(ContextSPtr ctx, Ino ino, int flags, uint64_t fh) override;
  Status Close(ContextSPtr ctx, Ino ino, uint64_t fh) override;

  Status ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
                   std::vector<Slice>* slices) override;
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

  bool GetDescription(ContextSPtr ctx, Json::Value& value) override;

 private:
  bool SetRandomEndpoint();
  bool SetEndpoints();
  bool MountFs();
  bool UnmountFs();

  void Heartbeat();

  bool InitCrontab();

  // inode cache
  // bool GetAttrFromCache(Ino ino, Attr& out_attr);
  // bool GetXAttrFromCache(Ino ino, const std::string& name, std::string&
  // value); void InsertInodeToCache(Ino ino, const AttrEntry& attr_entry); void
  // UpdateInodeToCache(Ino ino, const Attr& attr); void UpdateInodeToCache(Ino
  // ino, const AttrEntry& attr_entry); void DeleteInodeFromCache(Ino ino);

  // slice cache
  bool GetSliceFromCache(Ino ino, uint64_t index, std::vector<Slice>* slices);
  // void UpdateInodeLength(Ino ino, uint64_t new_length);
  // bool WriteSliceToCache(Ino ino, uint64_t index, uint64_t fh,
  //                        const std::vector<Slice>& slices);
  // void DeleteDeltaSliceFromCache(
  //     Ino ino, uint64_t fh,
  //     const std::vector<mdsv2::DeltaSliceEntry>& delta_slice_entries);

  // void UpdateChunkToCache(Ino ino, uint64_t fh,
  //                         const std::vector<mdsv2::ChunkEntry>& chunks);
  void ClearChunkCache(Ino ino, uint64_t fh, uint64_t index);
  // Status SyncDeltaSlice(ContextSPtr ctx, Ino ino, uint64_t fh);

  const std::string name_;
  const ClientId client_id_;

  mdsv2::FsInfoSPtr fs_info_;

  MDSDiscoverySPtr mds_discovery_;

  MDSClientSPtr mds_client_;

  FileSessionMap file_session_map_;

  DirIteratorManager dir_iterator_manager_;

  IdCache id_cache_;
  InodeCacheSPtr inode_cache_;

  // Crontab config
  std::vector<mdsv2::CrontabConfig> crontab_configs_;
  // This is manage crontab, like heartbeat.
  mdsv2::CrontabManager crontab_manager_;

  WriteSliceProcessorSPtr write_slice_processor_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_FILESYSTEM_H_
