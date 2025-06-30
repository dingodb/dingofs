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

#include "client/vfs/handle/dir_iterator.h"
#include "client/vfs/meta/meta_system.h"
#include "client/vfs/meta/v2/client_id.h"
#include "client/vfs/meta/v2/mds_client.h"
#include "client/vfs/meta/v2/mds_discovery.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/crontab.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class MDSV2FileSystem;
using MDSV2FileSystemPtr = std::shared_ptr<MDSV2FileSystem>;
using MDSV2FileSystemUPtr = std::unique_ptr<MDSV2FileSystem>;

class DirIteratorImpl : public DirIterator {
 public:
  DirIteratorImpl(MDSClientPtr mds_client, Ino ino)
      : mds_client_(mds_client), ino_(ino) {}

  Status Seek() override;
  bool Valid() override;
  DirEntry GetValue(bool with_attr) override;
  void Next() override;
  // TODO: need implemented
  std::string Dump() override { return {}; };
  // TODO: need implemented
  void Load(const std::string& data) override {};

 private:
  Ino ino_;
  std::string last_name_;
  bool with_attr_{false};

  uint32_t offset_{0};
  std::vector<DirEntry> entries_;

  MDSClientPtr mds_client_;
};

class FileSessionMap {
 public:
  FileSessionMap() = default;
  ~FileSessionMap() = default;

  bool Put(uint64_t fh, std::string session_id);
  void Delete(uint64_t fh);
  std::string Get(uint64_t fh);

 private:
  utils::RWLock lock_;
  // fh -> session id
  std::map<uint64_t, std::string> file_session_map_;
};

class MDSV2FileSystem : public vfs::MetaSystem {
 public:
  MDSV2FileSystem(mdsv2::FsInfoPtr fs_info, const ClientId& client_id,
                  MDSDiscoveryPtr mds_discovery, MDSClientPtr mds_client);
  ~MDSV2FileSystem() override;

  static MDSV2FileSystemUPtr New(mdsv2::FsInfoPtr fs_info,
                                 const ClientId& client_id,
                                 MDSDiscoveryPtr mds_discovery,
                                 MDSClientPtr mds_client) {
    return std::make_unique<MDSV2FileSystem>(fs_info, client_id, mds_discovery,
                                             mds_client);
  }

  static MDSV2FileSystemUPtr Build(const std::string& fs_name,
                                   const std::string& mds_addr,
                                   const std::string& mountpoint);

  Status Init() override;

  void UnInit() override;

  pb::mdsv2::FsInfo GetFsInfo() { return fs_info_->Get(); }

  Status GetFsInfo(FsInfo* fs_info) override;

  Status StatFs(Ino ino, FsStat* fs_stat) override;

  Status Lookup(Ino parent, const std::string& name, Attr* out_attr) override;

  Status Create(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
                uint32_t mode, int flags, Attr* attr, uint64_t fh) override;

  Status MkNod(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, uint64_t rdev, Attr* attr) override;

  Status Open(Ino ino, int flags, uint64_t fh) override;
  Status Close(Ino ino, uint64_t fh) override;

  Status ReadSlice(Ino ino, uint64_t index,
                   std::vector<Slice>* slices) override;
  Status NewSliceId(Ino ino, uint64_t* id) override;
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
                 uint32_t gid, const std::string& link, Attr* attr) override;
  Status ReadLink(Ino ino, std::string* link) override;

  Status GetAttr(Ino ino, Attr* attr) override;
  Status SetAttr(Ino ino, int set, const Attr& attr, Attr* out_attr) override;
  Status GetXattr(Ino ino, const std::string& name,
                  std::string* value) override;
  Status SetXattr(Ino ino, const std::string& name, const std::string& value,
                  int flags) override;
  Status ListXattr(Ino ino, std::vector<std::string>* xattrs) override;

  Status Rename(Ino old_parent, const std::string& old_name, Ino new_parent,
                const std::string& new_name) override;

 private:
  bool SetRandomEndpoint();
  bool SetEndpoints();
  bool MountFs();
  bool UnmountFs();

  void Heartbeat();

  bool InitCrontab();

  const std::string name_;
  const ClientId client_id_;

  mdsv2::FsInfoPtr fs_info_;

  MDSDiscoveryPtr mds_discovery_;

  MDSClientPtr mds_client_;

  FileSessionMap file_session_map_;

  // Crontab config
  std::vector<mdsv2::CrontabConfig> crontab_configs_;
  // This is manage crontab, like heartbeat.
  mdsv2::CrontabManager crontab_manager_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_FILESYSTEM_H_
