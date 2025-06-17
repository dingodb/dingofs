/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DINGOFS_CLIENT_VFS_OLD_VFS_OLD_H_
#define DINGOFS_CLIENT_VFS_OLD_VFS_OLD_H_

#include <brpc/server.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "blockaccess/block_accesser.h"
#include "client/meta/vfs_meta.h"
#include "client/vfs.h"
#include "client/vfs_legacy/common/common.h"
#include "client/vfs_legacy/inode_cache_manager.h"
#include "client/vfs_legacy/lease/lease_excutor.h"
#include "client/vfs_legacy/service/inode_objects_service.h"
#include "client/vfs_legacy/warmup/warmup_manager.h"
#include "common/status.h"
#include "dingofs/mds.pb.h"
#include "options/client/vfs_legacy/vfs_legacy_option.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSOld : public VFS {
 public:
  VFSOld(const VFSLegacyOption& option) : option_(option) {}

  ~VFSOld() override = default;

  Status Start(const VFSConfig& vfs_conf) override;

  Status Stop() override;

  bool Dump(Json::Value& value) override;

  bool Load(const Json::Value& value) override;

  double GetAttrTimeout(const FileType& type) override;

  double GetEntryTimeout(const FileType& type) override;

  Status Lookup(Ino parent, const std::string& name, Attr* attr) override;

  Status GetAttr(Ino ino, Attr* attr) override;

  Status SetAttr(Ino ino, int set, const Attr& in_attr,
                 Attr* out_attr) override;

  Status ReadLink(Ino ino, std::string* link) override;

  Status MkNod(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, uint64_t dev, Attr* attr) override;

  Status Unlink(Ino parent, const std::string& name) override;

  Status Symlink(Ino parent, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& link, Attr* attr) override;

  Status Rename(Ino old_parent, const std::string& old_name, Ino new_parent,
                const std::string& new_name) override;

  Status Link(Ino ino, Ino new_parent, const std::string& new_name,
              Attr* attr) override;

  Status Open(Ino ino, int flags, uint64_t* fh) override;

  Status Create(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
                uint32_t mode, int flags, uint64_t* fh, Attr* attr) override;

  Status Read(Ino ino, char* buf, uint64_t size, uint64_t offset, uint64_t fh,
              uint64_t* out_rsize) override;

  Status Write(Ino ino, const char* buf, uint64_t size, uint64_t offset,
               uint64_t fh, uint64_t* out_wsize) override;

  Status Flush(Ino ino, uint64_t fh) override;

  Status Release(Ino ino, uint64_t fh) override;

  Status Fsync(Ino ino, int datasync, uint64_t fh) override;

  Status SetXattr(Ino ino, const std::string& name, const std::string& value,
                  int flags) override;

  Status GetXattr(Ino ino, const std::string& name,
                  std::string* value) override;

  Status ListXattr(Ino ino, std::vector<std::string>* xattrs) override;

  Status MkDir(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, Attr* attr) override;

  Status OpenDir(Ino ino, uint64_t* fh) override;

  Status ReadDir(Ino ino, uint64_t fh, uint64_t offset, bool with_attr,
                 ReadDirHandler handler) override;

  Status ReleaseDir(Ino ino, uint64_t fh) override;

  Status RmDir(Ino parent, const std::string& name) override;

  Status StatFs(Ino ino, FsStat* fs_stat) override;

  uint64_t GetFsId() override { return fs_info_->fsid(); }

  uint64_t GetMaxNameLength() override;

  FuseOption GetFuseOption() override { return option_.fuse_option; }

  void InitQosParam();

 private:
  int SetMountStatus();
  int InitBrpcServer();

  void ReadThrottleAdd(uint64_t size);

  void WriteThrottleAdd(uint64_t size);

  Status AllocNode(Ino parent, const std::string& name, uint32_t uid,
                   uint32_t gid, pb::metaserver::FsFileType type, uint32_t mode,
                   uint64_t dev, std::string path,
                   std::shared_ptr<InodeWrapper>* out_inode_wrapper);

  Status Truncate(InodeWrapper* inode, uint64_t length);

  DINGOFS_ERROR UpdateParentMCTime(Ino parent);
  DINGOFS_ERROR UpdateParentMCTimeAndNlink(Ino parent,
                                           common::NlinkChange nlink);

  Status HandleOpenFlags(Ino ino, int flags);

  Status AddWarmupTask(common::WarmupType type, Ino key,
                       const std::string& path,
                       common::WarmupStorageType storage_type);
  Status Warmup(Ino key, const std::string& name, const std::string& value);
  void QueryWarmupTask(Ino key, std::string* result);

  Status InitDirHandle(Ino ino, uint64_t fh, bool with_attr);

  std::atomic<bool> started_{false};

  VFSConfig vfs_conf_;

  pb::mds::Mountpoint mount_point_;

  VFSLegacyOption option_;

  // fs info
  std::shared_ptr<pb::mds::FsInfo> fs_info_{nullptr};

  // filesystem
  std::shared_ptr<filesystem::FileSystem> fs_;

  // mds client
  std::shared_ptr<stub::rpcclient::MDSBaseClient> mds_base_;
  std::shared_ptr<stub::rpcclient::MdsClient> mds_client_;

  // metaserver client
  std::shared_ptr<stub::rpcclient::MetaServerClient> metaserver_client_;

  std::shared_ptr<LeaseExecutor> lease_executor_;

  // inode cache manager
  std::shared_ptr<InodeCacheManager> inode_cache_manager_;

  // dentry cache manager
  std::shared_ptr<DentryCacheManager> dentry_cache_manager_;

  // warmup manager
  std::shared_ptr<warmup::WarmupManager> warmup_manager_;

  utils::Throttle throttle_;
  bthread_timer_t throttle_timer_;

  // s3 adaptor
  std::shared_ptr<S3ClientAdaptor> s3_adapter_;

  std::unique_ptr<blockaccess::BlockAccesser> block_accesser_{nullptr};

  brpc::Server server_;
  InodeObjectsService inode_object_service_;

  dingofs::utils::Mutex rename_mutex_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_OLD_VFS_OLD_H_