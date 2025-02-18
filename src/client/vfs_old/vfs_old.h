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

#include "client/vfs_old/common/common.h"
#include "client/vfs_old/common/config.h"
#include "client/common/status.h"
#include "client/vfs_old/inode_cache_manager.h"
#include "client/vfs_old/lease/lease_excutor.h"
#include "client/vfs_old/service/inode_objects_service.h"
#include "client/vfs/vfs.h"
#include "client/vfs/vfs_meta.h"
#include "client/vfs_old/warmup/warmup_manager.h"
#include "dingofs/mds.pb.h"
#include "stub/rpcclient/mds_client.h"
#include "utils/throttle.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSOld : public VFS {
 public:
  VFSOld() = default;

  ~VFSOld() override = default;

  Status Start(const VFSConfig& vfs_conf) override;

  Status Stop() override;

  bool EnableSplice() override;

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

  Status Open(Ino ino, int flags, uint64_t* fh, Attr* attr) override;

  Status Create(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
                uint32_t mode, int flags, uint64_t* fh, Attr* attr) override;

  Status Read(Ino ino, char* buf, uint64_t size, uint64_t offset, uint64_t fh,
              uint64_t* out_rsize) override;

  Status Write(Ino ino, const char* buf, uint64_t size, uint64_t offset,
               uint64_t fh, uint64_t* out_wsize) override;

  Status Flush(Ino ino, uint64_t fh) override;

  Status Release(Ino ino, uint64_t fh) override;

  Status Fsync(Ino ino, int datasync, uint64_t fh) override;

  Status SetXAttr(Ino ino, const std::string& name, const std::string& value,
                  int flags) override;

  Status GetXAttr(Ino ino, const std::string& name,
                  std::string* value) override;

  Status ListXAttr(Ino ino, std::vector<std::string>* xattrs) override;

  Status Mkdir(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, Attr* attr) override;

  Status Opendir(Ino ino, uint64_t* fh) override;

  Status Readdir(Ino ino, uint64_t fh, bool plus,
                 std::vector<DirEntry>* entries) override;

  Status ReleaseDir(Ino ino, uint64_t fh) override;

  Status Rmdir(Ino parent, const std::string& name) override;

  Status StatFs(Ino ino, FsStat* fs_stat) override;

  uint64_t GetFsId() override { return fs_info_->fsid(); }

  uint64_t GetMaxNameLength() override;

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

  Status HandleOpenFlags(Ino ino, int flags, Attr* attr);

  Status AddWarmupTask(common::WarmupType type, Ino key,
                       const std::string& path,
                       common::WarmupStorageType storage_type);
  Status Warmup(Ino key, const std::string& name, const std::string& value);
  void QueryWarmupTask(Ino key, std::string* result);

  std::atomic<bool> started_{false};

  VFSConfig vfs_conf_;

  pb::mds::Mountpoint mount_point_;

  common::FuseClientOption fuse_client_option_;

  // fs info
  std::shared_ptr<pb::mds::FsInfo> fs_info_{nullptr};

  // filesystem
  std::shared_ptr<filesystem::FileSystem> fs_;

  // enable record summary info in dir inode xattr
  std::atomic<bool> enable_sum_in_dir_{false};

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

  brpc::Server server_;
  InodeObjectsService inode_object_service_;

  dingofs::utils::Mutex rename_mutex_;
};

struct FsMetricGuard {
  explicit FsMetricGuard(stub::metric::InterfaceMetric* metric, size_t* count)
      : metric(metric), count(count), start(butil::cpuwide_time_us()) {}

  ~FsMetricGuard() {
    if (!fail) {
      metric->bps.count << *count;
      metric->qps.count << 1;
      auto duration = butil::cpuwide_time_us() - start;
      metric->latency << duration;
      metric->latTotal << duration;
    } else {
      metric->eps.count << 1;
    }
  }

  void Fail() { fail = true; }

  bool fail{false};
  stub::metric::InterfaceMetric* metric;
  size_t* count;
  uint64_t start;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_OLD_VFS_OLD_H_