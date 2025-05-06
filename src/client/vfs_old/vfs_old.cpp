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

#include "client/vfs_old/vfs_old.h"

#include <absl/cleanup/cleanup.h>
#include <bthread/unstable.h>
#include <fcntl.h>
#include <fmt/core.h>
#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "cache/blockcache/block_cache.h"
#include "client/common/dynamic_config.h"
#include "common/status.h"
#include "client/datastream/data_stream.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/vfs_meta.h"
#include "client/vfs_old/client_operator.h"
#include "client/vfs_old/dentry_cache_manager.h"
#include "client/vfs_old/filesystem/dir_cache.h"
#include "client/vfs_old/filesystem/filesystem.h"
#include "client/vfs_old/filesystem/meta.h"
#include "client/vfs_old/filesystem/package.h"
#include "client/vfs_old/inode_cache_manager.h"
#include "client/vfs_old/inode_wrapper.h"
#include "client/vfs_old/service/metrics_dumper.h"
#include "client/vfs_old/tools.h"
#include "common/define.h"
#include "dataaccess/accesser.h"
#include "dataaccess/s3/aws/s3_adapter.h"
#include "dataaccess/s3/s3_accesser.h"
#include "dingofs/mds.pb.h"
#include "dingofs/metaserver.pb.h"
#include "stub/filesystem/xattr.h"
#include "stub/rpcclient/base_client.h"
#include "stub/rpcclient/channel_manager.h"
#include "stub/rpcclient/cli2_client.h"
#include "stub/rpcclient/mds_client.h"
#include "stub/rpcclient/metacache.h"
#include "utils/configuration.h"
#include "utils/net_common.h"
#include "utils/throttle.h"

#define PORT_LIMIT 65535

#define RETURN_IF_UNSUCCESS(action)                                   \
  do {                                                                \
    rc = rename_operator.action();                                    \
    if (rc != DINGOFS_ERROR::OK) {                                    \
      return ::dingofs::client::filesystem::DingofsErrorToStatus(rc); \
    }                                                                 \
  } while (0)

namespace dingofs {
namespace client {
namespace vfs {

using cache::blockcache::BlockCacheImpl;

static void OnThrottleTimer(void* arg) {
  VFSOld* vfs = reinterpret_cast<VFSOld*>(arg);
  vfs->InitQosParam();
}

void VFSOld::InitQosParam() {
  utils::ReadWriteThrottleParams params;
  params.iopsWrite =
      utils::ThrottleParams(common::FLAGS_fuseClientAvgWriteIops,
                            common::FLAGS_fuseClientBurstWriteIops,
                            common::FLAGS_fuseClientBurstWriteIopsSecs);

  params.bpsWrite =
      utils::ThrottleParams(common::FLAGS_fuseClientAvgWriteBytes,
                            common::FLAGS_fuseClientBurstWriteBytes,
                            common::FLAGS_fuseClientBurstWriteBytesSecs);

  params.iopsRead =
      utils::ThrottleParams(common::FLAGS_fuseClientAvgReadIops,
                            common::FLAGS_fuseClientBurstReadIops,
                            common::FLAGS_fuseClientBurstReadIopsSecs);

  params.bpsRead =
      utils::ThrottleParams(common::FLAGS_fuseClientAvgReadBytes,
                            common::FLAGS_fuseClientBurstReadBytes,
                            common::FLAGS_fuseClientBurstReadBytesSecs);

  throttle_.UpdateThrottleParams(params);

  int ret = bthread_timer_add(&throttle_timer_, butil::seconds_from_now(1),
                              OnThrottleTimer, this);
  if (ret != 0) {
    LOG(ERROR) << "Create vfs_old throttle timer failed!";
  }
}

static bool StartBrpcServer(brpc::Server& server, brpc::ServerOptions* options,
                            uint32_t start_port, uint32_t end_port,
                            uint32_t* listen_port) {
  static std::once_flag flag;
  std::call_once(flag, [&]() {
    while (start_port < end_port) {
      if (server.Start(start_port, options) == 0) {
        LOG(INFO) << "Start brpc server success, listen port = " << start_port;
        *listen_port = start_port;
        break;
      }

      ++start_port;
    }
  });

  if (start_port >= end_port) {
    LOG(ERROR) << "Start brpc server failed, start_port = " << start_port;
    return false;
  }

  return true;
}

int VFSOld::InitBrpcServer() {
  inode_object_service_.Init(s3_adapter_, inode_cache_manager_);

  if (server_.AddService(&inode_object_service_,
                         brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add InodeObjectsService";
    return -1;
  }

  brpc::ServerOptions brpc_server_options;
  if (common::FLAGS_bthread_worker_num > 0) {
    brpc_server_options.num_threads = common::FLAGS_bthread_worker_num;
  }

  uint32_t listen_port = 0;
  if (!StartBrpcServer(server_, &brpc_server_options,
                       fuse_client_option_.dummyServerStartPort, PORT_LIMIT,
                       &listen_port)) {
    LOG(ERROR) << "Start brpc server failed!";
    return -1;
  }

  std::string local_ip;
  if (!utils::NetCommon::GetLocalIP(&local_ip)) {
    LOG(ERROR) << "Get local ip failed!";
    return -1;
  }

  stub::common::ClientDummyServerInfo::GetInstance().SetPort(listen_port);
  stub::common::ClientDummyServerInfo::GetInstance().SetIP(local_ip);

  return 0;
}

Status VFSOld::Start(const VFSConfig& vfs_conf) {
  vfs_conf_ = vfs_conf;

  {
    // init mds client
    mds_base_ = std::make_shared<stub::rpcclient::MDSBaseClient>();

    mds_client_ = std::make_shared<stub::rpcclient::MdsClientImpl>();
    pb::mds::FSStatusCode ret =
        mds_client_->Init(fuse_client_option_.mdsOpt, mds_base_.get());
    if (ret != pb::mds::FSStatusCode::OK) {
      LOG(WARNING) << "mds_client_ init failed, FSStatusCode = " << ret
                   << ", FSStatusCode_Name = " << FSStatusCode_Name(ret);
      return Status::Internal("mds_client_ init failed");
    }
  }

  {
    // get fs info
    auto fs_info = std::make_shared<pb::mds::FsInfo>();
    pb::mds::FSStatusCode ret =
        mds_client_->GetFsInfo(vfs_conf_.fs_name, fs_info.get());
    if (ret != pb::mds::FSStatusCode::OK) {
      if (pb::mds::FSStatusCode::NOT_FOUND == ret) {
        LOG(ERROR) << "The fsName not exist, fsName = " << vfs_conf_.fs_name;
        return Status::NotExist("The fsName not exist");
      } else {
        LOG(ERROR) << "GetFsInfo failed, FSStatusCode = " << ret
                   << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                   << ", fsName = " << vfs_conf_.fs_name;
        return Status::Internal("GetFsInfo failed");
      }
    }

    // init fs info
    fs_info_ = fs_info;
  }

  auto cli2_client = std::make_shared<stub::rpcclient::Cli2ClientImpl>();
  auto meta_cache = std::make_shared<stub::rpcclient::MetaCache>();
  meta_cache->Init(fuse_client_option_.metaCacheOpt, cli2_client, mds_client_);
  auto channel_manager = std::make_shared<
      stub::rpcclient::ChannelManager<stub::common::MetaserverID>>();

  {
    // init metaserver client
    metaserver_client_ =
        std::make_shared<stub::rpcclient::MetaServerClientImpl>();

    pb::metaserver::MetaStatusCode ret2 = metaserver_client_->Init(
        fuse_client_option_.excutorOpt, fuse_client_option_.excutorInternalOpt,
        meta_cache, channel_manager);
    if (ret2 != pb::metaserver::MetaStatusCode::OK) {
      LOG(WARNING) << "metaserver_client_ init failed, MetaStatusCode = "
                   << ret2
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret2);
      return Status::Internal("metaserver_client_ init failed");
    }
  }

  {
    // init s3 adapter
    s3_adapter_ = std::make_shared<S3ClientAdaptorImpl>();
    s3_adapter_->SetFsId(fs_info_->fsid());
  }

  inode_cache_manager_ =
      std::make_shared<InodeCacheManagerImpl>(metaserver_client_);

  dentry_cache_manager_ =
      std::make_shared<DentryCacheManagerImpl>(metaserver_client_);

  {
    // init fs
    filesystem::ExternalMember member(dentry_cache_manager_,
                                      inode_cache_manager_, metaserver_client_,
                                      mds_client_);
    fs_ = std::make_shared<filesystem::FileSystem>(
        fs_info_->fsid(), fs_info_->fsname(),
        fuse_client_option_.fileSystemOption, member);
  }

  {
    // init inode cache manager
    auto member = fs_->BorrowMember();
    DINGOFS_ERROR rc =
        inode_cache_manager_->Init(fuse_client_option_.refreshDataOption,
                                   member.openFiles, member.deferSync);
    if (rc != DINGOFS_ERROR::OK) {
      LOG(WARNING) << "inode_cache_manager_ init failed, DINGOFS_ERROR = "
                   << rc;
      return Status::Internal("inode_cache_manager_ init failed");
    }
  }

  InitQosParam();

  // init brpc server
  if (InitBrpcServer() != 0) {
    LOG(ERROR) << "InitBrpcServer failed";
    return Status::Internal("InitBrpcServer failed");
  }

  {
    // init mount point
    pb::mds::Mountpoint mount_point;
    mount_point.set_path(vfs_conf_.mount_point);
    mount_point.set_cto(common::FLAGS_enableCto);

    int ret = SetHostPortInMountPoint(mount_point);
    if (ret != 0) {
      LOG(ERROR) << "Set Host and Port in MountPoint failed, ret = " << ret;
      return Status::Internal("Set Host and Port in MountPoint failed");
    }

    mount_point_ = mount_point;
  }

  {
    // mount in mds
    pb::mds::FSStatusCode mount_ret =
        mds_client_->MountFs(vfs_conf_.fs_name, mount_point_, fs_info_.get());
    // NOTE: why check MOUNT_POINT_EXIST ?
    if (mount_ret != pb::mds::FSStatusCode::OK &&
        mount_ret != pb::mds::FSStatusCode::MOUNT_POINT_EXIST) {
      LOG(ERROR) << "MountFs failed, FSStatusCode = " << mount_ret
                 << ", FSStatusCode_Name = "
                 << pb::mds::FSStatusCode_Name(mount_ret)
                 << ", fsName = " << vfs_conf_.fs_name
                 << ", mountPoint = " << mount_point_.ShortDebugString();
      return Status::Internal("MountFs failed");
    }

    LOG(INFO) << "MountFs success, fsName = " << vfs_conf_.fs_name
              << ", mountPoint = " << mount_point_.ShortDebugString();
  }

  inode_cache_manager_->SetFsId(fs_info_->fsid());
  dentry_cache_manager_->SetFsId(fs_info_->fsid());

  {
    lease_executor_ = absl::make_unique<LeaseExecutor>(
        fuse_client_option_.leaseOpt, meta_cache, mds_client_,
        &enable_sum_in_dir_);
    lease_executor_->SetFsName(vfs_conf.fs_name);
    lease_executor_->SetMountPoint(mount_point_);
    if (!lease_executor_->Start()) {
      LOG(ERROR) << "lease_executor_ start failed";
      return Status::Internal("lease_executor_ start failed");
    }
  }

  {
    // fill fuse client option s3 option use fs s3 info
    const auto& s3info = fs_info_->detail().s3info();
    dataaccess::aws::S3InfoOption fs_s3_option;
    common::S3Info2FsS3Option(s3info, &fs_s3_option);
    SetClientS3Option(&fuse_client_option_, fs_s3_option);

    // init blockcache s3 client
    data_accesser_ =
        dataaccess::S3Accesser::New(fuse_client_option_.s3Opt.s3AdaptrOpt);
    data_accesser_->Init();
  }

  // data stream
  if (!datastream::DataStream::GetInstance().Init(
          fuse_client_option_.data_stream_option)) {
    return Status::Internal("datastream init failed");
  }

  {
    auto page_option = fuse_client_option_.data_stream_option.page_option;
    auto max_memory_size = page_option.total_size;
    auto fs_cache_manager = std::make_shared<FsCacheManager>(
        dynamic_cast<S3ClientAdaptorImpl*>(s3_adapter_.get()),
        fuse_client_option_.s3Opt.s3ClientAdaptorOpt.readCacheMaxByte,
        max_memory_size,
        fuse_client_option_.s3Opt.s3ClientAdaptorOpt.readCacheThreads, nullptr);

    // block cache
    auto block_cache_option = fuse_client_option_.block_cache_option;
    std::string uuid =
        absl::StrFormat("%d-%s", fs_info_->fsid(), fs_info_->fsname());
    if (fs_info_->has_uuid()) {
      uuid = fs_info_->uuid();
    }
    RewriteCacheDir(&block_cache_option, uuid);
    auto block_cache =
        std::make_shared<BlockCacheImpl>(block_cache_option, data_accesser_);

    if (s3_adapter_->Init(fuse_client_option_.s3Opt.s3ClientAdaptorOpt,
                          data_accesser_, inode_cache_manager_, mds_client_,
                          fs_cache_manager, fs_, block_cache, nullptr,
                          true) != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "s3_adapter_ init failed";
      return Status::Internal("s3_adapter_ init failed");
    }
  }

  {
    warmup_manager_ = std::make_shared<warmup::WarmupManagerS3Impl>(
        metaserver_client_, inode_cache_manager_, dentry_cache_manager_,
        fs_info_, s3_adapter_, nullptr, this, data_accesser_);
    warmup_manager_->Init(fuse_client_option_);
    warmup_manager_->SetFsInfo(fs_info_);
    warmup_manager_->SetMounted(true);
  }

  fs_->Run();

  started_.store(true);

  return Status::OK();
}

Status VFSOld::Stop() {
  if (!started_.load()) {
    return Status::OK();
  }

  started_.store(false);

  {
    // flush all data
    DINGOFS_ERROR ret = DINGOFS_ERROR::UNKNOWN;
    do {
      ret = s3_adapter_->FsSync();
      if (ret != DINGOFS_ERROR::OK) {
        LOG(INFO) << "Fail FsSync in stop, ret = " << ret;
      }
    } while (ret != DINGOFS_ERROR::OK);
  }

  fs_->Destory();

  // stop lease before umount fs, otherwise, lease request after umount fs
  // will add a mountpoint entry.
  lease_executor_.reset();

  {
    // umount fs
    std::string fs_name = fs_info_->fsname();
    LOG(INFO) << "Umount " << fs_name << " on "
              << mount_point_.ShortDebugString() << " start";
    pb::mds::FSStatusCode ret = mds_client_->UmountFs(fs_name, mount_point_);

    if (ret != pb::mds::FSStatusCode::OK &&
        ret != pb::mds::FSStatusCode::MOUNT_POINT_NOT_EXIST) {
      LOG(ERROR) << "Fail UmountFs FSStatusCode = " << ret
                 << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                 << ", fsName = " << fs_name
                 << ", mountPoint = " << mount_point_.ShortDebugString();
    } else {
      LOG(INFO) << "Umount " << fs_name << " on "
                << mount_point_.ShortDebugString() << " success!";
    }
  }

  warmup_manager_->UnInit();

  mds_client_.reset();
  mds_base_.reset();

  while (bthread_timer_del(throttle_timer_) != 0) {
    bthread_usleep(1000);
  }

  s3_adapter_->Stop();
  data_accesser_->Destroy();
  datastream::DataStream::GetInstance().Shutdown();

  return Status::OK();
}

double VFSOld::GetAttrTimeout(const FileType& type) {
  if (type == FileType::kDirectory) {
    return fuse_client_option_.fileSystemOption.kernelCacheOption
        .dirAttrTimeoutSec;
  } else {
    return fuse_client_option_.fileSystemOption.kernelCacheOption
        .attrTimeoutSec;
  }
}

double VFSOld::GetEntryTimeout(const FileType& type) {
  if (type == FileType::kDirectory) {
    return fuse_client_option_.fileSystemOption.kernelCacheOption
        .dirEntryTimeoutSec;
  } else {
    return fuse_client_option_.fileSystemOption.kernelCacheOption
        .entryTimeoutSec;
  }
}

Status VFSOld::Lookup(Ino parent, const std::string& name, Attr* attr) {
  VLOG(1) << "Lookup parent inodeId=" << parent << ", name: " << name;
  // check if parent is root inode and file name is .stats name
  if (BAIDU_UNLIKELY(parent == ROOTINODEID &&
                     name == STATSNAME)) {  // stats node
    *attr = GenerateVirtualInodeAttr(STATSINODEID);
    return Status::OK();
  }

  DINGOFS_ERROR rc = DINGOFS_ERROR::OK;

  filesystem::EntryOut entry_out;

  auto defer = absl::MakeCleanup([&]() {
    if (rc == DINGOFS_ERROR::OK) {
      fs_->BeforeReplyEntry(entry_out.attr);
    }
  });

  rc = fs_->Lookup(parent, name, &entry_out);
  if (rc == DINGOFS_ERROR::OK) {
    *attr = InodeAttrPBToAttr(entry_out.attr);
    auto entry_watcher = fs_->BorrowMember().entry_watcher;
    entry_watcher->Remeber(entry_out.attr, name);
  } else if (rc == DINGOFS_ERROR::NOTEXIST) {
    // do nothing
  } else {
    LOG(ERROR) << "Lookup() failed, retCode = " << rc << ", parent = " << parent
               << ", name = " << name;
  }

  return filesystem::DingofsErrorToStatus(rc);
}

Status VFSOld::GetAttr(Ino ino, Attr* attr) {
  VLOG(1) << "GetAttr inodeId=" << ino;

  if BAIDU_UNLIKELY ((ino == STATSINODEID)) {
    *attr = GenerateVirtualInodeAttr(STATSINODEID);
    return Status::OK();
  }

  filesystem::AttrOut attr_out;
  DINGOFS_ERROR rc = fs_->GetAttr(ino, &attr_out);
  if (rc != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "getattr() fail, retCode = " << rc << ", inodeId=" << ino;
  } else {
    fs_->BeforeReplyAttr(attr_out.attr);
    *attr = InodeAttrPBToAttr(attr_out.attr);
  }

  return filesystem::DingofsErrorToStatus(rc);
}

// NOTE: inode lock should be acqurire before calling this function
Status VFSOld::Truncate(InodeWrapper* inode, uint64_t length) {
  pb::metaserver::InodeAttr attr;
  inode->GetInodeAttrUnLocked(&attr);
  int64_t change_size = length - static_cast<int64_t>(attr.length());

  if (change_size > 0) {
    if (!fs_->CheckFsQuota(change_size, 0)) {
      return Status::NoSpace("checkfs quota fail");
    }

    for (int i = 0; i < attr.parent_size(); i++) {
      auto parent = attr.parent(i);
      if (!fs_->CheckDirQuota(parent, change_size, 0)) {
        return Status::NoSpace("checkdir quota fail");
      }
    }
  }

  DINGOFS_ERROR rc = s3_adapter_->Truncate(inode, length);

  if (rc == DINGOFS_ERROR::OK) {
    for (int i = 0; i < attr.parent_size(); i++) {
      auto parent = attr.parent(i);
      fs_->UpdateDirQuotaUsage(parent, change_size, 0);
    }

    fs_->UpdateFsQuotaUsage(change_size, 0);
  }

  return filesystem::DingofsErrorToStatus(rc);
}

Status VFSOld::SetAttr(Ino ino, int set, const Attr& in_attr, Attr* out_attr) {
  VLOG(1) << "SetAttr inodeId=" << ino << ", set: " << set
          << ", attr: " << Attr2Str(in_attr);

  if BAIDU_UNLIKELY ((ino == STATSINODEID)) {
    *out_attr = GenerateVirtualInodeAttr(STATSINODEID);
    return Status::OK();
  }
  DINGOFS_ERROR ret = DINGOFS_ERROR::OK;
  pb::metaserver::InodeAttr attr;

  auto defer = ::absl::MakeCleanup([&]() {
    if (ret == DINGOFS_ERROR::OK) {
      fs_->BeforeReplyAttr(attr);
    }
  });

  std::shared_ptr<InodeWrapper> inode_wrapper;
  ret = inode_cache_manager_->GetInode(ino, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Fail get inode in SetAttr, ret = " << ret
               << ", inodeId=" << ino;
    return filesystem::DingofsErrorToStatus(ret);
  }

  utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();
  if (set & kSetAttrMode) {
    inode_wrapper->SetMode(in_attr.mode);
  }

  if (set & kSetAttrUid) {
    inode_wrapper->SetUid(in_attr.uid);
  }

  if (set & kSetAttrGid) {
    inode_wrapper->SetGid(in_attr.gid);
  }

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);

  if (set & kSetAttrAtime) {
    struct timespec a_time;
    ToTimeSpec(in_attr.atime, &a_time);

    inode_wrapper->UpdateTimestampLocked(a_time, kAccessTime);
  }

  if (set & kSetAttrAtimeNow) {
    inode_wrapper->UpdateTimestampLocked(now, kAccessTime);
  }

  if (set & kSetAttrMtime) {
    struct timespec m_time;
    ToTimeSpec(in_attr.mtime, &m_time);

    inode_wrapper->UpdateTimestampLocked(m_time, kModifyTime);
  }

  if (set & kSetAttrMtimeNow) {
    inode_wrapper->UpdateTimestampLocked(now, kModifyTime);
  }

  if (set & kSetAttrCtime) {
    struct timespec c_time;
    ToTimeSpec(in_attr.ctime, &c_time);
    inode_wrapper->UpdateTimestampLocked(c_time, kChangeTime);
  } else {
    inode_wrapper->UpdateTimestampLocked(now, kChangeTime);
  }

  if (set & kSetAttrSize) {
    Status s = Truncate(inode_wrapper.get(), in_attr.length);
    if (!s.ok()) {
      LOG(ERROR) << "truncate file fail, ret = " << ret << ", inodeId=" << ino;
      return s;
    }

    inode_wrapper->SetLengthLocked(in_attr.length);
    ret = inode_wrapper->Sync();
    if (ret != DINGOFS_ERROR::OK) {
      return filesystem::DingofsErrorToStatus(ret);
    }

    inode_wrapper->GetInodeAttrUnLocked(&attr);

    *out_attr = InodeAttrPBToAttr(attr);
    return Status::OK();
  }

  ret = inode_wrapper->SyncAttr();
  if (ret != DINGOFS_ERROR::OK) {
    return filesystem::DingofsErrorToStatus(ret);
  }

  inode_wrapper->GetInodeAttrUnLocked(&attr);

  *out_attr = InodeAttrPBToAttr(attr);
  return Status::OK();
}

Status VFSOld::ReadLink(Ino ino, std::string* link) {
  VLOG(1) << "ReadLink inodeId=" << ino;

  pb::metaserver::InodeAttr attr;
  DINGOFS_ERROR rc = inode_cache_manager_->GetInodeAttr(ino, &attr);
  if (rc != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Failed GetInodeAttr()  rc: " << rc << ", inodeId=" << ino;
    return filesystem::DingofsErrorToStatus(rc);
  }

  *link = attr.symlink();

  return Status::OK();
}

DINGOFS_ERROR VFSOld::UpdateParentMCTime(Ino parent) {
  std::shared_ptr<InodeWrapper> inode_wrapper;
  auto ret = inode_cache_manager_->GetInode(parent, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Fail get parent inode ret = " << ret
               << ", parent inodeId=" << parent;
    return ret;
  }

  {
    dingofs::utils::UniqueLock lk = inode_wrapper->GetUniqueLock();
    inode_wrapper->UpdateTimestampLocked(kModifyTime | kChangeTime);

    if (fuse_client_option_.fileSystemOption.deferSyncOption.deferDirMtime) {
      inode_cache_manager_->ShipToFlush(inode_wrapper);
    } else {
      return inode_wrapper->SyncAttr();
    }
  }

  VLOG(6) << "Success update parent mtime and ctime, parent inodeId=" << parent;

  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR VFSOld::UpdateParentMCTimeAndNlink(Ino parent,
                                                 common::NlinkChange nlink) {
  std::shared_ptr<InodeWrapper> inode_wrapper;
  auto ret = inode_cache_manager_->GetInode(parent, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Fail get parent inode ret = " << ret
               << ", parent inodeId=" << parent;
    return ret;
  }

  {
    dingofs::utils::UniqueLock lk = inode_wrapper->GetUniqueLock();
    inode_wrapper->UpdateTimestampLocked(kModifyTime | kChangeTime);
    inode_wrapper->UpdateNlinkLocked(nlink);

    if (fuse_client_option_.fileSystemOption.deferSyncOption.deferDirMtime) {
      inode_cache_manager_->ShipToFlush(inode_wrapper);
    } else {
      return inode_wrapper->SyncAttr();
    }
  }

  VLOG(6) << "Success update parent attr in mknode, parent inodeId=" << parent;

  return DINGOFS_ERROR::OK;
}

Status VFSOld::AllocNode(Ino parent, const std::string& name, uint32_t uid,
                         uint32_t gid, pb::metaserver::FsFileType type,
                         uint32_t mode, uint64_t dev, std::string path,
                         std::shared_ptr<InodeWrapper>* out_inode_wrapper) {
  VLOG(1) << "AllocNode parent inodeId=" << parent << ", name: " << name
          << ", uid: " << uid << ", gid: " << gid
          << ", type: " << pb::metaserver::FsFileType_Name(type)
          << ", mode: " << mode << ", dev: " << dev;
  {
    // precheck
    if (name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
      LOG(WARNING) << "name too long, name: " << name << ", maxNameLength: "
                   << fuse_client_option_.fileSystemOption.maxNameLength;
      return Status::NameTooLong(
          fmt::format("name too long, length: {}", name.length()));
    }

    // check if node is recycle or under recycle or .stats node
    if (IsInternalName(name) && parent == ROOTINODEID) {
      LOG(WARNING) << "Can not make internal node, parent inodeId=" << parent
                   << ", name: " << name;
      return Status::NoPermitted("Can not make internal node");
    }

    if (parent == RECYCLEINODEID) {
      LOG(WARNING) << "Can not make node under recycle.";
      return Status::NoPermitted("Can not make node under recycle");
    }
  }

  if (!fs_->CheckQuota(parent, 0, 1)) {
    return Status::NoSpace("check quota fail");
  }

  std::shared_ptr<InodeWrapper> inode_wrapper;
  {
    // create inode
    stub::rpcclient::InodeParam param;
    param.fsId = fs_info_->fsid();
    param.uid = uid;
    param.gid = gid;
    param.mode = mode;
    param.rdev = dev;
    param.parent = parent;
    param.type = type;

    if (pb::metaserver::FsFileType::TYPE_DIRECTORY == type) {
      param.length = 4096;
    } else if (pb::metaserver::FsFileType::TYPE_S3 == type) {
      param.length = 0;
    } else if (pb::metaserver::FsFileType::TYPE_SYM_LINK == type) {
      param.length = 0;
      param.symlink = path;
    } else {
      CHECK(false) << "Invalid type: " << type;
    }

    DINGOFS_ERROR ret = inode_cache_manager_->CreateInode(param, inode_wrapper);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail CreateInode in AllocNode, ret = " << ret
                 << ", parent inodeId=" << parent << ", name: " << name
                 << ", mode: " << mode
                 << ", type: " << pb::metaserver::FsFileType_Name(type);

      return filesystem::DingofsErrorToStatus(ret);
    }

    VLOG(6) << "Success AllocNode, parent inodeId=" << parent
            << ", name: " << name << ", mode: " << mode << ", dev: " << dev
            << ", inodeId=" << inode_wrapper->GetInodeId()
            << ", type: " << pb::metaserver::FsFileType_Name(type);
  }

  {
    //  create dentry for inode
    pb::metaserver::Dentry dentry;
    dentry.set_fsid(fs_info_->fsid());
    dentry.set_inodeid(inode_wrapper->GetInodeId());
    dentry.set_parentinodeid(parent);
    dentry.set_name(name);
    dentry.set_type(inode_wrapper->GetType());

    if (pb::metaserver::FsFileType::TYPE_S3 == type) {
      dentry.set_flag(pb::metaserver::DentryFlag::TYPE_FILE_FLAG);
    }

    DINGOFS_ERROR ret = dentry_cache_manager_->CreateDentry(dentry);

    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail CreateDentry rc: " << ret
                 << ", parent inodeId=" << parent << ", name: " << name
                 << ", mode: " << mode << ", dev: " << dev
                 << ", type: " << pb::metaserver::FsFileType_Name(type);

      DINGOFS_ERROR ret2 =
          inode_cache_manager_->DeleteInode(inode_wrapper->GetInodeId());
      if (ret2 != DINGOFS_ERROR::OK) {
        LOG(ERROR) << "Fail delete inode after CreateDentry fail , ret = "
                   << ret2 << ", inodeId=" << inode_wrapper->GetInodeId();
      }

      return filesystem::DingofsErrorToStatus(ret);
    }

    VLOG(6) << "Success create dentry, parent inodeId=" << parent
            << ", name: " << name << ", mode: " << mode << ", dev: " << dev
            << ", type: " << pb::metaserver::FsFileType_Name(type);
  }

  // update parent mtime and ctime
  if (pb::metaserver::FsFileType::TYPE_DIRECTORY == type) {
    DINGOFS_ERROR ret =
        UpdateParentMCTimeAndNlink(parent, common::NlinkChange::kAddOne);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail UpdateParentMCTimeAndNlink parent inodeId=" << parent
                 << ", name: " << name << ", mode: " << mode << ", dev: " << dev
                 << ", type: " << pb::metaserver::FsFileType_Name(type);

      return filesystem::DingofsErrorToStatus(ret);
    }
  } else {
    DINGOFS_ERROR ret = UpdateParentMCTime(parent);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail UpdateParentMCTimeAndNlink parent inodeId=" << parent
                 << ", name: " << name << ", mode: " << mode << ", dev: " << dev
                 << ", type: " << pb::metaserver::FsFileType_Name(type);

      return filesystem::DingofsErrorToStatus(ret);
    }
  }

  // NOTE: if update parent attr fail, maybe consistence
  fs_->UpdateFsQuotaUsage(0, 1);
  fs_->UpdateDirQuotaUsage(parent, 0, 1);

  *out_inode_wrapper = inode_wrapper;

  VLOG(1) << "Success AllocNode inodeId=" << inode_wrapper->GetInodeId()
          << ", parent inodeId=" << parent << ", name: " << name
          << ", mode: " << mode << ", dev: " << dev
          << ", type: " << pb::metaserver::FsFileType_Name(type);
  return Status::OK();
}

Status VFSOld::MkNod(Ino parent, const std::string& name, uint32_t uid,
                     uint32_t gid, uint32_t mode, uint64_t dev, Attr* attr) {
  VLOG(1) << "Mknod parent inodeId=" << parent << ", name: " << name
          << ", uid: " << uid << ", gid: " << gid << ", mode: " << mode
          << ", dev: " << dev;

  std::shared_ptr<InodeWrapper> inode_wrapper;
  Status s =
      AllocNode(parent, name, uid, gid, pb::metaserver::FsFileType::TYPE_S3,
                mode, dev, "", &inode_wrapper);
  if (!s.ok()) {
    LOG(WARNING) << "Fail AllocNode in MkNod, parent inodeId=" << parent
                 << ", name: " << name << ", mode: " << mode << ", dev: " << dev
                 << ", status: " << s.ToString();
    return s;
  }

  pb::metaserver::InodeAttr inode_attr;
  inode_wrapper->GetInodeAttr(&inode_attr);

  auto entry_watcher = fs_->BorrowMember().entry_watcher;
  entry_watcher->Remeber(inode_attr, name);

  fs_->BeforeReplyAttr(inode_attr);

  *attr = InodeAttrPBToAttr(inode_attr);
  return Status::OK();
}

Status VFSOld::Unlink(Ino parent, const std::string& name) {
  VLOG(1) << "Unlink parent inodeId=" << parent << ", name: " << name;

  if (name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
    LOG(WARNING) << "name too long, name: " << name << ", maxNameLength: "
                 << fuse_client_option_.fileSystemOption.maxNameLength;
    return Status::NameTooLong(
        fmt::format("name too long, length: {}", name.length()));
  }

  // check if node is recycle or recycle time dir or .stats node
  if ((IsInternalName(name) && parent == ROOTINODEID) ||
      parent == RECYCLEINODEID) {
    LOG(WARNING) << "Can not unlink internal node, parent inodeId=" << parent
                 << ", name: " << name;
    return Status::NoPermitted("Can not unlink internal node");
  }

  pb::metaserver::Dentry dentry;
  DINGOFS_ERROR ret = dentry_cache_manager_->GetDentry(parent, name, &dentry);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(WARNING) << "Fail GetDentry , ret = " << ret
                 << ", parent inodeId=" << parent << ", name = " << name;
    return filesystem::DingofsErrorToStatus(ret);
  }

  Ino inode_id = dentry.inodeid();
  // NOTE: recycle logic is removed

  {
    // 1. delete dentry
    DINGOFS_ERROR ret = dentry_cache_manager_->DeleteDentry(
        parent, name, pb::metaserver::FsFileType::TYPE_S3);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail DeleteDentry , ret = " << ret
                 << ", parent inodeId=" << parent << ", name = " << name;
      return filesystem::DingofsErrorToStatus(ret);
    }

    // 2. update parent mtime and ctime
    ret = UpdateParentMCTime(parent);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail UpdateParentMCTime"
                 << ", parent: " << parent << ", name: " << name;
      return filesystem::DingofsErrorToStatus(ret);
    }

    // 3. update inode
    std::shared_ptr<InodeWrapper> inode_wrapper;
    ret = inode_cache_manager_->GetInode(inode_id, inode_wrapper);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail get inode in unlink, ret = " << ret
                 << ", inodeId=" << inode_id;
      return filesystem::DingofsErrorToStatus(ret);
    }

    uint32_t new_links = UINT32_MAX;
    ret = inode_wrapper->UnLinkWithReturn(parent, new_links);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail UnLinkWithReturn ret = " << ret
                 << ", inodeId=" << inode_id << ", parent inodeId=" << parent
                 << ", name: " << name;

      return filesystem::DingofsErrorToStatus(ret);
    }

    // 4. update quota
    int64_t add_space = -(inode_wrapper->GetLength());
    // sym link we not add space
    if (inode_wrapper->GetType() == pb::metaserver::FsFileType::TYPE_SYM_LINK) {
      add_space = 0;
    }

    fs_->UpdateDirQuotaUsage(parent, add_space, -1);

    if (new_links == 0) {
      fs_->UpdateFsQuotaUsage(add_space, -1);
    }
  }

  return Status::OK();
}

Status VFSOld::Symlink(Ino parent, const std::string& name, uint32_t uid,
                       uint32_t gid, const std::string& link, Attr* attr) {
  {
    // internal file name can not allowed for symlink

    // cant't allow  ln -s <file> .stats
    if (parent == ROOTINODEID && IsInternalName(name)) {
      LOG(WARNING) << "Can not symlink internal node, parent inodeId=" << parent
                   << ", name: " << name;
      return Status::NoPermitted("Can not symlink internal node");
    }

    // cant't allow  ln -s  .stats  <file>
    if (parent == ROOTINODEID && IsInternalName(link)) {
      LOG(WARNING) << "Can not symlink to internal node, parent inodeId="
                   << parent << ", link: " << link;
      return Status::NoPermitted("Can not symlink to internal node");
    }
  }

  Status s;
  pb::metaserver::InodeAttr inode_attr;

  auto defer = ::absl::MakeCleanup([&]() {
    if (s.ok()) {
      fs_->BeforeReplyEntry(inode_attr);
    }
  });

  std::shared_ptr<InodeWrapper> inode_wrapper;
  s = AllocNode(parent, name, uid, gid,
                pb::metaserver::FsFileType::TYPE_SYM_LINK, (S_IFLNK | 0777), 0,
                link, &inode_wrapper);
  if (!s.ok()) {
    LOG(WARNING) << "Fail AllocNode in Symlink, parent inodeId=" << parent
                 << ", name: " << name << ", link: " << link
                 << ", status: " << s.ToString();
    return s;
  }

  inode_wrapper->GetInodeAttr(&inode_attr);

  *attr = InodeAttrPBToAttr(inode_attr);
  return Status::OK();
}

Status VFSOld::Rename(Ino old_parent, const std::string& old_name,
                      Ino new_parent, const std::string& new_name) {
  VLOG(1) << "Rename from (" << old_parent << ", " << old_name << ") to ("
          << new_parent << ", " << new_name << ")";

  // internel name can not be rename or rename to
  if ((IsInternalName(old_name) || IsInternalName(new_name)) &&
      old_parent == ROOTINODEID) {
    return Status::NoPermitted("Can not rename internal node");
  }

  uint64_t max_name_length = fuse_client_option_.fileSystemOption.maxNameLength;
  if (old_name.length() > max_name_length ||
      new_name.length() > max_name_length) {
    LOG(WARNING) << "Rename name too long, name = " << old_name
                 << ", name len = " << old_name.length()
                 << ", new name = " << new_name
                 << ", new name len = " << new_name.length()
                 << ", maxNameLength = " << max_name_length;
    return Status::NameTooLong(
        fmt::format("name too long, old_name length: {}, new_name_length: {}",
                    old_name.length(), new_name.length()));
  }

  if (old_parent != new_parent) {
    pb::metaserver::Dentry entry;
    auto rc = dentry_cache_manager_->GetDentry(old_parent, old_name, &entry);
    if (rc != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail GetDentry ret = " << rc
                 << ", old_parent inodeId=" << old_parent
                 << ", dentry name: " << old_name;
      return filesystem::DingofsErrorToStatus(rc);
    }

    pb::metaserver::InodeAttr attr;
    rc = inode_cache_manager_->GetInodeAttr(entry.inodeid(), &attr);
    if (rc != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail GetInodeAttr  ret = " << rc
                 << ", old_parent inodeId=" << old_parent
                 << ", entry inodeId=" << entry.inodeid()
                 << ", entry name: " << old_name;
      return filesystem::DingofsErrorToStatus(rc);
    }

    if (attr.type() == pb::metaserver::FsFileType::TYPE_DIRECTORY) {
      // TODO : remove this restrict when we support rename dir in dirfferent
      // quota  dir
      Ino parent_nearest_quota_ino = 0;
      bool parent_has =
          fs_->NearestDirQuota(old_parent, parent_nearest_quota_ino);

      Ino newparent_nearest_quota_ino = 0;
      bool newparent_has =
          fs_->NearestDirQuota(new_parent, newparent_nearest_quota_ino);

      bool can_rename =
          (!parent_has && !newparent_has) ||
          (newparent_has && parent_has &&
           parent_nearest_quota_ino == newparent_nearest_quota_ino);

      if (!can_rename) {
        LOG(WARNING) << "Rename not support rename dir between quota dir "
                     << ", name: " << old_name << ", parent: " << old_parent
                     << ", parent_has: " << (parent_has ? "true" : "false")
                     << ", parent_nearest_quota_ino: "
                     << parent_nearest_quota_ino
                     << ", newparent: " << new_parent << ", newparent_has: "
                     << (newparent_has ? "true" : "false")
                     << ", newparent_nearest_quota_ino: "
                     << newparent_nearest_quota_ino;

        return Status::NotSupport("Not support rename dir between quota dir");
      }
    }
  }

  auto rename_operator = RenameOperator(
      fs_info_->fsid(), fs_info_->fsname(), old_parent, old_name, new_parent,
      new_name, dentry_cache_manager_, inode_cache_manager_, metaserver_client_,
      mds_client_, fuse_client_option_.enableMultiMountPointRename);

  dingofs::utils::LockGuard lg(rename_mutex_);
  DINGOFS_ERROR rc = DINGOFS_ERROR::OK;
  VLOG(3) << "Rename [start]: " << rename_operator.DebugString();

  RETURN_IF_UNSUCCESS(Precheck);
  RETURN_IF_UNSUCCESS(RecordSrcInodeInfo);
  rename_operator.UpdateSrcDirUsage(fs_);
  if (!rename_operator.CheckNewParentQuota(fs_)) {
    rename_operator.RollbackUpdateSrcDirUsage(fs_);
    return Status::NoSpace("check new parent quota fail");
  }

  RETURN_IF_UNSUCCESS(GetTxId);
  RETURN_IF_UNSUCCESS(RecordOldInodeInfo);

  // Do not move LinkDestParentInode behind CommitTx.
  // If so, the nlink will be lost when the machine goes down
  RETURN_IF_UNSUCCESS(LinkDestParentInode);
  RETURN_IF_UNSUCCESS(PrepareTx);
  RETURN_IF_UNSUCCESS(CommitTx);
  VLOG(3) << "FuseOpRename [success]: " << rename_operator.DebugString();
  // Do not check UnlinkSrcParentInode, beause rename is already success
  rename_operator.UnlinkSrcParentInode();
  rename_operator.UnlinkOldInode();
  if (old_parent != new_parent) {
    rename_operator.UpdateInodeParent();
  }
  rename_operator.UpdateInodeCtime();
  rename_operator.UpdateCache();

  rename_operator.FinishUpdateUsage(fs_);

  // careful about the rc
  return filesystem::DingofsErrorToStatus(rc);
}

Status VFSOld::Link(Ino ino, Ino new_parent, const std::string& new_name,
                    Attr* attr) {
  VLOG(1) << "Link inodeId=" << ino << ", new_parent: " << new_parent
          << ", new_name: " << new_name;

  if (new_name.length() > fuse_client_option_.fileSystemOption.maxNameLength) {
    LOG(WARNING) << "name too long, name: " << new_name << ", maxNameLength: "
                 << fuse_client_option_.fileSystemOption.maxNameLength;
    return Status::NameTooLong("name too long, length: " +
                               std::to_string(new_name.length()));
  }

  {
    // cant't allow  ln   <file> .stats
    // cant't allow  ln  .stats  <file>
    if (IsInternalNode(ino) ||
        (new_parent == ROOTINODEID && IsInternalName(new_name))) {
      return Status::NoPermitted("Can not link internal node");
    }
  }

  std::shared_ptr<InodeWrapper> inode_wrapper;
  DINGOFS_ERROR ret = inode_cache_manager_->GetInode(ino, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
               << ", inodeId=" << ino;
    return filesystem::DingofsErrorToStatus(ret);
  }

  // for hard link, we only check dir quota
  if (!fs_->CheckDirQuota(new_parent, inode_wrapper->GetLength(), 1)) {
    return Status::NoSpace("check dir quota fail");
  }

  //  change inode attr ctime/nlink/parent
  ret = inode_wrapper->Link(new_parent);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Link Inode fail, ret = " << ret << ", inodeId=" << ino
               << ", newparent = " << new_parent << ", newname = " << new_name;
    return filesystem::DingofsErrorToStatus(ret);
  }

  {
    // create dentry for new_name
    pb::metaserver::Dentry dentry;
    dentry.set_fsid(fs_info_->fsid());
    dentry.set_inodeid(inode_wrapper->GetInodeId());
    dentry.set_parentinodeid(new_parent);
    dentry.set_name(new_name);
    dentry.set_type(inode_wrapper->GetType());
    ret = dentry_cache_manager_->CreateDentry(dentry);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail CreateDentry for new_name: " << new_name
                 << ", in new_parent inodeId=" << new_parent
                 << ", inodeId=" << ino << ", ret = " << ret;

      // rollback link inode
      DINGOFS_ERROR ret2 = inode_wrapper->UnLink(new_parent);
      if (ret2 != DINGOFS_ERROR::OK) {
        LOG(ERROR) << "Also unlink inode failed, ret = " << ret2
                   << ", inodeId=" << inode_wrapper->GetInodeId();
      }

      return filesystem::DingofsErrorToStatus(ret);
    }
  }

  // update new_parent mtime and ctime
  ret = UpdateParentMCTime(new_parent);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR)
        << "Fail UpdateParentMCTimeAndNlink in rename, new_parent inodeId="
        << new_parent << ", new_name: " << new_name << ", inodeId=" << ino
        << ", ret = " << ret;
    return filesystem::DingofsErrorToStatus(ret);
  }

  auto entry_watcher = fs_->BorrowMember().entry_watcher;
  entry_watcher->Forget(ino);

  pb::metaserver::InodeAttr inode_attr;
  inode_wrapper->GetInodeAttr(&inode_attr);

  fs_->BeforeReplyEntry(inode_attr);

  *attr = InodeAttrPBToAttr(inode_attr);

  fs_->UpdateDirQuotaUsage(new_parent, inode_attr.length(), 1);

  return Status::OK();
}

Status VFSOld::HandleOpenFlags(Ino ino, int flags) {
  std::shared_ptr<InodeWrapper> inode_wrapper;
  // alredy opened
  DINGOFS_ERROR ret = inode_cache_manager_->GetInode(ino, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Failed get inode, ret: " << ret << ", inodeId=" << ino;

    return filesystem::DingofsErrorToStatus(ret);
  }

  pb::metaserver::InodeAttr inode_attr;
  inode_wrapper->GetInodeAttr(&inode_attr);

  if (flags & O_TRUNC) {
    if (flags & O_WRONLY || flags & O_RDWR) {
      VLOG(1) << "HandleOpenFlags, truncate file, inodeId=" << ino;

      dingofs::utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();
      uint64_t old_length = inode_wrapper->GetLengthLocked();
      Status s = Truncate(inode_wrapper.get(), 0);
      if (!s.ok()) {
        LOG(ERROR) << "Fail truncate file, inodeId=" << ino
                   << ", status: " << s.ToString();
        return s;
      }

      inode_wrapper->SetLengthLocked(0);
      inode_wrapper->UpdateTimestampLocked(kChangeTime | kModifyTime);
      if (old_length != 0) {
        ret = inode_wrapper->Sync();
        if (ret != DINGOFS_ERROR::OK) {
          return filesystem::DingofsErrorToStatus(ret);
        }
      } else {
        inode_wrapper->MarkDirty();
      }

      inode_wrapper->GetInodeAttrUnLocked(&inode_attr);
    } else {
      return Status::NoPermission("O_TRUNC without O_WRONLY or O_RDWR");
    }
  }

  fs_->BeforeReplyOpen(inode_attr);
  return Status::OK();
}

Status VFSOld::Open(Ino ino, int flags, uint64_t* fh) {
  VLOG(1) << "Open inodeId=" << ino << " Octal flags: " << std::oct << flags;

  // check if ino is .stats inode,if true ,get metric data and generate
  // inodeattr information
  if (BAIDU_UNLIKELY(ino == STATSINODEID)) {
    auto handler = fs_->NewHandler();
    *fh = handler->fh;

    MetricsDumper metrics_dumper;
    bvar::DumpOptions opts;
    int ret = bvar::Variable::dump_exposed(&metrics_dumper, &opts);
    std::string contents = metrics_dumper.Contents();

    size_t len = contents.size();
    if (len == 0) {
      return Status::NoData("No data in .stats");
    }

    handler->buffer->size = len;
    handler->buffer->p = static_cast<char*>(malloc(len));
    memcpy(handler->buffer->p, contents.c_str(), len);

    return Status::OK();
  } else {
    auto handler = fs_->NewHandler();
    *fh = handler->fh;
    handler->flags = flags;

    DINGOFS_ERROR rc = fs_->Open(ino);
    if (rc != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Failed open inodeId=" << ino << ", rc: " << rc;
      return filesystem::DingofsErrorToStatus(rc);
    }

    Status s = HandleOpenFlags(ino, flags);
    if (!s.ok()) {
      LOG(ERROR) << "Fail HandleOpenFlags, inodeId=" << ino
                 << ", flags: " << flags << ", status: " << s.ToString();
    }
    return s;
  }
}

Status VFSOld::Create(Ino parent, const std::string& name, uint32_t uid,
                      uint32_t gid, uint32_t mode, int flags, uint64_t* fh,
                      Attr* attr) {
  VLOG(1) << "Create parent inodeId=" << parent << ", name: " << name
          << ", uid: " << uid << ", gid: " << gid << ", mode: " << mode
          << " Octal flags: " << std::oct << flags;

  std::shared_ptr<InodeWrapper> inode_wrapper;
  Status s =
      AllocNode(parent, name, uid, gid, pb::metaserver::FsFileType::TYPE_S3,
                mode, 0, "", &inode_wrapper);
  if (!s.ok()) {
    LOG(WARNING) << "Fail AllocNode in Create, parent inodeId=" << parent
                 << ", name: " << name << ", mode: " << mode
                 << ", flags: " << flags << ", status: " << s.ToString();
    return s;
  }

  auto handler = fs_->NewHandler();
  *fh = handler->fh;
  handler->flags = flags;

  // open
  auto open_files = fs_->BorrowMember().openFiles;
  open_files->Open(inode_wrapper->GetInodeId(), inode_wrapper);

  pb::metaserver::InodeAttr inode_attr;
  inode_wrapper->GetInodeAttr(&inode_attr);

  auto entry_watcher = fs_->BorrowMember().entry_watcher;
  entry_watcher->Remeber(inode_attr, name);

  fs_->BeforeReplyCreate(inode_attr);

  *attr = InodeAttrPBToAttr(inode_attr);
  return Status::OK();
}

void VFSOld::ReadThrottleAdd(size_t size) { throttle_.Add(true, size); }

Status VFSOld::Read(Ino ino, char* buf, uint64_t size, uint64_t offset,
                    uint64_t fh, uint64_t* out_rsize) {
  VLOG(1) << "Read inodeId=" << ino << ", size: " << size
          << ", offset: " << offset << ", fh: " << fh;

  ReadThrottleAdd(size);

  auto read_size = [](uint64_t& size, uint64_t& off,
                      uint64_t& file_size) -> uint64_t {
    if (file_size <= off) {
      return 0;
    } else if (file_size < off + size) {
      return file_size - off;
    } else {
      return size;
    }
  };

  if (ino == STATSINODEID) {
    auto handler = fs_->FindHandler(fh);
    auto* data_buf = handler->buffer;

    size_t file_size = data_buf->size;
    size_t len = read_size(size, offset, file_size);
    if (len > 0 && data_buf->p != nullptr) {
      memcpy(buf, data_buf->p + offset, len);
    }
    *out_rsize = len;
    return Status::OK();
  }

  // fuse read metrics
  uint64_t r_size = 0;

  FsMetricGuard guard(&stub::metric::FSMetric::GetInstance().user_read,
                      &r_size);

  std::shared_ptr<InodeWrapper> inode_wrapper;
  DINGOFS_ERROR ret = inode_cache_manager_->GetInode(ino, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Fail get inode fail in read, rc: " << ret
               << ", inodeId=" << ino;
    guard.Fail();
    return filesystem::DingofsErrorToStatus(ret);
  }

  uint64_t file_size = inode_wrapper->GetLength();
  size_t len = read_size(size, offset, file_size);
  if (len == 0) {
    r_size = 0;
    return Status::OK();
  }

  // Read do not change inode. so we do not get lock here.
  int r_ret = s3_adapter_->Read(ino, offset, len, buf);
  if (r_ret < 0) {
    LOG(ERROR) << "Fail read for inodeId=" << ino << ", rc: " << r_ret;
    guard.Fail();
    return Status::Internal("read s3 fail");
  }
  r_size = r_ret;

  utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();
  inode_wrapper->UpdateTimestampLocked(kAccessTime);
  inode_cache_manager_->ShipToFlush(inode_wrapper);

  *out_rsize = r_size;

  VLOG(1) << "Success read for inodeId=" << ino << ", read size = " << r_size
          << ", out_rsize = " << *out_rsize;

  return Status::OK();
}

void VFSOld::WriteThrottleAdd(uint64_t size) { throttle_.Add(false, size); }

Status VFSOld::Write(Ino ino, const char* buf, uint64_t size, uint64_t offset,
                     uint64_t fh, uint64_t* out_wsize) {
  VLOG(1) << "Write inodeId=" << ino << ", size: " << size
          << ", offset: " << offset << ", fh: " << fh;

  WriteThrottleAdd(size);

  if (!fs_->CheckQuota(ino, size, 0)) {
    return Status::NoSpace("check quota fail");
  }

  // fuse write metrics
  uint64_t w_size = 0;
  FsMetricGuard guard(&stub::metric::FSMetric::GetInstance().user_write,
                      &w_size);

  int w_ret = s3_adapter_->Write(ino, offset, size, buf);
  if (w_ret < 0) {
    LOG(ERROR) << "Fail write for inodeId=" << ino << ", rc: " << w_ret;
    guard.Fail();
    return Status::Internal("write s3 fail");
  }

  w_size = w_ret;

  std::shared_ptr<InodeWrapper> inode_wrapper;
  DINGOFS_ERROR ret = inode_cache_manager_->GetInode(ino, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Fail get inode fail, rc: " << ret << ", inodeId=" << ino;
    guard.Fail();
    return filesystem::DingofsErrorToStatus(ret);
  }

  pb::metaserver::InodeAttr inode_attr;
  size_t change_size = 0;
  {
    utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();

    // update file len
    if (inode_wrapper->GetLengthLocked() < offset + w_size) {
      change_size = offset + w_size - inode_wrapper->GetLengthLocked();
      inode_wrapper->SetLengthLocked(offset + w_size);
    }

    inode_wrapper->UpdateTimestampLocked(kModifyTime | kChangeTime);

    inode_cache_manager_->ShipToFlush(inode_wrapper);

    inode_wrapper->GetInodeAttrUnLocked(&inode_attr);
  }

  fs_->BeforeReplyWrite(inode_attr);

  for (int i = 0; i < inode_attr.parent_size(); i++) {
    auto parent = inode_attr.parent(i);
    fs_->UpdateDirQuotaUsage(parent, change_size, 0);
  }
  fs_->UpdateFsQuotaUsage(change_size, 0);

  *out_wsize = w_size;

  VLOG(1) << "Success write for inodeId=" << ino << ", write size = " << w_size
          << ", out_wsize = " << *out_wsize;
  return Status::OK();
}

Status VFSOld::Flush(Ino ino, uint64_t fh) {
  VLOG(1) << "Flush inodeId=" << ino << ", fh: " << fh;

  if (ino == STATSINODEID) {
    return Status::OK();
  }

  std::shared_ptr<filesystem::FileHandler> handler = fs_->FindHandler(fh);

  if (handler == nullptr) {
    LOG(ERROR) << "Flush find handler fail, inodeId=" << ino << " fh: " << fh;
    return Status::Internal("find handler fail");
  }

  VLOG(1) << "Flush inodeId=" << ino << " fh: " << fh
          << " Octal flags: " << std::oct << handler->flags;

  if ((handler->flags & O_ACCMODE) == O_RDONLY) {
    return Status::OK();
  }

  auto entry_watcher = fs_->BorrowMember().entry_watcher;

  // if enableCto, flush all write cache both in memory cache and disk cache
  if (common::FLAGS_enableCto && !entry_watcher->ShouldWriteback(ino)) {
    DINGOFS_ERROR ret = s3_adapter_->FlushAllCache(ino);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail flush all cache rc: " << ret << ", inodeId=" << ino;
      return filesystem::DingofsErrorToStatus(ret);
    }

    VLOG(3) << "Success flush to s3 inodeId=" << ino;

    std::shared_ptr<InodeWrapper> inode_wrapper;
    ret = inode_cache_manager_->GetInode(ino, inode_wrapper);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail get inode in flush, rc: " << ret
                 << ", inodeId=" << ino;

      return filesystem::DingofsErrorToStatus(ret);
    }

    utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();
    ret = inode_wrapper->Sync();
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail sync s3 chunk info fail, rc: " << ret
                 << ", inodeId=" << ino;
      return filesystem::DingofsErrorToStatus(ret);
    }
  } else {
    // if disableCto, flush just flush data in memory
    DINGOFS_ERROR ret = s3_adapter_->Flush(ino);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "FuseOpFlush, flush to diskcache failed, ret = " << ret
                 << ", inodeId=" << ino;
      return filesystem::DingofsErrorToStatus(ret);
    }
  }

  VLOG(1) << "Success flush inodeId=" << ino;
  return Status::OK();
}

Status VFSOld::Release(Ino ino, uint64_t fh) {
  VLOG(1) << "Release inodeId=" << ino << ", fh: " << fh;

  if (ino == STATSINODEID) {
    fs_->ReleaseHandler(fh);
    return Status::OK();
  }

  auto handler = fs_->FindHandler(fh);
  if (handler == nullptr) {
    LOG(WARNING) << "Release not fine handle, inodeId=" << ino << " fh=" << fh;
  } else {
    fs_->ReleaseHandler(fh);
  }

  DINGOFS_ERROR rc = fs_->Release(ino);
  if (rc != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Fail release inodeId=" << ino << ", rc: " << rc;
    return filesystem::DingofsErrorToStatus(rc);
  }

  return Status::OK();
}

//   If the datasync parameter is non-zero, then only the user data  should be
//   flushed, not the meta data.
Status VFSOld::Fsync(Ino ino, int datasync, uint64_t fh) {
  VLOG(1) << "Fsync inodeId=" << ino << ", datasync: " << datasync
          << ", fh: " << fh;

  if (ino == STATSINODEID) {
    return Status::OK();
  }

  std::shared_ptr<filesystem::FileHandler> handler = fs_->FindHandler(fh);

  if (handler == nullptr) {
    LOG(ERROR) << "Fsync find handler fail, inodeId=" << ino << " fh: " << fh;
    return Status::Internal("find handler fail");
  }

  VLOG(1) << "FuseOpFsync inodeId=" << ino << " fh: " << fh
          << " Octal flags: " << std::oct << handler->flags;

  if ((handler->flags & O_ACCMODE) == O_RDONLY) {
    return Status::OK();
  }

  {
    // sync data
    DINGOFS_ERROR ret = s3_adapter_->Flush(ino);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail flush in fsync inodeId=" << ino << ", rc: " << ret;
      return filesystem::DingofsErrorToStatus(ret);
    }
  }

  if (datasync != 0) {
    VLOG(1) << "Success fsync inodeId=" << ino << ", datasync: " << datasync;
    return Status::OK();
  }

  {
    std::shared_ptr<InodeWrapper> inode_wrapper;
    DINGOFS_ERROR ret = inode_cache_manager_->GetInode(ino, inode_wrapper);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail get inode in fsync, rc: " << ret
                 << ", inodeId=" << ino;
      return filesystem::DingofsErrorToStatus(ret);
    }

    utils::UniqueLock guard = inode_wrapper->GetUniqueLock();
    ret = inode_wrapper->Sync();
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail sync inode meta in fsync, rc: " << ret
                 << ", inodeId=" << ino;
      return filesystem::DingofsErrorToStatus(ret);
    }
  }

  return Status::OK();
}

Status VFSOld::AddWarmupTask(common::WarmupType type, Ino key,
                             const std::string& path,
                             common::WarmupStorageType storage_type) {
  bool add = true;
  switch (type) {
    case dingofs::client::common::WarmupType::kWarmupTypeList:
      add = warmup_manager_->AddWarmupFilelist(key, storage_type);
      break;
    case dingofs::client::common::WarmupType::kWarmupTypeSingle:
      add = warmup_manager_->AddWarmupFile(key, path, storage_type);
      break;
    default:
      // not support add warmup type (warmup single file/dir or filelist)
      LOG(ERROR) << "not support warmup type, only support single/list";
      return Status::NotSupport("not support warmup type");
  }

  Status s = Status::OK();
  if (!add) {
    s = Status::Internal("add warmup task fail");
  }

  return s;
}

// warmup format: op_type\nwarmup_type\npath\nstorage_type
Status VFSOld::Warmup(Ino key, const std::string& name,
                      const std::string& value) {
  CHECK(fs_info_->fstype() == pb::common::FSType::TYPE_S3)
      << "warmup only support s3";

  std::vector<std::string> op_type_path;
  dingofs::utils::SplitString(value, "\n", &op_type_path);
  if (op_type_path.size() != dingofs::client::common::kWarmupOpNum) {
    LOG(ERROR) << "Fail warmup because " << name << " has invalid xattr value "
               << value;
    return Status::InvalidParam("invalid xattr value for warmup");
  }

  auto storage_type =
      dingofs::client::common::GetWarmupStorageType(op_type_path[3]);
  if (storage_type ==
      dingofs::client::common::WarmupStorageType::kWarmupStorageTypeUnknown) {
    LOG(ERROR) << name << " not support storage type: " << value;
    return Status::InvalidParam("invalid storage type for warmup");
  }

  if (common::GetWarmupOpType(op_type_path[0]) ==
      common::WarmupOpType::kWarmupOpAdd) {
    Status s =
        AddWarmupTask(dingofs::client::common::GetWarmupType(op_type_path[1]),
                      key, op_type_path[2], storage_type);
    if (!s.ok()) {
      LOG(ERROR) << "Fail add warmuptask for " << name << ", xattr value "
                 << value << ", status: " << s.ToString();
    }
    return s;
  } else {
    LOG(ERROR) << "Fail warmup because " << name << ", value " << value
               << " is not kWarmupOpAdd";
    return Status::InvalidParam("invalid warmup op type");
  }
}

// TODO: how to process the flags
Status VFSOld::SetXattr(Ino ino, const std::string& name,
                        const std::string& value, int flags) {
  VLOG(1) << "SetXattr inodeId=" << ino << ", name: " << name
          << ", value: " << value << ", flags: " << flags;

  if BAIDU_UNLIKELY ((ino == STATSINODEID)) {
    return Status::NoPermitted("Can not set xattr for .stats");
  }

  if (stub::filesystem::IsWarmupXAttr(name)) {
    return Warmup(ino, name, value);
  }

  if (fuse_client_option_.fileSystemOption.disableXAttr &&
      !stub::filesystem::IsSpecialXAttr(name)) {
    // NOTE: why return nodata?
    return Status::NoData("xattr is disabled");
  }

  if (name.length() > stub::filesystem::MAX_XATTR_NAME_LENGTH ||
      value.length() > stub::filesystem::MAX_XATTR_VALUE_LENGTH) {
    LOG(ERROR) << "Fail setxattr xattr length is too long, name = " << name
               << ", name length = " << name.length()
               << ", value length = " << value.length();
    return Status::InvalidParam("xattr length is too long");
  }

  {
    // set xattr
    std::shared_ptr<InodeWrapper> inode_wrapper;
    DINGOFS_ERROR ret = inode_cache_manager_->GetInode(ino, inode_wrapper);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail get inode in setxattr, rc: " << ret
                 << ", inodeId=" << ino;

      return filesystem::DingofsErrorToStatus(ret);
    }

    dingofs::utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();
    inode_wrapper->SetXattrLocked(name, value);

    ret = inode_wrapper->SyncAttr();
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail sync inode attr in setxattr, rc: " << ret
                 << ", inodeId=" << ino << ", name = " << name
                 << ", value = " << value;

      return filesystem::DingofsErrorToStatus(ret);
    }

    VLOG(1) << "Success SetXattr inodeId=" << ino << ", name: " << name
            << ", value: " << value;
  }

  return Status::OK();
}

void VFSOld::QueryWarmupTask(Ino key, std::string* result) {
  warmup::WarmupProgress progress;
  bool ret = warmup_manager_->QueryWarmupProgress(key, &progress);
  if (!ret) {
    *result = "finished";
  } else {
    *result = std::to_string(progress.GetFinished()) + "/" +
              std::to_string(progress.GetTotal()) + "/" +
              std::to_string(progress.GetErrors());
  }

  VLOG(6) << "Warmup [" << key << "]" << *result;
}

Status VFSOld::GetXattr(Ino ino, const std::string& name, std::string* value) {
  VLOG(1) << "GetXattr inodeId=" << ino << ", name: " << name;

  if BAIDU_UNLIKELY ((ino == STATSINODEID)) {
    // NOTE: why return nodata?
    return Status::NoData("No data for .stats");
  }

  if (stub::filesystem::IsWarmupXAttr(name)) {
    CHECK(fs_info_->fstype() == pb::common::FSType::TYPE_S3)
        << "warmup only support s3";
    QueryWarmupTask(ino, value);
    return Status::OK();
  }

  if (fuse_client_option_.fileSystemOption.disableXAttr &&
      !stub::filesystem::IsSpecialXAttr(name)) {
    return Status::NoData("xattr is disabled");
  }

  pb::metaserver::InodeAttr inode_attr;
  DINGOFS_ERROR ret = inode_cache_manager_->GetInodeAttr(ino, &inode_attr);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Fail get inodeAttr in GetXAttr, rc: " << ret
               << ", inodeId=" << ino;

    return filesystem::DingofsErrorToStatus(ret);
  }

  auto it = inode_attr.xattr().find(name);
  if (it != inode_attr.xattr().end()) {
    *value = it->second;
  }

  VLOG(1) << "Success GetXattr inodeId=" << ino << ", name: " << name
          << ", value length: " << value->length() << ", value: " << *value;

  return Status::OK();
}

Status VFSOld::ListXattr(Ino ino, std::vector<std::string>* xattrs) {
  VLOG(1) << "ListXattr inodeId=" << ino;

  pb::metaserver::InodeAttr inode_attr;
  DINGOFS_ERROR ret = inode_cache_manager_->GetInodeAttr(ino, &inode_attr);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Fail get inodeAttr rc: " << ret << ", inodeId=" << ino;
    return filesystem::DingofsErrorToStatus(ret);
  }

  for (const auto& kv : inode_attr.xattr()) {
    xattrs->push_back(kv.first);
  }

  return Status::OK();
}

Status VFSOld::MkDir(Ino parent, const std::string& name, uint32_t uid,
                     uint32_t gid, uint32_t mode, Attr* attr) {
  VLOG(1) << "Mkdir parent inodeId=" << parent << ", name: " << name
          << ", uid: " << uid << ", gid: " << gid << ", mode: " << mode;

  std::shared_ptr<InodeWrapper> inode_wrapper;
  Status s = AllocNode(parent, name, uid, gid,
                       pb::metaserver::FsFileType::TYPE_DIRECTORY, mode, 0, "",
                       &inode_wrapper);
  if (!s.ok()) {
    LOG(WARNING) << "Fail AllocNode in Mkdir, parent inodeId=" << parent
                 << ", name: " << name << ", mode: " << mode
                 << ", status: " << s.ToString();
    return s;
  }

  pb::metaserver::InodeAttr inode_attr;
  inode_wrapper->GetInodeAttr(&inode_attr);

  fs_->BeforeReplyEntry(inode_attr);
  *attr = InodeAttrPBToAttr(inode_attr);
  return Status::OK();
}

Status VFSOld::OpenDir(Ino ino, uint64_t* fh) {
  VLOG(1) << "OpenDir inodeId=" << ino;

  DINGOFS_ERROR rc = fs_->OpenDir(ino, fh);
  if (rc != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Fail open dir inodeId=" << ino << ", rc: " << rc;
    return filesystem::DingofsErrorToStatus(rc);
  }

  return Status::OK();
}

Status VFSOld::InitDirHandle(Ino ino, uint64_t fh, bool with_attr) {
  VLOG(1) << "InitDirHandle inodeId=" << ino << ", fh: " << fh;

  auto file_handler = fs_->FindHandler(fh);
  if (file_handler->dir_handler_init) {
    return Status::OK();
  }

  auto entry_list = std::make_shared<filesystem::DirEntryList>();
  DINGOFS_ERROR rc = fs_->ReadDir(ino, fh, &entry_list);
  if (rc != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Fail readdir() rc: " << rc << ", inodeId=" << ino
               << ", fh = " << fh;
    return filesystem::DingofsErrorToStatus(rc);
  }

  file_handler->dir_iterator = std::make_unique<filesystem::FsDirIterator>();

  bool has_stats = false;

  std::vector<pb::metaserver::InodeAttr> inode_attrs;

  std::vector<vfs::DirEntry> entries;
  entry_list->Iterate([&](filesystem::DirEntry* dir_entry) {
    if (dir_entry->ino == STATSINODEID) {
      has_stats = true;
    }
    DirEntry entry;
    entry.ino = dir_entry->ino;
    entry.name = dir_entry->name;
    if (with_attr) {
      inode_attrs.push_back(dir_entry->attr);
      entry.attr = InodeAttrPBToAttr(dir_entry->attr);
    }

    file_handler->dir_iterator->Append(entry);
  });

  // out of iterate
  for (auto& inode_attr : inode_attrs) {
    fs_->BeforeReplyEntry(inode_attr);
  }

  // root dir(add .stats file)
  if (!has_stats && ino == ROOTINODEID) {
    DirEntry entry;
    entry.ino = STATSINODEID;
    entry.name = STATSNAME;
    entry.attr = GenerateVirtualInodeAttr(STATSINODEID);
    file_handler->dir_iterator->Append(entry);
  }

  file_handler->dir_handler_init = true;

  return Status::OK();
}

Status VFSOld::ReadDir(Ino ino, uint64_t fh, uint64_t offset, bool with_attr,
                       ReadDirHandler handler) {
  VLOG(1) << "ReadDir inodeId=" << ino << ", fh: " << fh
          << ", offset: " << offset
          << ", with_attr: " << (with_attr ? "true" : "false");
  Status s = InitDirHandle(ino, fh, with_attr);
  if (!s.ok()) {
    LOG(WARNING) << "Fail InitDirHandle in ReadDir, inodeId=" << ino
                 << ", fh: " << fh << ", status: " << s.ToString();
    return s;
  }

  auto file_handler = fs_->FindHandler(fh);
  CHECK(file_handler->dir_handler_init);
  CHECK_NOTNULL(file_handler->dir_iterator);

  auto& dir_iterator = file_handler->dir_iterator;

  while (dir_iterator->Valid()) {
    DirEntry entry = dir_iterator->GetValue(with_attr);
    if (!handler(entry)) {
      LOG(INFO) << "ReadDir break by handler next_offset: " << offset;
      break;
    }

    dir_iterator->Next();
  }

  LOG(INFO) << "ReadDir inodeId=" << ino << ", fh: " << fh
            << ", offset: " << offset << " success";

  return Status::OK();
}

Status VFSOld::ReleaseDir(Ino ino, uint64_t fh) {
  VLOG(1) << "ReleaseDir inodeId=" << ino << ", fh: " << fh;

  DINGOFS_ERROR rc = fs_->ReleaseDir(fh);
  if (rc != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Fail release dir inodeId=" << ino << ", rc: " << rc;
    return filesystem::DingofsErrorToStatus(rc);
  }

  return Status::OK();
}

Status VFSOld::RmDir(Ino parent, const std::string& name) {
  VLOG(1) << "Rmdir parent inodeId=" << parent << ", name: " << name;

  // check if node is recycle or recycle time dir or .stats node
  if ((IsInternalName(name) && parent == ROOTINODEID) ||
      parent == RECYCLEINODEID) {
    return Status::NoPermitted("not permit rmdir internal dir");
  }

  pb::metaserver::Dentry dentry;
  DINGOFS_ERROR ret = dentry_cache_manager_->GetDentry(parent, name, &dentry);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(WARNING) << "Fail GetDentry in rmdir rc: " << ret
                 << ", parent inodeId=" << parent << ", name = " << name;

    return filesystem::DingofsErrorToStatus(ret);
  }

  uint64_t inode_id = dentry.inodeid();

  {
    // check dir empty
    auto limit = fuse_client_option_.listDentryLimit;

    std::list<pb::metaserver::Dentry> dentry_list;
    ret = dentry_cache_manager_->ListDentry(inode_id, &dentry_list, limit);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail ListDentry in rmdir for inodeId=" << inode_id
                 << ", name = " << name << ", parent inodeId=" << parent
                 << ", rc: " << ret;
      return filesystem::DingofsErrorToStatus(ret);
    }

    if (!dentry_list.empty()) {
      LOG(ERROR) << "rmdir not empty";
      return Status::NotEmpty("dir not empty");
    }
  }

  // NOTE: recycle logic is removed

  {
    // remove dentry from parent
    pb::metaserver::FsFileType type =
        pb::metaserver::FsFileType::TYPE_DIRECTORY;

    DINGOFS_ERROR ret = dentry_cache_manager_->DeleteDentry(parent, name, type);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail  DeleteDentry in parent inondeId=" << parent
                 << ", name = " << name << ", inodeId=" << inode_id
                 << ", rc: " << ret;
      return filesystem::DingofsErrorToStatus(ret);
    }

    ret = UpdateParentMCTimeAndNlink(parent, common::NlinkChange::kSubOne);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR)
          << "Fail UpdateParentMCTimeAndNlink in rmdir for  parent inodeId="
          << parent << ", name: " << name;
      return filesystem::DingofsErrorToStatus(ret);
    }
  }

  {
    // unlink inode
    std::shared_ptr<InodeWrapper> inode_wrapper;
    ret = inode_cache_manager_->GetInode(inode_id, inode_wrapper);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail get inode in rmdir for inodeId=" << inode_id
                 << ", name = " << name << ", parent inodeId=" << parent
                 << ", rc: " << ret;
      return filesystem::DingofsErrorToStatus(ret);
    }

    ret = inode_wrapper->UnLink(parent);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Fail UnLink for inodeId=" << inode_id
                 << ", name = " << name << ", parent inodeId=" << parent
                 << ", rc: " << ret;
      return filesystem::DingofsErrorToStatus(ret);
    }
  }

  fs_->UpdateDirQuotaUsage(parent, 0, -1);
  fs_->UpdateFsQuotaUsage(0, -1);

  return Status::OK();
}

Status VFSOld::StatFs(Ino ino, FsStat* fs_stat) {
  VLOG(1) << "StatFs inodeId=" << ino;

  pb::metaserver::Quota quota = fs_->GetFsQuota();

  uint64_t total_bytes = UINT64_MAX;
  if (quota.maxbytes() > 0) {
    total_bytes = quota.maxbytes();
  }

  uint64_t total_inodes = UINT64_MAX;
  if (quota.maxinodes() > 0) {
    total_inodes = quota.maxinodes();
  }

  fs_stat->max_bytes = total_bytes;
  fs_stat->used_bytes = quota.usedbytes();
  fs_stat->max_inodes = total_inodes;
  fs_stat->used_inodes = quota.usedinodes();

  return Status::OK();
}

uint64_t VFSOld::GetMaxNameLength() {
  return fuse_client_option_.fileSystemOption.maxNameLength;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
