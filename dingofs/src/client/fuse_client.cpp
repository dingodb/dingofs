/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include "client/fuse_client.h"

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "brpc/server.h"
#include "client/filesystem/error.h"
#include "client/filesystem/meta.h"
#include "client/fuse_common.h"
#include "client/inode_wrapper.h"
#include "client/service/metrics_dumper.h"
#include "common/define.h"
#include "stub/common/common.h"
#include "stub/filesystem/xattr.h"
#include "utils/net_common.h"
#include "glog/logging.h"

#define RETURN_IF_UNSUCCESS(action) \
  do {                              \
    rc = renameOp.action();         \
    if (rc != DINGOFS_ERROR::OK) {  \
      return rc;                    \
    }                               \
  } while (0)

namespace dingofs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
DECLARE_uint64(fuseClientAvgWriteIops);
DECLARE_uint64(fuseClientBurstWriteIops);
DECLARE_uint64(fuseClientBurstWriteIopsSecs);

DECLARE_uint64(fuseClientAvgWriteBytes);
DECLARE_uint64(fuseClientBurstWriteBytes);
DECLARE_uint64(fuseClientBurstWriteBytesSecs);

DECLARE_uint64(fuseClientAvgReadIops);
DECLARE_uint64(fuseClientBurstReadIops);
DECLARE_uint64(fuseClientBurstReadIopsSecs);

DECLARE_uint64(fuseClientAvgReadBytes);
DECLARE_uint64(fuseClientBurstReadBytes);
DECLARE_uint64(fuseClientBurstReadBytesSecs);
}  // namespace common
}  // namespace client
}  // namespace dingofs

namespace dingofs {
namespace client {

using pb::mds::FSStatusCode;
using pb::mds::FSStatusCode_Name;
using pb::mds::Mountpoint;
using pb::metaserver::Dentry;
using pb::metaserver::DentryFlag;
using pb::metaserver::FsFileType;
using pb::metaserver::InodeAttr;
using pb::metaserver::ManageInodeType;
using pb::metaserver::MetaStatusCode;
using pb::metaserver::Quota;

using common::FuseClientOption;
using common::NlinkChange;
using stub::common::MetaserverID;
using stub::filesystem::IsSpecialXAttr;
using stub::filesystem::MAX_XATTR_NAME_LENGTH;
using stub::filesystem::MAX_XATTR_VALUE_LENGTH;
using stub::filesystem::XATTR_DIR_RENTRIES;
using stub::filesystem::XATTR_DIR_RFBYTES;
using stub::filesystem::XATTR_DIR_RFILES;
using stub::filesystem::XATTR_DIR_RSUBDIRS;
using stub::metric::FSMetric;
using stub::rpcclient::ChannelManager;
using stub::rpcclient::Cli2ClientImpl;
using stub::rpcclient::InodeParam;
using stub::rpcclient::MDSBaseClient;
using stub::rpcclient::MetaCache;
using utils::Atomic;
using utils::ReadWriteThrottleParams;
using utils::Thread;
using utils::ThrottleParams;

using filesystem::DirEntryList;
using filesystem::EntryOut;
using filesystem::ExternalMember;
using filesystem::FileOut;
using filesystem::FileSystem;
using filesystem::Ino;

using common::FLAGS_enableCto;
using common::FLAGS_fuseClientAvgReadBytes;
using common::FLAGS_fuseClientAvgReadIops;
using common::FLAGS_fuseClientAvgWriteBytes;
using common::FLAGS_fuseClientAvgWriteIops;
using common::FLAGS_fuseClientBurstReadBytes;
using common::FLAGS_fuseClientBurstReadBytesSecs;
using common::FLAGS_fuseClientBurstReadIops;
using common::FLAGS_fuseClientBurstReadIopsSecs;
using common::FLAGS_fuseClientBurstWriteBytes;
using common::FLAGS_fuseClientBurstWriteBytesSecs;
using common::FLAGS_fuseClientBurstWriteIops;
using common::FLAGS_fuseClientBurstWriteIopsSecs;

static void on_throttle_timer(void* arg) {
  FuseClient* fuseClient = reinterpret_cast<FuseClient*>(arg);
  fuseClient->InitQosParam();
}

DINGOFS_ERROR FuseClient::Init(const FuseClientOption& option) {
  option_ = option;

  mdsBase_ = new MDSBaseClient();
  FSStatusCode ret = mdsClient_->Init(option.mdsOpt, mdsBase_);
  if (ret != FSStatusCode::OK) {
    return DINGOFS_ERROR::INTERNAL;
  }

  auto cli2Client = std::make_shared<Cli2ClientImpl>();
  auto metaCache = std::make_shared<MetaCache>();
  metaCache->Init(option.metaCacheOpt, cli2Client, mdsClient_);
  auto channelManager = std::make_shared<ChannelManager<MetaserverID>>();

  leaseExecutor_ = absl::make_unique<LeaseExecutor>(
      option.leaseOpt, metaCache, mdsClient_, &enableSumInDir_);

  xattrManager_ = std::make_shared<XattrManager>(inodeManager_, dentryManager_,
                                                 option_.listDentryLimit,
                                                 option_.listDentryThreads);

  MetaStatusCode ret2 = metaClient_->Init(
      option.excutorOpt, option.excutorInternalOpt, metaCache, channelManager);
  if (ret2 != MetaStatusCode::OK) {
    return DINGOFS_ERROR::INTERNAL;
  }

  {
    CHECK_NOTNULL(fsInfo_);
    ExternalMember member(dentryManager_, inodeManager_, metaClient_);
    fs_ = std::make_shared<FileSystem>(fsInfo_->fsid(),
                                       option_.fileSystemOption, member);
  }

  {  // init inode manager
    auto member = fs_->BorrowMember();
    DINGOFS_ERROR rc = inodeManager_->Init(option.refreshDataOption,
                                           member.openFiles, member.deferSync);
    if (rc != DINGOFS_ERROR::OK) {
      return rc;
    }
  }

  if (warmupManager_ != nullptr) {
    warmupManager_->Init(option);
    warmupManager_->SetFsInfo(fsInfo_);
  }

  InitQosParam();

  return DINGOFS_ERROR::OK;
}

void FuseClient::UnInit() {
  if (warmupManager_ != nullptr) {
    warmupManager_->UnInit();
  }

  delete mdsBase_;
  mdsBase_ = nullptr;

  while (bthread_timer_del(throttleTimer_) == 1) {
    bthread_usleep(1000);
  }
}

DINGOFS_ERROR FuseClient::Run() {
  if (isStop_.exchange(false)) {
    return DINGOFS_ERROR::OK;
  }
  return DINGOFS_ERROR::INTERNAL;
}

void FuseClient::Fini() {
  if (!isStop_.exchange(true)) {
    xattrManager_->Stop();
  }
}

DINGOFS_ERROR FuseClient::FuseOpInit(void* userdata,
                                     struct fuse_conn_info* conn) {
  (void)userdata;
  (void)conn;
  fs_->Run();
  return DINGOFS_ERROR::OK;
}

void FuseClient::FuseOpDestroy(void* userdata) {
  if (!init_) {
    return;
  }

  FlushAll();
  fs_->Destory();

  // stop lease before umount fs, otherwise, lease request after umount fs
  // will add a mountpoint entry.
  leaseExecutor_.reset();

  struct MountOption* mOpts = (struct MountOption*)userdata;
  std::string fsName = (mOpts->fsName == nullptr) ? "" : mOpts->fsName;

  Mountpoint mountPoint;
  mountPoint.set_path((mOpts->mountPoint == nullptr) ? "" : mOpts->mountPoint);
  int retVal = SetHostPortInMountPoint(&mountPoint);
  if (retVal < 0) {
    return;
  }
  LOG(INFO) << "Umount " << fsName << " on " << mountPoint.ShortDebugString()
            << " start";

  FSStatusCode ret = mdsClient_->UmountFs(fsName, mountPoint);
  if (ret != FSStatusCode::OK && ret != FSStatusCode::MOUNT_POINT_NOT_EXIST) {
    LOG(ERROR) << "UmountFs failed, FSStatusCode = " << ret
               << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
               << ", fsName = " << fsName
               << ", mountPoint = " << mountPoint.ShortDebugString();
    return;
  }

  LOG(INFO) << "Umount " << fsName << " on " << mountPoint.ShortDebugString()
            << " success!";
}

DINGOFS_ERROR FuseClient::FuseOpLookup(fuse_req_t req, fuse_ino_t parent,
                                       const char* name, EntryOut* entryOut) {
  // check if parent is root inode and file name is .stats name
  if (BAIDU_UNLIKELY(parent == ROOTINODEID &&
                     strcmp(name, STATSNAME) == 0)) {  // stats node
    InodeAttr attr = GenerateVirtualInodeAttr(STATSINODEID, fsInfo_->fsid());
    *entryOut = EntryOut(attr);
    return DINGOFS_ERROR::OK;
  }

  auto entry_watcher = fs_->BorrowMember().entry_watcher;
  DINGOFS_ERROR rc = fs_->Lookup(req, parent, name, entryOut);
  if (rc == DINGOFS_ERROR::OK) {
    entry_watcher->Remeber(entryOut->attr, name);
  } else if (rc == DINGOFS_ERROR::NOTEXIST) {
    // do nothing
  } else {
    LOG(ERROR) << "Lookup() failed, retCode = " << rc << ", parent = " << parent
               << ", name = " << name;
  }
  return rc;
}

DINGOFS_ERROR FuseClient::HandleOpenFlags(fuse_req_t req, fuse_ino_t ino,
                                          struct fuse_file_info* fi,
                                          FileOut* fileOut) {
  std::shared_ptr<InodeWrapper> inode_wrapper;
  // alredy opened
  DINGOFS_ERROR ret = inodeManager_->GetInode(ino, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
               << ", inodeId=" << ino;
    return ret;
  }

  fileOut->fi = fi;
  inode_wrapper->GetInodeAttr(&fileOut->attr);

  if (fi->flags & O_TRUNC) {
    if (fi->flags & O_WRONLY || fi->flags & O_RDWR) {
      VLOG(1) << "HandleOpenFlags, truncate file, ino: " << ino;

      ::dingofs::utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();
      uint64_t length = inode_wrapper->GetLengthLocked();
      DINGOFS_ERROR t_ret = Truncate(inode_wrapper.get(), 0);
      if (t_ret != DINGOFS_ERROR::OK) {
        LOG(ERROR) << "truncate file fail, ret = " << ret
                   << ", inodeId=" << ino;
        return DINGOFS_ERROR::INTERNAL;
      }

      inode_wrapper->SetLengthLocked(0);
      inode_wrapper->UpdateTimestampLocked(kChangeTime | kModifyTime);
      if (length != 0) {
        ret = inode_wrapper->Sync();
        if (ret != DINGOFS_ERROR::OK) {
          return ret;
        }
      } else {
        inode_wrapper->MarkDirty();
      }

      inode_wrapper->GetInodeAttrUnLocked(&fileOut->attr);
    } else {
      return DINGOFS_ERROR::NOPERMISSION;
    }
  }
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FuseClient::FuseOpOpen(fuse_req_t req, fuse_ino_t ino,
                                     struct fuse_file_info* fi,
                                     FileOut* fileOut) {
  VLOG(1) << "FuseOpOpen, ino: " << ino << ", flags: " << fi->flags;
  // check if ino is .stats inode,if true ,get metric data and generate
  // inodeattr information
  if (BAIDU_UNLIKELY(ino == STATSINODEID)) {
    fi->direct_io = 1;
    auto handler = fs_->NewHandler();
    fi->fh = handler->fh;

    MetricsDumper metrics_dumper;
    bvar::DumpOptions opts;
    // opts.white_wildcards =
    // "dingofs_s3_adaptor_read_bps;dingofs_fuse_op_write_lat_qps";
    // opts.black_wildcards = "*var5";
    int ret = bvar::Variable::dump_exposed(&metrics_dumper, &opts);
    std::string contents = metrics_dumper.Contents();

    size_t len = contents.size();
    if (len == 0) return DINGOFS_ERROR::NODATA;

    handler->buffer->size = len;
    handler->buffer->p = static_cast<char*>(malloc(len));
    memcpy(handler->buffer->p, contents.c_str(), len);

    InodeAttr attr = GenerateVirtualInodeAttr(STATSINODEID, fsInfo_->fsid());
    *fileOut = FileOut(fi, attr);
    return DINGOFS_ERROR::OK;
  }

  DINGOFS_ERROR rc = fs_->Open(req, ino, fi);
  if (rc != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "open(" << ino << ") failed, retCode = " << rc;
    return rc;
  }
  return HandleOpenFlags(req, ino, fi, fileOut);
}

DINGOFS_ERROR FuseClient::UpdateParentMCTimeAndNlink(fuse_ino_t parent,
                                                     FsFileType type,
                                                     NlinkChange nlink) {
  std::shared_ptr<InodeWrapper> inode_wrapper;
  auto ret = inodeManager_->GetInode(parent, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
               << ", inodeId=" << parent;
    return ret;
  }

  {
    dingofs::utils::UniqueLock lk = inode_wrapper->GetUniqueLock();
    inode_wrapper->UpdateTimestampLocked(kModifyTime | kChangeTime);

    if (FsFileType::TYPE_DIRECTORY == type) {
      inode_wrapper->UpdateNlinkLocked(nlink);
    }

    if (option_.fileSystemOption.deferSyncOption.deferDirMtime) {
      inodeManager_->ShipToFlush(inode_wrapper);
    } else {
      return inode_wrapper->SyncAttr();
    }
  }

  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FuseClient::MakeNode(
    fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode,
    FsFileType type, dev_t rdev, bool internal,
    std::shared_ptr<InodeWrapper>& inode_wrapper) {
  if (strlen(name) > option_.fileSystemOption.maxNameLength) {
    return DINGOFS_ERROR::NAMETOOLONG;
  }

  // check if node is recycle or under recycle or .stats node
  if (!internal && IsInternalName(name) && parent == ROOTINODEID) {
    LOG(WARNING) << "Can not make node " << name << " under root dir.";
    return DINGOFS_ERROR::NOPERMITTED;
  }

  if (!internal && parent == RECYCLEINODEID) {
    LOG(WARNING) << "Can not make node under recycle.";
    return DINGOFS_ERROR::NOPERMISSION;
  }

  if (!fs_->CheckQuota(parent, 0, 1)) {
    return DINGOFS_ERROR::NO_SPACE;
  }

  const struct fuse_ctx* ctx = fuse_req_ctx(req);
  InodeParam param;
  param.fsId = fsInfo_->fsid();
  if (FsFileType::TYPE_DIRECTORY == type) {
    param.length = 4096;
  } else {
    param.length = 0;
  }
  param.uid = ctx->uid;
  param.gid = ctx->gid;
  param.mode = mode;
  param.type = type;
  param.rdev = rdev;
  param.parent = parent;

  DINGOFS_ERROR ret = inodeManager_->CreateInode(param, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager CreateInode fail, ret = " << ret
               << ", parent = " << parent << ", name = " << name
               << ", mode = " << mode;
    return ret;
  }

  VLOG(6) << "inodeManager CreateInode success, parent = " << parent
          << ", name = " << name << ", mode = " << mode
          << ", inodeId=" << inode_wrapper->GetInodeId();

  Dentry dentry;
  dentry.set_fsid(fsInfo_->fsid());
  dentry.set_inodeid(inode_wrapper->GetInodeId());
  dentry.set_parentinodeid(parent);
  dentry.set_name(name);
  dentry.set_type(inode_wrapper->GetType());
  if (type == FsFileType::TYPE_FILE || type == FsFileType::TYPE_S3) {
    dentry.set_flag(DentryFlag::TYPE_FILE_FLAG);
  }

  ret = dentryManager_->CreateDentry(dentry);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "dentryManager_ CreateDentry fail, ret = " << ret
               << ", parent = " << parent << ", name = " << name
               << ", mode = " << mode;

    DINGOFS_ERROR ret2 =
        inodeManager_->DeleteInode(inode_wrapper->GetInodeId());
    if (ret2 != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Also delete inode failed, ret = " << ret2
                 << ", inodeId=" << inode_wrapper->GetInodeId();
    }
    return ret;
  }

  ret = UpdateParentMCTimeAndNlink(parent, type, NlinkChange::kAddOne);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "UpdateParentMCTimeAndNlink failed, parent: " << parent
               << ", name: " << name << ", type: " << type;
    return ret;
  }

  fs_->UpdateFsQuotaUsage(0, 1);
  fs_->UpdateDirQuotaUsage(parent, 0, 1);

  VLOG(6) << "dentryManager_ CreateDentry success, parent = " << parent
          << ", name = " << name << ", mode = " << mode;

  return ret;
}

DINGOFS_ERROR FuseClient::FuseOpMkDir(fuse_req_t req, fuse_ino_t parent,
                                      const char* name, mode_t mode,
                                      EntryOut* entryOut) {
  VLOG(1) << "FuseOpMkDir, parent: " << parent << ", name: " << name
          << ", mode: " << mode;
  bool internal = false;
  std::shared_ptr<InodeWrapper> inode;
  DINGOFS_ERROR rc = MakeNode(req, parent, name, S_IFDIR | mode,
                              FsFileType::TYPE_DIRECTORY, 0, internal, inode);
  if (rc != DINGOFS_ERROR::OK) {
    return rc;
  }

  inode->GetInodeAttr(&entryOut->attr);
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FuseClient::FuseOpRmDir(fuse_req_t req, fuse_ino_t parent,
                                      const char* name) {
  VLOG(1) << "FuseOpRmDir, parent: " << parent << ", name: " << name;

  if (strlen(name) > option_.fileSystemOption.maxNameLength) {
    return DINGOFS_ERROR::NAMETOOLONG;
  }

  // check if node is recycle or recycle time dir or .stats node
  if ((IsInternalName(name) && parent == ROOTINODEID) ||
      parent == RECYCLEINODEID) {
    return DINGOFS_ERROR::NOPERMITTED;
  }

  Dentry dentry;
  DINGOFS_ERROR ret = dentryManager_->GetDentry(parent, name, &dentry);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(WARNING) << "dentryManager_ GetDentry fail, ret = " << ret
                 << ", parent = " << parent << ", name = " << name;
    return ret;
  }

  uint64_t ino = dentry.inodeid();

  {
    // check dir empty
    std::list<Dentry> dentry_list;
    auto limit = option_.listDentryLimit;
    ret = dentryManager_->ListDentry(ino, &dentry_list, limit);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "dentryManager_ ListDentry fail, ret = " << ret
                 << ", parent = " << ino;
      return ret;
    }

    if (!dentry_list.empty()) {
      LOG(ERROR) << "rmdir not empty";
      return DINGOFS_ERROR::NOTEMPTY;
    }
  }

  FsFileType type = FsFileType::TYPE_DIRECTORY;

  // check if inode should move to recycle
  if (ShouldMoveToRecycle(parent)) {
    ret = MoveToRecycle(req, ino, parent, name, type);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "MoveToRecycle failed, ret = " << ret
                 << ", inodeId=" << ino;
      return ret;
    }
  } else {
    DINGOFS_ERROR ret = dentryManager_->DeleteDentry(parent, name, type);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "dentryManager_ DeleteDentry fail, ret = " << ret
                 << ", parent = " << parent << ", name = " << name;
      return ret;
    }

    ret = UpdateParentMCTimeAndNlink(parent, type, NlinkChange::kSubOne);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "UpdateParentMCTimeAndNlink failed"
                 << ", parent: " << parent << ", name: " << name
                 << ", type: " << type;
      return ret;
    }

    std::shared_ptr<InodeWrapper> inode_wrapper;
    ret = inodeManager_->GetInode(ino, inode_wrapper);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                 << ", inodeId=" << ino;
      return ret;
    }

    ret = inode_wrapper->UnLink(parent);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "UnLink failed, ret = " << ret << ", inodeId=" << ino
                 << ", parent = " << parent << ", name = " << name;
      return ret;
    }

    fs_->UpdateDirQuotaUsage(parent, 0, -1);
    fs_->UpdateFsQuotaUsage(0, -1);
  }

  return DINGOFS_ERROR::OK;
}

std::string GetRecycleTimeDirName() {
  time_t time_stamp;
  time(&time_stamp);
  struct tm p = *localtime_r(&time_stamp, &p);
  char now[64];
  strftime(now, 64, "%Y-%m-%d-%H", &p);
  return now;
}

DINGOFS_ERROR FuseClient::CreateManageNode(fuse_req_t req, uint64_t parent,
                                           const char* name, mode_t mode,
                                           ManageInodeType manageType,
                                           EntryOut* entryOut) {
  if (strlen(name) > option_.fileSystemOption.maxNameLength) {
    return DINGOFS_ERROR::NAMETOOLONG;
  }

  InodeParam param;
  param.fsId = fsInfo_->fsid();
  const struct fuse_ctx* ctx = fuse_req_ctx(req);
  param.uid = ctx->uid;
  param.gid = ctx->gid;
  param.mode = mode;
  param.manageType = manageType;

  std::shared_ptr<InodeWrapper> inodeWrapper;
  DINGOFS_ERROR ret = inodeManager_->CreateManageInode(param, inodeWrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager CreateManageNode fail, ret = " << ret
               << ", parent = " << parent << ", name = " << name
               << ", mode = " << mode;
    return ret;
  }

  VLOG(6) << "inodeManager CreateManageNode success, parent = " << parent
          << ", name = " << name << ", mode = " << mode
          << ", inodeId=" << inodeWrapper->GetInodeId();

  Dentry dentry;
  dentry.set_fsid(fsInfo_->fsid());
  dentry.set_inodeid(inodeWrapper->GetInodeId());
  dentry.set_parentinodeid(parent);
  dentry.set_name(name);
  dentry.set_type(inodeWrapper->GetType());
  FsFileType type = inodeWrapper->GetType();
  if (type == FsFileType::TYPE_FILE || type == FsFileType::TYPE_S3) {
    dentry.set_flag(DentryFlag::TYPE_FILE_FLAG);
  }

  ret = dentryManager_->CreateDentry(dentry);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "dentryManager_ CreateDentry fail, ret = " << ret
               << ", parent = " << parent << ", name = " << name
               << ", mode = " << mode;

    DINGOFS_ERROR ret2 = inodeManager_->DeleteInode(inodeWrapper->GetInodeId());
    if (ret2 != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Also delete inode failed, ret = " << ret2
                 << ", inodeId=" << inodeWrapper->GetInodeId();
    }
    return ret;
  }

  ret = UpdateParentMCTimeAndNlink(parent, type, NlinkChange::kAddOne);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "UpdateParentMCTimeAndNlink failed, parent: " << parent
               << ", name: " << name << ", type: " << type;
    return ret;
  }

  VLOG(6) << "dentryManager_ CreateDentry success, parent = " << parent
          << ", name = " << name << ", mode = " << mode;

  inodeWrapper->GetInodeAttrUnLocked(&entryOut->attr);
  return ret;
}

DINGOFS_ERROR FuseClient::GetOrCreateRecycleDir(fuse_req_t req, Dentry* out) {
  auto ret = dentryManager_->GetDentry(ROOTINODEID, RECYCLENAME, out);
  if (ret != DINGOFS_ERROR::OK && ret != DINGOFS_ERROR::NOTEXIST) {
    LOG(ERROR) << "dentryManager_ GetDentry fail, ret = " << ret
               << ", inode = " << ROOTINODEID << ", name = " << RECYCLENAME;
    return ret;
  } else if (ret == DINGOFS_ERROR::NOTEXIST) {
    LOG(INFO) << "recycle dir is not exist, create " << RECYCLENAME
              << ", parentid = " << ROOTINODEID;
    EntryOut entryOut;
    ret = CreateManageNode(req, ROOTINODEID, RECYCLENAME, S_IFDIR | 0755,
                           ManageInodeType::TYPE_RECYCLE, &entryOut);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "CreateManageNode failed, ret = " << ret
                 << ", inode = " << ROOTINODEID << ", name = " << RECYCLENAME
                 << ", type = TYPE_RECYCLE";
      return ret;
    }
  }

  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FuseClient::MoveToRecycle(fuse_req_t req, fuse_ino_t ino,
                                        fuse_ino_t parent, const char* name,
                                        FsFileType type) {
  (void)type;
  // 1. check recycle exist, if not exist, create recycle dir
  Dentry recycleDir;
  DINGOFS_ERROR ret = GetOrCreateRecycleDir(req, &recycleDir);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "GetOrCreateRecycleDir fail, move " << name
               << " to recycle fail, ret = " << ret;
    return ret;
  }

  // 2. check recycle time dir is exist, if not exist, create time dir
  std::string recycle_time_dir_name = GetRecycleTimeDirName();
  Dentry dentry;
  uint64_t recycle_time_dir_ino;
  ret =
      dentryManager_->GetDentry(RECYCLEINODEID, recycle_time_dir_name, &dentry);
  if (ret != DINGOFS_ERROR::OK && ret != DINGOFS_ERROR::NOTEXIST) {
    LOG(ERROR) << "dentryManager_ GetDentry fail, ret = " << ret
               << ", inode = " << RECYCLEINODEID
               << ", name = " << recycle_time_dir_name;
    return ret;
  } else if (ret == DINGOFS_ERROR::NOTEXIST) {
    std::shared_ptr<InodeWrapper> inode;
    bool internal = true;
    ret = MakeNode(req, RECYCLEINODEID, recycle_time_dir_name.c_str(),
                   S_IFDIR | 0755, FsFileType::TYPE_DIRECTORY, 0, internal,
                   inode);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "MakeNode failed, ret = " << ret
                 << ", inode = " << RECYCLEINODEID
                 << ", name = " << recycle_time_dir_name;
      return ret;
    }
    recycle_time_dir_ino = inode->GetInodeId();
  } else {
    recycle_time_dir_ino = dentry.inodeid();
  }

  // 3. generate new name, parentid_inodeid_name
  std::string new_name = GenerateNewRecycleName(ino, parent, name);

  // 4. move inode to recycle time dir
  ret = FuseOpRename(req, parent, name, recycle_time_dir_ino, new_name.c_str(),
                     0);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "MoveToRecycle failed, ret = " << ret << ", inodeId=" << ino
               << ", parent = " << parent << ", name = " << name;
    return ret;
  }

  return DINGOFS_ERROR::OK;
}

bool FuseClient::ShouldMoveToRecycle(fuse_ino_t parent) {
  // 1. check if recycle is open, if recycle not open, return false
  if (!fsInfo_->has_recycletimehour() || fsInfo_->recycletimehour() == 0) {
    return false;
  }

  // 2. check if inode is in recycle, if node in recycle, return false
  std::shared_ptr<InodeWrapper> inodeWrapper;
  DINGOFS_ERROR ret = inodeManager_->GetInode(parent, inodeWrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
               << ", inodeId=" << parent;
    return false;
  }

  InodeAttr attr;
  inodeWrapper->GetInodeAttrUnLocked(&attr);

  return attr.parent_size() == 0 || attr.parent(0) != RECYCLEINODEID;
}

DINGOFS_ERROR FuseClient::OpUnlink(fuse_req_t req, fuse_ino_t parent,
                                   const char* name, FsFileType type) {
  if (strlen(name) > option_.fileSystemOption.maxNameLength) {
    return DINGOFS_ERROR::NAMETOOLONG;
  }

  // check if node is recycle or recycle time dir or .stats node
  if ((IsInternalName(name) && parent == ROOTINODEID) ||
      parent == RECYCLEINODEID) {
    return DINGOFS_ERROR::NOPERMITTED;
  }

  Dentry dentry;
  DINGOFS_ERROR ret = dentryManager_->GetDentry(parent, name, &dentry);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(WARNING) << "dentryManager_ GetDentry fail, ret = " << ret
                 << ", parent = " << parent << ", name = " << name;
    return ret;
  }

  uint64_t ino = dentry.inodeid();

  // check dir empty
  CHECK(FsFileType::TYPE_DIRECTORY != type) << "unlink dir not supported";

  // check if inode should move to recycle
  if (ShouldMoveToRecycle(parent)) {
    ret = MoveToRecycle(req, ino, parent, name, type);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "MoveToRecycle failed, ret = " << ret
                 << ", inodeId=" << ino;
      return ret;
    }
  } else {
    DINGOFS_ERROR ret = dentryManager_->DeleteDentry(parent, name, type);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "dentryManager_ DeleteDentry fail, ret = " << ret
                 << ", parent = " << parent << ", name = " << name;
      return ret;
    }

    ret = UpdateParentMCTimeAndNlink(parent, type, NlinkChange::kSubOne);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "UpdateParentMCTimeAndNlink failed"
                 << ", parent: " << parent << ", name: " << name
                 << ", type: " << type;
      return ret;
    }

    std::shared_ptr<InodeWrapper> inode_wrapper;
    ret = inodeManager_->GetInode(ino, inode_wrapper);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                 << ", inodeId=" << ino;
      return ret;
    }

    uint32_t new_links = UINT32_MAX;
    ret = inode_wrapper->UnLinkWithReturn(parent, new_links);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "UnLink failed, ret = " << ret << ", inodeId=" << ino
                 << ", parent = " << parent << ", name = " << name;
      return ret;
    }

    int64_t add_space = -(inode_wrapper->GetLength());
    // sym link we not add space
    if (inode_wrapper->GetType() == FsFileType::TYPE_SYM_LINK) {
      add_space = 0;
    }

    fs_->UpdateDirQuotaUsage(parent, add_space, -1);

    if (new_links == 0) {
      fs_->UpdateFsQuotaUsage(add_space, -1);
    }
  }

  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FuseClient::FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info* fi) {
  DINGOFS_ERROR rc = fs_->OpenDir(req, ino, fi);
  if (rc != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "opendir() failed, retCode = " << rc << ", inodeId=" << ino;
  }
  return rc;
}

DINGOFS_ERROR FuseClient::FuseOpReadDir(fuse_req_t req, fuse_ino_t ino,
                                        size_t size, off_t off,
                                        struct fuse_file_info* fi,
                                        char** bufferOut, size_t* rSize,
                                        bool plus) {
  auto handler = fs_->FindHandler(fi->fh);
  DirBufferHead* buffer = handler->buffer;
  if (!handler->padding) {
    auto entries = std::make_shared<DirEntryList>();
    DINGOFS_ERROR rc = fs_->ReadDir(req, ino, fi, &entries);
    if (rc != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "readdir() failed, retCode = " << rc << ", inodeId=" << ino
                 << ", fh = " << fi->fh;
      return rc;
    }

    if (BAIDU_UNLIKELY(ino == ROOTINODEID)) {  // root dir(add .stats file)
      filesystem::DirEntry dir_entry;
      if (!entries->Get(STATSINODEID,
                        &dir_entry)) {  // dirEntry not in dircache
        dir_entry.ino = STATSINODEID;
        dir_entry.name = STATSNAME;
        dir_entry.attr =
            GenerateVirtualInodeAttr(STATSINODEID, fsInfo_->fsid());
        entries->Add(dir_entry);
      }
    }

    entries->Iterate([&](filesystem::DirEntry* dir_entry) {
      if (plus) {
        fs_->AddDirEntryPlus(req, buffer, dir_entry);
      } else {
        fs_->AddDirEntry(req, buffer, dir_entry);
      }
    });
    handler->padding = true;
  }

  if (off < buffer->size) {
    *bufferOut = buffer->p + off;
    *rSize = std::min(buffer->size - off, size);
  } else {
    *bufferOut = nullptr;
    *rSize = 0;
  }
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FuseClient::FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
                                           struct fuse_file_info* fi) {
  DINGOFS_ERROR rc = fs_->ReleaseDir(req, ino, fi);
  if (rc != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "releasedir() failed, retCode = " << rc
               << ", inodeId=" << ino;
  }
  return rc;
}

DINGOFS_ERROR FuseClient::FuseOpRename(fuse_req_t req, fuse_ino_t parent,
                                       const char* name, fuse_ino_t newparent,
                                       const char* newname,
                                       unsigned int flags) {
  VLOG(1) << "FuseOpRename from (" << parent << ", " << name << ") to ("
          << newparent << ", " << newname << ")";

  // internel name can not be rename or rename to
  if ((IsInternalName(name) || IsInternalName(newname)) &&
      parent == ROOTINODEID) {
    return DINGOFS_ERROR::NOPERMITTED;
  }

  // TODO(Wine93): the flag RENAME_EXCHANGE and RENAME_NOREPLACE
  // is only used in linux interface renameat(), not required by posix,
  // we can ignore it now
  if (flags != 0) {
    return DINGOFS_ERROR::INVALIDPARAM;
  }

  uint64_t max_name_length = option_.fileSystemOption.maxNameLength;
  if (strlen(name) > max_name_length || strlen(newname) > max_name_length) {
    LOG(WARNING) << "FuseOpRename name too long, name = " << name
                 << ", name len = " << strlen(name)
                 << ", new name = " << newname
                 << ", new name len = " << strlen(newname)
                 << ", maxNameLength = " << max_name_length;
    return DINGOFS_ERROR::NAMETOOLONG;
  }

  if (parent != newparent) {
    Dentry entry;
    auto rc = dentryManager_->GetDentry(parent, name, &entry);
    if (rc != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "GetDentry failed, ret = " << rc << ", inodeId=" << parent
                 << ", dentry name: " << name;
      return rc;
    }

    InodeAttr attr;
    rc = inodeManager_->GetInodeAttr(entry.inodeid(), &attr);
    if (rc != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "GetInodeAttr failed, ret = " << rc
                 << ", parent inodeId=" << parent
                 << ", entry inodeId=" << entry.inodeid()
                 << ", entry name: " << name;
      return rc;
    }

    if (attr.type() == FsFileType::TYPE_DIRECTORY) {
      // TODO : remove this restrict when we support rename dir in dirfferent
      // quota  dir
      Ino parent_nearest_quota_ino = 0;
      bool parent_has = fs_->NearestDirQuota(parent, parent_nearest_quota_ino);

      Ino newparent_nearest_quota_ino = 0;
      bool newparent_has =
          fs_->NearestDirQuota(newparent, newparent_nearest_quota_ino);

      bool can_rename =
          (!parent_has && !newparent_has) ||
          (newparent_has && parent_has &&
           parent_nearest_quota_ino == newparent_nearest_quota_ino);

      if (!can_rename) {
        LOG(WARNING) << "FuseOpRename not support rename dir between quota dir "
                     << ", name: " << name << ", parent: " << parent
                     << ", parent_has: " << (parent_has ? "true" : "false")
                     << ", parent_nearest_quota_ino: "
                     << parent_nearest_quota_ino << ", newparent: " << newparent
                     << ", newparent_has: "
                     << (newparent_has ? "true" : "false")
                     << ", newparent_nearest_quota_ino: "
                     << newparent_nearest_quota_ino;
        return DINGOFS_ERROR::NOTSUPPORT;
      }
    }
  }

  auto renameOp = RenameOperator(fsInfo_->fsid(), fsInfo_->fsname(), parent,
                                 name, newparent, newname, dentryManager_,
                                 inodeManager_, metaClient_, mdsClient_,
                                 option_.enableMultiMountPointRename);

  dingofs::utils::LockGuard lg(renameMutex_);
  DINGOFS_ERROR rc = DINGOFS_ERROR::OK;
  VLOG(3) << "FuseOpRename [start]: " << renameOp.DebugString();

  RETURN_IF_UNSUCCESS(Precheck);
  RETURN_IF_UNSUCCESS(RecordSrcInodeInfo);
  renameOp.UpdateSrcDirUsage(fs_);
  if (!renameOp.CheckNewParentQuota(fs_)) {
    renameOp.RollbackUpdateSrcDirUsage(fs_);
    return DINGOFS_ERROR::NO_SPACE;
  }

  RETURN_IF_UNSUCCESS(GetTxId);
  RETURN_IF_UNSUCCESS(RecordOldInodeInfo);

  // Do not move LinkDestParentInode behind CommitTx.
  // If so, the nlink will be lost when the machine goes down
  RETURN_IF_UNSUCCESS(LinkDestParentInode);
  RETURN_IF_UNSUCCESS(PrepareTx);
  RETURN_IF_UNSUCCESS(CommitTx);
  VLOG(3) << "FuseOpRename [success]: " << renameOp.DebugString();
  // Do not check UnlinkSrcParentInode, beause rename is already success
  renameOp.UnlinkSrcParentInode();
  renameOp.UnlinkOldInode();
  if (parent != newparent) {
    renameOp.UpdateInodeParent();
  }
  renameOp.UpdateInodeCtime();
  renameOp.UpdateCache();

  renameOp.FinishUpdateUsage(fs_);

  return rc;
}

DINGOFS_ERROR FuseClient::FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info* fi,
                                        filesystem::AttrOut* attrOut) {
  if BAIDU_UNLIKELY ((ino == STATSINODEID)) {
    InodeAttr attr = GenerateVirtualInodeAttr(STATSINODEID, fsInfo_->fsid());
    *attrOut = filesystem::AttrOut(attr);
    return DINGOFS_ERROR::OK;
  }
  DINGOFS_ERROR rc = fs_->GetAttr(req, ino, attrOut);
  if (rc != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "getattr() fail, retCode = " << rc << ", inodeId=" << ino;
  }
  return rc;
}

// TODO: maybe we should check quota when set size is larger than current file
// size
DINGOFS_ERROR FuseClient::FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino,
                                        struct stat* attr, int to_set,
                                        struct fuse_file_info* fi,
                                        struct filesystem::AttrOut* attr_out) {
  VLOG(1) << "FuseOpSetAttr to_set: " << to_set << ", inodeId=" << ino
          << ", attr: " << *attr;
  if BAIDU_UNLIKELY ((ino == STATSINODEID)) {
    InodeAttr attr = GenerateVirtualInodeAttr(STATSINODEID, fsInfo_->fsid());
    *attr_out = filesystem::AttrOut(attr);
    return DINGOFS_ERROR::OK;
  }
  std::shared_ptr<InodeWrapper> inode_wrapper;
  DINGOFS_ERROR ret = inodeManager_->GetInode(ino, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
               << ", inodeId=" << ino;
    return ret;
  }

  ::dingofs::utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();
  if (to_set & FUSE_SET_ATTR_MODE) {
    inode_wrapper->SetMode(attr->st_mode);
  }

  if (to_set & FUSE_SET_ATTR_UID) {
    inode_wrapper->SetUid(attr->st_uid);
  }

  if (to_set & FUSE_SET_ATTR_GID) {
    inode_wrapper->SetGid(attr->st_gid);
  }

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);

  if (to_set & FUSE_SET_ATTR_ATIME) {
    inode_wrapper->UpdateTimestampLocked(attr->st_atim, kAccessTime);
  }

  if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
    inode_wrapper->UpdateTimestampLocked(now, kAccessTime);
  }

  if (to_set & FUSE_SET_ATTR_MTIME) {
    inode_wrapper->UpdateTimestampLocked(attr->st_mtim, kModifyTime);
  }

  if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
    inode_wrapper->UpdateTimestampLocked(now, kModifyTime);
  }

  if (to_set & FUSE_SET_ATTR_CTIME) {
    inode_wrapper->UpdateTimestampLocked(attr->st_ctim, kChangeTime);
  } else {
    inode_wrapper->UpdateTimestampLocked(now, kChangeTime);
  }

  if (to_set & FUSE_SET_ATTR_SIZE) {
    ret = Truncate(inode_wrapper.get(), attr->st_size);
    if (ret != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "truncate file fail, ret = " << ret << ", inodeId=" << ino;
      return ret;
    }

    inode_wrapper->SetLengthLocked(attr->st_size);
    ret = inode_wrapper->Sync();
    if (ret != DINGOFS_ERROR::OK) {
      return ret;
    }
    inode_wrapper->GetInodeAttrUnLocked(&attr_out->attr);

    return ret;
  }

  ret = inode_wrapper->SyncAttr();
  if (ret != DINGOFS_ERROR::OK) {
    return ret;
  }

  inode_wrapper->GetInodeAttrUnLocked(&attr_out->attr);
  return ret;
}

DINGOFS_ERROR FuseClient::FuseOpGetXattr(fuse_req_t req, fuse_ino_t ino,
                                         const char* name, std::string* value,
                                         size_t size) {
  (void)req;
  VLOG(9) << "FuseOpGetXattr, inodeId=" << ino << ", name: " << name
          << ", size = " << size;

  if (option_.fileSystemOption.disableXAttr && !IsSpecialXAttr(name)) {
    return DINGOFS_ERROR::NODATA;
  }
  if BAIDU_UNLIKELY ((ino == STATSINODEID)) {
    return DINGOFS_ERROR::NODATA;
  }

  InodeAttr inodeAttr;
  DINGOFS_ERROR ret = inodeManager_->GetInodeAttr(ino, &inodeAttr);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inodeAttr fail, ret = " << ret
               << ", inodeId=" << ino;
    return ret;
  }

  ret =
      xattrManager_->GetXattr(name, value, &inodeAttr, enableSumInDir_.load());
  if (DINGOFS_ERROR::OK != ret) {
    LOG(ERROR) << "xattrManager get xattr failed, name = " << name;
    return ret;
  }

  ret = DINGOFS_ERROR::NODATA;
  if (value->length() > 0) {
    if ((size == 0 && value->length() <= MAX_XATTR_VALUE_LENGTH) ||
        (size >= value->length() &&
         value->length() <= MAX_XATTR_VALUE_LENGTH)) {
      VLOG(1) << "FuseOpGetXattr name = " << name
              << ", length = " << value->length() << ", value = " << *value;
      ret = DINGOFS_ERROR::OK;
    } else {
      ret = DINGOFS_ERROR::OUT_OF_RANGE;
    }
  }
  return ret;
}

DINGOFS_ERROR FuseClient::FuseOpSetXattr(fuse_req_t req, fuse_ino_t ino,
                                         const char* name, const char* value,
                                         size_t size, int flags) {
  std::string strname(name);
  std::string strvalue(value, size);
  VLOG(1) << "FuseOpSetXattr inodeId=" << ino << ", name: " << name
          << ", size = " << size << ", strvalue: " << strvalue;

  if (option_.fileSystemOption.disableXAttr && !IsSpecialXAttr(name)) {
    return DINGOFS_ERROR::NODATA;
  }
  if BAIDU_UNLIKELY ((ino == STATSINODEID)) {
    return DINGOFS_ERROR::NODATA;
  }

  if (strname.length() > MAX_XATTR_NAME_LENGTH ||
      size > MAX_XATTR_VALUE_LENGTH) {
    LOG(ERROR) << "xattr length is too long, name = " << name
               << ", name length = " << strname.length()
               << ", value length = " << size;
    return DINGOFS_ERROR::OUT_OF_RANGE;
  }

  std::shared_ptr<InodeWrapper> inode_wrapper;
  DINGOFS_ERROR ret = inodeManager_->GetInode(ino, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
               << ", inodeId=" << ino;
    return ret;
  }

  ::dingofs::utils::UniqueLock lg_guard = inode_wrapper->GetUniqueLock();
  inode_wrapper->SetXattrLocked(strname, strvalue);
  ret = inode_wrapper->SyncAttr();
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "set xattr fail, ret = " << ret << ", inodeId=" << ino
               << ", name = " << strname << ", value = " << strvalue;
    return ret;
  }
  VLOG(1) << "FuseOpSetXattr end";
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FuseClient::FuseOpListXattr(fuse_req_t req, fuse_ino_t ino,
                                          char* value, size_t size,
                                          size_t* realSize) {
  (void)req;
  VLOG(1) << "FuseOpListXattr, inodeId=" << ino << ", size = " << size;
  InodeAttr inodeAttr;
  DINGOFS_ERROR ret = inodeManager_->GetInodeAttr(ino, &inodeAttr);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inodeAttr fail, ret = " << ret
               << ", inodeId=" << ino;
    return ret;
  }

  // get xattr key
  for (const auto& it : inodeAttr.xattr()) {
    // +1 because, the format is key\0key\0
    *realSize += it.first.length() + 1;
  }

  // add summary xattr key
  if (inodeAttr.type() == FsFileType::TYPE_DIRECTORY) {
    *realSize += strlen(XATTR_DIR_RFILES) + 1;
    *realSize += strlen(XATTR_DIR_RSUBDIRS) + 1;
    *realSize += strlen(XATTR_DIR_RENTRIES) + 1;
    *realSize += strlen(XATTR_DIR_RFBYTES) + 1;
  }

  if (size == 0) {
    return DINGOFS_ERROR::OK;
  } else if (size >= *realSize) {
    for (const auto& it : inodeAttr.xattr()) {
      auto tsize = it.first.length() + 1;
      memcpy(value, it.first.c_str(), tsize);
      value += tsize;
    }
    if (inodeAttr.type() == FsFileType::TYPE_DIRECTORY) {
      memcpy(value, XATTR_DIR_RFILES, strlen(XATTR_DIR_RFILES) + 1);
      value += strlen(XATTR_DIR_RFILES) + 1;
      memcpy(value, XATTR_DIR_RSUBDIRS, strlen(XATTR_DIR_RSUBDIRS) + 1);
      value += strlen(XATTR_DIR_RSUBDIRS) + 1;
      memcpy(value, XATTR_DIR_RENTRIES, strlen(XATTR_DIR_RENTRIES) + 1);
      value += strlen(XATTR_DIR_RENTRIES) + 1;
      memcpy(value, XATTR_DIR_RFBYTES, strlen(XATTR_DIR_RFBYTES) + 1);
      value += strlen(XATTR_DIR_RFBYTES) + 1;
    }
    return DINGOFS_ERROR::OK;
  }
  return DINGOFS_ERROR::OUT_OF_RANGE;
}

DINGOFS_ERROR FuseClient::FuseOpSymlink(fuse_req_t req, const char* link,
                                        fuse_ino_t parent, const char* name,
                                        EntryOut* entry_out) {
  if (strlen(name) > option_.fileSystemOption.maxNameLength) {
    return DINGOFS_ERROR::NAMETOOLONG;
  }

  // internal file name can not allowed for symlink
  if (parent == ROOTINODEID &&
      IsInternalName(name)) {  // cant't allow  ln -s <file> .stats
    return DINGOFS_ERROR::EXISTS;
  }

  if (parent == ROOTINODEID &&
      IsInternalName(link)) {  // cant't allow  ln -s  .stats  <file>
    return DINGOFS_ERROR::NOPERMITTED;
  }

  // no need to cal length
  if (!fs_->CheckQuota(parent, 0, 1)) {
    return DINGOFS_ERROR::NO_SPACE;
  }

  const struct fuse_ctx* ctx = fuse_req_ctx(req);
  InodeParam param;
  param.fsId = fsInfo_->fsid();
  param.length = std::strlen(link);
  param.uid = ctx->uid;
  param.gid = ctx->gid;
  param.mode = S_IFLNK | 0777;
  param.type = FsFileType::TYPE_SYM_LINK;
  param.symlink = link;
  param.parent = parent;

  std::shared_ptr<InodeWrapper> inode_wrapper;
  DINGOFS_ERROR ret = inodeManager_->CreateInode(param, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager CreateInode fail, ret = " << ret
               << ", parent = " << parent << ", name = " << name
               << ", mode = " << param.mode;
    return ret;
  }

  Dentry dentry;
  dentry.set_fsid(fsInfo_->fsid());
  dentry.set_inodeid(inode_wrapper->GetInodeId());
  dentry.set_parentinodeid(parent);
  dentry.set_name(name);
  dentry.set_type(inode_wrapper->GetType());
  ret = dentryManager_->CreateDentry(dentry);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "dentryManager_ CreateDentry fail, ret = " << ret
               << ", parent = " << parent << ", name = " << name
               << ", mode = " << param.mode;

    DINGOFS_ERROR ret2 =
        inodeManager_->DeleteInode(inode_wrapper->GetInodeId());
    if (ret2 != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Also delete inode failed, ret = " << ret2
                 << ", inodeId=" << inode_wrapper->GetInodeId();
    }
    return ret;
  }

  ret = UpdateParentMCTimeAndNlink(parent, FsFileType::TYPE_SYM_LINK,
                                   NlinkChange::kAddOne);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "UpdateParentMCTimeAndNlink failed, link:" << link
               << ", parent: " << parent << ", name: " << name
               << ", type: " << FsFileType::TYPE_SYM_LINK;
    return ret;
  }

  fs_->UpdateFsQuotaUsage(0, 1);
  fs_->UpdateDirQuotaUsage(parent, 0, 1);

  inode_wrapper->GetInodeAttr(&entry_out->attr);
  return ret;
}

DINGOFS_ERROR FuseClient::FuseOpStatFs(fuse_req_t req, fuse_ino_t ino,
                                       struct statvfs* stbuf) {
  (void)req;
  (void)ino;
  Quota quota = fs_->GetFsQuota();

  uint64_t block_size = 4096;

  uint64_t total_bytes = UINT64_MAX;
  if (quota.maxbytes() > 0) {
    total_bytes = quota.maxbytes();
  }

  uint64_t total_blocks =
      ((total_bytes % block_size == 0) ? total_bytes / block_size
                                       : total_bytes / block_size + 1);
  uint64_t free_blocks = 0;
  if (total_bytes - quota.usedbytes() <= 0) {
    free_blocks = 0;
  } else {
    if (quota.usedbytes() > 0) {
      uint64_t used_blocks = (quota.usedbytes() % block_size == 0)
                                 ? quota.usedbytes() / block_size
                                 : quota.usedbytes() / block_size + 1;
      free_blocks = total_blocks - used_blocks;
    } else {
      free_blocks = total_blocks;
    }
  }

  uint64_t total_inodes = UINT64_MAX;
  if (quota.maxinodes() > 0) {
    total_inodes = quota.maxinodes();
  }

  uint64_t free_inodes = 0;
  if (total_inodes - quota.usedinodes() <= 0) {
    free_inodes = 0;
  } else {
    if (quota.usedinodes() > 0) {
      free_inodes = total_inodes - quota.usedinodes();
    } else {
      free_inodes = total_inodes;
    }
  }

  stbuf->f_frsize = stbuf->f_bsize = block_size;
  stbuf->f_blocks = total_blocks;
  stbuf->f_bfree = stbuf->f_bavail = free_blocks;
  stbuf->f_files = total_inodes;
  stbuf->f_ffree = stbuf->f_favail = free_inodes;
  stbuf->f_fsid = fsInfo_->fsid();
  stbuf->f_flag = 0;
  stbuf->f_namemax = option_.fileSystemOption.maxNameLength;

  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FuseClient::OpLink(fuse_req_t req, fuse_ino_t ino,
                                 fuse_ino_t newparent, const char* newname,
                                 FsFileType type, EntryOut* entry_out) {
  if (strlen(newname) > option_.fileSystemOption.maxNameLength) {
    return DINGOFS_ERROR::NAMETOOLONG;
  }

  if (IsInternalNode(ino) ||  // cant't allow  ln   <file> .stats
      (newparent == ROOTINODEID &&
       IsInternalName(newname))) {  // cant't allow  ln  .stats  <file>
    return DINGOFS_ERROR::NOPERMITTED;
  }

  std::shared_ptr<InodeWrapper> inode_wrapper;
  DINGOFS_ERROR ret = inodeManager_->GetInode(ino, inode_wrapper);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
               << ", inodeId=" << ino;
    return ret;
  }

  if (!fs_->CheckDirQuota(newparent, inode_wrapper->GetLength(), 1)) {
    return DINGOFS_ERROR::NO_SPACE;
  }

  ret = inode_wrapper->Link(newparent);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "Link Inode fail, ret = " << ret << ", inodeId=" << ino
               << ", newparent = " << newparent << ", newname = " << newname;
    return ret;
  }

  Dentry dentry;
  dentry.set_fsid(fsInfo_->fsid());
  dentry.set_inodeid(inode_wrapper->GetInodeId());
  dentry.set_parentinodeid(newparent);
  dentry.set_name(newname);
  dentry.set_type(inode_wrapper->GetType());
  ret = dentryManager_->CreateDentry(dentry);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "dentryManager_ CreateDentry fail, ret = " << ret
               << ", parent = " << newparent << ", name = " << newname;

    DINGOFS_ERROR ret2 = inode_wrapper->UnLink(newparent);
    if (ret2 != DINGOFS_ERROR::OK) {
      LOG(ERROR) << "Also unlink inode failed, ret = " << ret2
                 << ", inodeId=" << inode_wrapper->GetInodeId();
    }
    return ret;
  }

  ret = UpdateParentMCTimeAndNlink(newparent, type, NlinkChange::kAddOne);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "UpdateParentMCTimeAndNlink failed"
               << ", parent: " << newparent << ", name: " << newname
               << ", type: " << type;
    return ret;
  }

  inode_wrapper->GetInodeAttr(&entry_out->attr);
  auto entry_watcher = fs_->BorrowMember().entry_watcher;
  entry_watcher->Forget(ino);

  fs_->UpdateDirQuotaUsage(newparent, entry_out->attr.length(), 1);
  return ret;
}

DINGOFS_ERROR FuseClient::FuseOpReadLink(fuse_req_t req, fuse_ino_t ino,
                                         std::string* linkStr) {
  (void)req;
  VLOG(1) << "FuseOpReadLink, inodeId=" << ino << ", linkStr: " << linkStr;
  InodeAttr attr;
  DINGOFS_ERROR ret = inodeManager_->GetInodeAttr(ino, &attr);
  if (ret != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "inodeManager get inodeAttr fail, ret = " << ret
               << ", inodeId=" << ino;
    return ret;
  }
  *linkStr = attr.symlink();
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FuseClient::FuseOpRelease(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info* fi) {
  DINGOFS_ERROR rc = fs_->Release(req, ino, fi);
  if (rc != DINGOFS_ERROR::OK) {
    LOG(ERROR) << "release() failed, inodeId=" << ino;
  }
  return rc;
}

void FuseClient::FlushAll() { FlushData(); }

DINGOFS_ERROR
FuseClient::SetMountStatus(const struct MountOption* mountOption) {
  mountpoint_.set_path(
      (mountOption->mountPoint == nullptr) ? "" : mountOption->mountPoint);
  std::string fsName =
      (mountOption->fsName == nullptr) ? "" : mountOption->fsName;

  mountpoint_.set_cto(FLAGS_enableCto);

  int retVal = SetHostPortInMountPoint(&mountpoint_);
  if (retVal < 0) {
    LOG(ERROR) << "Set Host and Port in MountPoint failed, ret = " << retVal;
    return DINGOFS_ERROR::INTERNAL;
  }

  auto ret = mdsClient_->MountFs(fsName, mountpoint_, fsInfo_.get());
  if (ret != FSStatusCode::OK && ret != FSStatusCode::MOUNT_POINT_EXIST) {
    LOG(ERROR) << "MountFs failed, FSStatusCode = " << ret
               << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
               << ", fsName = " << fsName
               << ", mountPoint = " << mountpoint_.ShortDebugString();
    return DINGOFS_ERROR::MOUNT_FAILED;
  }
  inodeManager_->SetFsId(fsInfo_->fsid());
  dentryManager_->SetFsId(fsInfo_->fsid());
  enableSumInDir_.store(fsInfo_->enablesumindir());
  if (fsInfo_->has_recycletimehour()) {
    enableSumInDir_.store(enableSumInDir_.load() &&
                          (fsInfo_->recycletimehour() == 0));
  }

  LOG(INFO) << "Mount " << fsName << " on " << mountpoint_.ShortDebugString()
            << " success! enableSumInDir = " << enableSumInDir_.load();

  fsMetric_ = std::make_shared<FSMetric>(fsName);

  // init fsname and mountpoint
  leaseExecutor_->SetFsName(fsName);
  leaseExecutor_->SetMountPoint(mountpoint_);
  if (!leaseExecutor_->Start()) {
    return DINGOFS_ERROR::INTERNAL;
  }

  init_ = true;
  if (warmupManager_ != nullptr) {
    warmupManager_->SetMounted(true);
  }
  return DINGOFS_ERROR::OK;
}

void FuseClient::InitQosParam() {
  ReadWriteThrottleParams params;
  params.iopsWrite = ThrottleParams(FLAGS_fuseClientAvgWriteIops,
                                    FLAGS_fuseClientBurstWriteIops,
                                    FLAGS_fuseClientBurstWriteIopsSecs);

  params.bpsWrite = ThrottleParams(FLAGS_fuseClientAvgWriteBytes,
                                   FLAGS_fuseClientBurstWriteBytes,
                                   FLAGS_fuseClientBurstWriteBytesSecs);

  params.iopsRead =
      ThrottleParams(FLAGS_fuseClientAvgReadIops, FLAGS_fuseClientBurstReadIops,
                     FLAGS_fuseClientBurstReadIopsSecs);

  params.bpsRead = ThrottleParams(FLAGS_fuseClientAvgReadBytes,
                                  FLAGS_fuseClientBurstReadBytes,
                                  FLAGS_fuseClientBurstReadBytesSecs);

  throttle_.UpdateThrottleParams(params);

  int ret = bthread_timer_add(&throttleTimer_, butil::seconds_from_now(1),
                              on_throttle_timer, this);
  if (ret != 0) {
    LOG(ERROR) << "Create fuse client throttle timer failed!";
  }
}

static bool StartBrpcDummyserver(uint32_t start_port, uint32_t end_port,
                                 uint32_t* listen_port) {
  static std::once_flag flag;
  std::call_once(flag, [&]() {
    while (start_port < end_port) {
      int ret = brpc::StartDummyServerAt(start_port);
      if (ret >= 0) {
        LOG(INFO) << "Start dummy server success, listen port = " << start_port;
        *listen_port = start_port;
        break;
      }

      ++start_port;
    }
  });

  if (start_port >= end_port) {
    LOG(ERROR) << "Start dummy server failed, start_port = " << start_port;
    return false;
  }

  return true;
}

DINGOFS_ERROR FuseClient::InitBrpcServer() {
  uint32_t listen_port = 0;
  if (!StartBrpcDummyserver(option_.dummyServerStartPort, PORT_LIMIT,
                            &listen_port)) {
    return DINGOFS_ERROR::INTERNAL;
  }

  std::string local_ip;
  if (!dingofs::utils::NetCommon::GetLocalIP(&local_ip)) {
    LOG(ERROR) << "Get local ip failed!";
    return DINGOFS_ERROR::INTERNAL;
  }

  dingofs::stub::common::ClientDummyServerInfo::GetInstance().SetPort(
      listen_port);
  dingofs::stub::common::ClientDummyServerInfo::GetInstance().SetIP(local_ip);

  return DINGOFS_ERROR::OK;
}

}  // namespace client
}  // namespace dingofs
