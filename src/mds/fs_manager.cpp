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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include "mds/fs_manager.h"

#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>
#include <sys/stat.h>

#include <cstdint>
#include <list>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>

#include "blockaccess/accesser_common.h"
#include "blockaccess/block_accesser.h"
#include "common/config_mapper.h"
#include "common/define.h"
#include "dingofs/common.pb.h"
#include "dingofs/mds.pb.h"
#include "mds/common/types.h"
#include "mds/metric/fs_metric.h"
#include "utils/string_util.h"
#include "utils/timeutility.h"

namespace dingofs {
namespace mds {

using ::google::protobuf::util::MessageDifferencer;

using mds::dlock::LOCK_STATUS;
using mds::topology::TopoStatusCode;
using utils::Thread;

using NameLockGuard = ::dingofs::utils::GenericNameLockGuard<Mutex>;

bool FsManager::Init() {
  LOG_IF(FATAL, !fsStorage_->Init()) << "fsStorage Init fail";
  RebuildTimeRecorder();
  return true;
}

void FsManager::Run() {
  if (isStop_.exchange(false)) {
    backEndThread_ = Thread(&FsManager::BackEndFunc, this);
    checkMountPointThread_ = Thread(&FsManager::BackEndCheckMountPoint, this);
    LOG(INFO) << "FsManager start running";
  } else {
    LOG(INFO) << "FsManager already is running";
  }
}

void FsManager::Stop() {
  if (!isStop_.exchange(true)) {
    LOG(INFO) << "stop FsManager...";
    sleeper_.interrupt();
    backEndThread_.join();
    checkMountPointSleeper_.interrupt();
    checkMountPointThread_.join();
    LOG(INFO) << "stop FsManager ok.";
  } else {
    LOG(INFO) << "FsManager not running.";
  }
}

void FsManager::Uninit() {
  Stop();
  fsStorage_->Uninit();
  LOG(INFO) << "FsManager Uninit ok.";
}

bool FsManager::DeletePartiton(std::string fs_name,
                               const pb::common::PartitionInfo& partition) {
  LOG(INFO) << "delete fs partition, fsName = " << fs_name
            << ", partitionId = " << partition.partitionid();
  // send rpc to metaserver, get copyset members
  std::set<std::string> addrs;
  if (TopoStatusCode::TOPO_OK !=
      topoManager_->GetCopysetMembers(partition.poolid(), partition.copysetid(),
                                      &addrs)) {
    LOG(ERROR) << "delete partition fail, get copyset "
                  "members fail"
               << ", poolId = " << partition.poolid()
               << ", copysetId = " << partition.copysetid();
    return false;
  }

  FSStatusCode ret = metaserverClient_->DeletePartition(
      partition.poolid(), partition.copysetid(), partition.partitionid(),
      addrs);
  if (ret != FSStatusCode::OK && ret != FSStatusCode::UNDER_DELETING) {
    LOG(ERROR) << "delete partition fail, fsName = " << fs_name
               << ", partitionId = " << partition.partitionid()
               << ", errCode = " << FSStatusCode_Name(ret);
    return false;
  }

  return true;
}

bool FsManager::SetPartitionToDeleting(
    const pb::common::PartitionInfo& partition) {
  LOG(INFO) << "set partition status to deleting, partitionId = "
            << partition.partitionid();
  TopoStatusCode ret = topoManager_->UpdatePartitionStatus(
      partition.partitionid(), pb::common::PartitionStatus::DELETING);
  if (ret != TopoStatusCode::TOPO_OK) {
    LOG(ERROR) << "set partition to deleting fail, partitionId = "
               << partition.partitionid();
    return false;
  }
  return true;
}

void FsManager::ScanFs(const FsInfoWrapper& wrapper) {
  VLOG(1) << "start to scan fs, fsName = " << wrapper.GetFsName()
          << ", fsId = " << wrapper.GetFsId()
          << ", status = " << pb::mds::FsStatus_Name(wrapper.GetStatus());

  if (wrapper.GetStatus() != pb::mds::FsStatus::DELETING) {
    return;
  }
  LOG(INFO) << "starting to delete fs, fsName = " << wrapper.GetFsName()
            << ", fsId = " << wrapper.GetFsId();

  std::list<pb::common::PartitionInfo> partition_list;
  topoManager_->ListPartitionOfFs(wrapper.GetFsId(), &partition_list);
  if (partition_list.empty()) {
    LOG(INFO) << "fs has no partition, delete fs record, fsName = "
              << wrapper.GetFsName() << ", fsId = " << wrapper.GetFsId();

    FSStatusCode ret = fsStorage_->Delete(wrapper.GetFsName());
    if (ret != FSStatusCode::OK) {
      LOG(ERROR) << "delete fs record fail, fsName = " << wrapper.GetFsName()
                 << ", errCode = " << FSStatusCode_Name(ret);
    } else {
      LOG(INFO) << "successfully delete fs, fsName = " << wrapper.GetFsName()
                << ", fsId = " << wrapper.GetFsId();
    }

    return;
  }

  for (const pb::common::PartitionInfo& partition : partition_list) {
    if (!DeletePartiton(wrapper.GetFsName(), partition)) {
      continue;
    }
    if (partition.status() != pb::common::PartitionStatus::DELETING) {
      SetPartitionToDeleting(partition);
    }
  }
}

void FsManager::BackEndFunc() {
  while (sleeper_.wait_for(
      std::chrono::seconds(option_.backEndThreadRunInterSec))) {
    std::vector<FsInfoWrapper> wrapper_vec;
    fsStorage_->GetAll(&wrapper_vec);
    for (const FsInfoWrapper& wrapper : wrapper_vec) {
      ScanFs(wrapper);
    }
  }
}

void MountPoint2Str(const Mountpoint& in, std::string* out) {
  *out = in.hostname() + ":" + std::to_string(in.port()) + ":" + in.path();
}

bool Str2MountPoint(const std::string& in, Mountpoint* out) {
  std::vector<std::string> vec;
  dingofs::utils::SplitString(in, ":", &vec);
  if (vec.size() != 3) {
    LOG(ERROR) << "split string to mountpoint failed, str = " << in;
    return false;
  }
  out->set_hostname(vec[0]);
  uint32_t port;
  if (!dingofs::utils::StringToUl(vec[1], &port)) {
    LOG(ERROR) << "StringToUl failed, str = " << vec[1];
    return false;
  }
  out->set_port(port);
  out->set_path(vec[2]);
  return true;
}

void FsManager::CheckMountPoint() {
  std::map<std::string, std::pair<std::string, uint64_t>> tmap;
  {
    ReadLockGuard rlock(recorderMutex_);
    tmap = mpTimeRecorder_;
  }
  uint64_t now = ::dingofs::utils::TimeUtility::GetTimeofDaySec();
  for (auto& iter : tmap) {
    std::string fs_name = iter.second.first;
    std::string mountpath = iter.first;
    if (now - iter.second.second > option_.clientTimeoutSec) {
      Mountpoint mountpoint;
      if (!Str2MountPoint(mountpath, &mountpoint)) {
        LOG(ERROR) << "mountpath to mountpoint failed, mountpath = "
                   << mountpath;
        DeleteClientAliveTime(iter.first);
      } else {
        auto ret = UmountFs(fs_name, mountpoint);
        if (ret != FSStatusCode::OK &&
            ret != FSStatusCode::MOUNT_POINT_NOT_EXIST) {
          LOG(WARNING) << "umount fs = " << fs_name
                       << " form mountpoint = " << mountpath
                       << " failed when client timeout";
        } else {
          LOG(INFO) << "umount fs = " << fs_name
                    << " mountpoint = " << mountpath
                    << " success after client timeout.";
        }
      }
    }
  }
}

void FsManager::BackEndCheckMountPoint() {
  while (checkMountPointSleeper_.wait_for(
      std::chrono::seconds(option_.backEndThreadRunInterSec))) {
    CheckMountPoint();
    // update mount point metrics, when leader transfer or dingo-fuse abnormal
    // exit, metrics still valid
    UpdateFsMountMetrics();
  }
}

bool FsManager::CheckFsName(const std::string& fs_name) {
  static const std::regex kReg("^([a-z0-9]+\\-?)+$");
  if (!std::regex_match(fs_name.cbegin(), fs_name.cend(), kReg)) {
    LOG(ERROR) << "fsname is invalid, fsname = " << fs_name;
    return false;
  }
  return true;
}

FSStatusCode FsManager::CreateFs(const pb::mds::CreateFsRequest* request,
                                 pb::mds::FsInfo* fs_info) {
  const auto& fs_name = request->fsname();
  const auto& block_size = request->block_size();
  const auto& chunk_size = request->chunk_size();

  const auto& storage_type = request->storage_info().type();
  const auto& storage_info = request->storage_info();

  NameLockGuard lock(nameLock_, fs_name);
  FsInfoWrapper wrapper;
  bool skip_create_new_fs = false;

  // query fs
  // TODO(cw123): if fs status is FsStatus::New, here need more consideration
  if (fsStorage_->Exist(fs_name)) {
    int exist_ret = IsExactlySameOrCreateUnComplete(fs_name, block_size,
                                                    chunk_size, storage_info);
    if (exist_ret == 0) {
      LOG(INFO) << "CreateFs success, fs exist, fsName = " << fs_name
                << ", storage_type = " << StorageType_Name(storage_type)
                << ", blocksize = " << block_size
                << ", chunk_size = " << chunk_size
                << ", storage_info = " << storage_info.ShortDebugString();
      fsStorage_->Get(fs_name, &wrapper);
      *fs_info = wrapper.ProtoFsInfo();
      return FSStatusCode::OK;
    }

    if (exist_ret == 1) {
      LOG(INFO) << "CreateFs found previous create operation uncompleted"
                << ", fsName = " << fs_name
                << ", storage_type = " << StorageType_Name(storage_type)
                << ", blocksize = " << block_size
                << ", chunk_size = " << chunk_size
                << ", storage_info = " << storage_info.ShortDebugString();
      skip_create_new_fs = true;
    } else {
      return FSStatusCode::FS_EXIST;
    }
  }

  // check fsname
  if (!CheckFsName(fs_name)) {
    return FSStatusCode::FSNAME_INVALID;
  }

  // check s3info

  if (!skip_create_new_fs) {
    blockaccess::BlockAccessOptions block_access_opt =
        option_.block_access_option;
    FillBlockAccessOption(storage_info, &block_access_opt);

    std::unique_ptr<blockaccess::BlockAccesser> block_accesser =
        option_.block_accesser_factory->NewBlockAccesser(block_access_opt);
    Status s = block_accesser->Init();
    if (!s.ok()) {
      LOG(ERROR) << "Fail CreateFs " << fs_name
                 << " because of block accesser init error: " << s.ToString();
      return FSStatusCode::S3_INFO_ERROR;
    }

    if (!block_accesser->ContainerExist()) {
      LOG(ERROR) << "Fail CreateFs " << fs_name
                 << " because of container not exist";
      return FSStatusCode::S3_INFO_ERROR;
    }
  }

  if (!skip_create_new_fs) {
    uint64_t fs_id = fsStorage_->NextFsId();
    if (fs_id == INVALID_FS_ID) {
      LOG(ERROR) << "Generator fs id failed, fsName = " << fs_name;
      return FSStatusCode::INTERNAL_ERROR;
    }

    wrapper = FsInfoWrapper(request, fs_id, GetRootId());

    FSStatusCode ret = fsStorage_->Insert(wrapper);
    if (ret != FSStatusCode::OK) {
      LOG(ERROR) << "CreateFs fail, insert fs fail, fsName = " << fs_name
                 << ", ret = " << FSStatusCode_Name(ret);
      return ret;
    }
  } else {
    fsStorage_->Get(fs_name, &wrapper);
  }

  uint32_t uid = 0;                 // TODO(cw123)
  uint32_t gid = 0;                 // TODO(cw123)
  uint32_t mode = S_IFDIR | 01777;  // TODO(cw123)

  pb::common::PartitionInfo partition;
  std::set<std::string> addrs;

  // handle create fs error
  bool is_valid_time =
      (request->has_recycletimehour() && request->recycletimehour() != 0);
  std::unordered_map<FSStatusCode, std::string> error_map{
      {FSStatusCode::INSERT_ROOT_INODE_ERROR, "insert root inode fail"},
      {FSStatusCode::INSERT_MANAGE_INODE_FAIL, "insert trash inode fail"},
      {FSStatusCode::INSERT_DENTRY_FAIL, "insert trash dentry fail"},
      {FSStatusCode::UPDATE_FS_FAIL, "create trash inode fail"}};

  auto clean_up_on_create_fs_failure =
      [&](FSStatusCode root_status,
          FSStatusCode failure_stage) -> FSStatusCode {
    if (error_map.find(failure_stage) == error_map.end()) {
      return root_status;
    }

    FSStatusCode child_status = FSStatusCode::OK;
    switch (failure_stage) {
      case FSStatusCode::UPDATE_FS_FAIL:
        if (is_valid_time) {
          child_status = metaserverClient_->DeleteDentry(
              wrapper.GetFsId(), partition.poolid(), partition.copysetid(),
              partition.partitionid(), ROOTINODEID, RECYCLENAME, addrs);
          if (child_status != FSStatusCode::OK) {
            LOG(ERROR) << "CreateFs fail, " << error_map[failure_stage]
                       << ", then delete recycle dentry fail"
                       << ", fsName = " << fs_name
                       << ", ret = " << FSStatusCode_Name(child_status);
            return child_status;
          }
        }
        FALLTHROUGH_INTENDED;
      case FSStatusCode::INSERT_DENTRY_FAIL:
        if (is_valid_time) {
          child_status =
              metaserverClient_->DeleteInode(wrapper.GetFsId(), RECYCLEINODEID);
          if (child_status != FSStatusCode::OK) {
            LOG(ERROR) << "CreateFs fail, " << error_map[failure_stage]
                       << ", then delete recycle inode fail"
                       << ", fsName = " << fs_name
                       << ", ret = " << FSStatusCode_Name(child_status);
            return child_status;
          }
        }
        FALLTHROUGH_INTENDED;
      case FSStatusCode::INSERT_MANAGE_INODE_FAIL:
        child_status =
            metaserverClient_->DeleteInode(wrapper.GetFsId(), GetRootId());
        if (child_status != FSStatusCode::OK) {
          LOG(ERROR) << "CreateFs fail, " << error_map[failure_stage]
                     << ", then delete root inode fail"
                     << ", fsName = " << fs_name
                     << ", ret = " << FSStatusCode_Name(child_status);
          return child_status;
        }
        FALLTHROUGH_INTENDED;
      case FSStatusCode::INSERT_ROOT_INODE_ERROR:
        if (FSStatusCode::CREATE_PARTITION_ERROR != root_status) {
          if (TopoStatusCode::TOPO_OK !=
              topoManager_->DeletePartition(partition.partitionid())) {
            LOG(ERROR) << "CreateFs fail, " << error_map[failure_stage]
                       << ", then delete partition fail"
                       << ", fsName = " << fs_name << ", ret = "
                       << FSStatusCode_Name(
                              FSStatusCode::DELETE_PARTITION_ERROR);
            return FSStatusCode::DELETE_PARTITION_ERROR;
          }
        }

        child_status = fsStorage_->Delete(fs_name);
        if (child_status != FSStatusCode::OK) {
          LOG(ERROR) << "CreateFs fail, " << error_map[failure_stage]
                     << ", then delete fs fail, fsName = " << fs_name
                     << ", ret = " << FSStatusCode_Name(child_status);
          return child_status;
        }
        break;
      default:
        return root_status;
    }
    return root_status;
  };

  // create partition
  FSStatusCode ret = FSStatusCode::OK;
  TopoStatusCode topo_ret = topoManager_->CreatePartitionsAndGetMinPartition(
      wrapper.GetFsId(), &partition);
  if (TopoStatusCode::TOPO_OK != topo_ret) {
    LOG(ERROR) << "CreateFs fail, create partition fail"
               << ", fsId = " << wrapper.GetFsId();
    ret = FSStatusCode::CREATE_PARTITION_ERROR;
  } else {
    // get copyset members
    if (TopoStatusCode::TOPO_OK !=
        topoManager_->GetCopysetMembers(partition.poolid(),
                                        partition.copysetid(), &addrs)) {
      LOG(ERROR) << "CreateFs fail, get copyset members fail,"
                 << " poolId = " << partition.poolid()
                 << ", copysetId = " << partition.copysetid();
      ret = FSStatusCode::UNKNOWN_ERROR;
    } else {
      // create root inode
      ret = metaserverClient_->CreateRootInode(
          wrapper.GetFsId(), partition.poolid(), partition.copysetid(),
          partition.partitionid(), uid, gid, mode, addrs);
    }
  }
  if (ret != FSStatusCode::OK && ret != FSStatusCode::INODE_EXIST) {
    LOG(ERROR) << "CreateFs fail, "
               << error_map[FSStatusCode::INSERT_ROOT_INODE_ERROR]
               << ", fsName = " << fs_name
               << ", ret = " << FSStatusCode_Name(ret);
    // delete partition if created
    return clean_up_on_create_fs_failure(ret,
                                         FSStatusCode::INSERT_ROOT_INODE_ERROR);
  }

  // if trash time is not 0, create trash inode and dentry for fs
  if (is_valid_time) {
    ret = metaserverClient_->CreateManageInode(
        wrapper.GetFsId(), partition.poolid(), partition.copysetid(),
        partition.partitionid(), uid, gid, mode, ManageInodeType::TYPE_RECYCLE,
        addrs);
    if (ret != FSStatusCode::OK && ret != FSStatusCode::INODE_EXIST) {
      LOG(ERROR) << "CreateFs fail, "
                 << error_map[FSStatusCode::INSERT_MANAGE_INODE_FAIL]
                 << ", fsName = " << fs_name
                 << ", ret = " << FSStatusCode_Name(ret);
      return clean_up_on_create_fs_failure(
          ret, FSStatusCode::INSERT_MANAGE_INODE_FAIL);
    }

    ret = metaserverClient_->CreateDentry(wrapper.GetFsId(), partition.poolid(),
                                          partition.copysetid(),
                                          partition.partitionid(), ROOTINODEID,
                                          RECYCLENAME, RECYCLEINODEID, addrs);
    if (ret != FSStatusCode::OK) {
      LOG(ERROR) << "CreateFs fail, "
                 << error_map[FSStatusCode::INSERT_DENTRY_FAIL]
                 << ", fsName = " << fs_name
                 << ", ret = " << FSStatusCode_Name(ret);
      return clean_up_on_create_fs_failure(ret,
                                           FSStatusCode::INSERT_DENTRY_FAIL);
    }
  }

  wrapper.SetStatus(pb::mds::FsStatus::INITED);

  // for persistence consider
  ret = fsStorage_->Update(wrapper);
  if (ret != FSStatusCode::OK) {
    LOG(ERROR) << "CreateFs fail, " << error_map[FSStatusCode::UPDATE_FS_FAIL]
               << ", fsName = " << fs_name
               << ", ret = " << FSStatusCode_Name(ret);
    // delete recycle dentry and recyle inode if created
    return clean_up_on_create_fs_failure(ret, FSStatusCode::UPDATE_FS_FAIL);
  }

  *fs_info = std::move(wrapper).ProtoFsInfo();
  return FSStatusCode::OK;
}

FSStatusCode FsManager::DeleteFs(const std::string& fs_name) {
  NameLockGuard lock(nameLock_, fs_name);

  // 1. query fs
  FsInfoWrapper wrapper;
  FSStatusCode ret = fsStorage_->Get(fs_name, &wrapper);
  if (ret != FSStatusCode::OK) {
    LOG(WARNING) << "DeleteFs fail, get fs fail, fsName = " << fs_name
                 << ", errCode = " << FSStatusCode_Name(ret);
    return ret;
  }

  // 2. check mount point num
  if (!wrapper.IsMountPointEmpty()) {
    LOG(WARNING) << "DeleteFs fail, mount point exist, fsName = " << fs_name;
    for (auto& it : wrapper.MountPoints()) {
      LOG(WARNING) << "mountpoint [" << it.ShortDebugString() << "] exist";
    }
    return FSStatusCode::FS_BUSY;
  }

  // 3. check fs status
  pb::mds::FsStatus status = wrapper.GetStatus();
  if (status == pb::mds::FsStatus::NEW || status == pb::mds::FsStatus::INITED) {
    FsInfoWrapper new_wrapper = wrapper;
    // update fs status to deleting
    new_wrapper.SetStatus(pb::mds::FsStatus::DELETING);
    // change fs name to oldname+"_deleting_"+fsid+deletetime
    uint64_t now = ::dingofs::utils::TimeUtility::GetTimeofDaySec();
    new_wrapper.SetFsName(fs_name + "_deleting_" +
                          std::to_string(wrapper.GetFsId()) + "_" +
                          std::to_string(now));
    // for persistence consider
    ret = fsStorage_->Rename(wrapper, new_wrapper);
    if (ret != FSStatusCode::OK) {
      LOG(ERROR) << "DeleteFs fail, update fs to deleting and rename fail"
                 << ", fsName = " << fs_name
                 << ", ret = " << FSStatusCode_Name(ret);
      return ret;
    }
    return FSStatusCode::OK;
  } else if (status == pb::mds::FsStatus::DELETING) {
    LOG(WARNING) << "DeleteFs already in deleting, fsName = " << fs_name;
    return FSStatusCode::UNDER_DELETING;
  } else {
    LOG(ERROR) << "DeleteFs fs in wrong status, fsName = " << fs_name
               << ", fs status = " << FsStatus_Name(status);
    return FSStatusCode::UNKNOWN_ERROR;
  }

  return FSStatusCode::OK;
}

FSStatusCode FsManager::MountFs(const std::string& fs_name,
                                const Mountpoint& mountpoint,
                                pb::mds::FsInfo* fs_info) {
  NameLockGuard lock(nameLock_, fs_name);

  // query fs
  FsInfoWrapper wrapper;
  FSStatusCode ret = fsStorage_->Get(fs_name, &wrapper);
  if (ret != FSStatusCode::OK) {
    LOG(WARNING) << "MountFs fail, get fs fail, fsName = " << fs_name
                 << ", errCode = " << FSStatusCode_Name(ret);
    return ret;
  }

  // check fs status
  pb::mds::FsStatus status = wrapper.GetStatus();
  switch (status) {
    case pb::mds::FsStatus::NEW:
      LOG(WARNING) << "MountFs fs is not inited, fsName = " << fs_name;
      return FSStatusCode::NOT_INITED;
    case pb::mds::FsStatus::INITED:
      // inited status, go on process
      break;
    case pb::mds::FsStatus::DELETING:
      LOG(WARNING) << "MountFs fs is in deleting, fsName = " << fs_name;
      return FSStatusCode::UNDER_DELETING;
    default:
      LOG(ERROR) << "MountFs fs in wrong status, fsName = " << fs_name
                 << ", fs status = " << FsStatus_Name(status);
      return FSStatusCode::UNKNOWN_ERROR;
  }

  // check param
  if (!mountpoint.has_cto()) {
    LOG(WARNING) << "MountFs fail, mount point miss cto param, fsName = "
                 << fs_name << ", fs status = " << FsStatus_Name(status);
    return FSStatusCode::PARAM_ERROR;
  }

  // mount point conflict
  if (wrapper.IsMountPointConflict(mountpoint)) {
    LOG(WARNING) << "MountFs fail, mount point conflict, fsName = " << fs_name
                 << ", mountpoint = " << mountpoint.ShortDebugString();
    return FSStatusCode::MOUNT_POINT_CONFLICT;
  }

  // insert mountpoint
  wrapper.AddMountPoint(mountpoint);
  // for persistence consider
  ret = fsStorage_->Update(wrapper);
  if (ret != FSStatusCode::OK) {
    LOG(WARNING) << "MountFs fail, update fs fail, fsName = " << fs_name
                 << ", mountpoint = " << mountpoint.ShortDebugString()
                 << ", errCode = " << FSStatusCode_Name(ret);
    return ret;
  }
  // update client alive time
  UpdateClientAliveTime(mountpoint, fs_name, false);

  // convert fs info
  *fs_info = std::move(wrapper).ProtoFsInfo();

  return FSStatusCode::OK;
}

FSStatusCode FsManager::UmountFs(const std::string& fs_name,
                                 const Mountpoint& mountpoint) {
  NameLockGuard lock(nameLock_, fs_name);

  // 1. query fs
  FsInfoWrapper wrapper;
  FSStatusCode ret = fsStorage_->Get(fs_name, &wrapper);
  if (ret != FSStatusCode::OK) {
    LOG(WARNING) << "UmountFs fail, get fs fail, fsName = " << fs_name
                 << ", errCode = " << FSStatusCode_Name(ret);
    return ret;
  }

  // 2. umount
  if (!wrapper.IsMountPointExist(mountpoint)) {
    ret = FSStatusCode::MOUNT_POINT_NOT_EXIST;
    LOG(WARNING) << "UmountFs fail, mount point not exist, fsName = " << fs_name
                 << ", mountpoint = " << mountpoint.ShortDebugString()
                 << ", errCode = " << FSStatusCode_Name(ret);
    return ret;
  }

  ret = wrapper.DeleteMountPoint(mountpoint);
  if (ret != FSStatusCode::OK) {
    LOG(WARNING) << "UmountFs fail, delete mount point fail, fsName = "
                 << fs_name
                 << ", mountpoint = " << mountpoint.ShortDebugString()
                 << ", errCode = " << FSStatusCode_Name(ret);
    return ret;
  }

  std::string mountpath;
  MountPoint2Str(mountpoint, &mountpath);
  DeleteClientAliveTime(mountpath);

  // 4. update fs info
  // for persistence consider
  ret = fsStorage_->Update(wrapper);
  if (ret != FSStatusCode::OK) {
    LOG(WARNING) << "UmountFs fail, update fs fail, fsName = " << fs_name
                 << ", mountpoint = " << mountpoint.ShortDebugString()
                 << ", errCode = " << FSStatusCode_Name(ret);
    return ret;
  }

  return FSStatusCode::OK;
}

FSStatusCode FsManager::GetFsInfo(const std::string& fs_name,
                                  pb::mds::FsInfo* fs_info) {
  // 1. query fs
  FsInfoWrapper wrapper;
  FSStatusCode ret = fsStorage_->Get(fs_name, &wrapper);
  if (ret != FSStatusCode::OK) {
    LOG(WARNING) << "GetFsInfo fail, get fs fail, fsName = " << fs_name
                 << ", errCode = " << FSStatusCode_Name(ret);
    return ret;
  }

  *fs_info = wrapper.ProtoFsInfo();
  return FSStatusCode::OK;
}

FSStatusCode FsManager::GetFsInfo(uint32_t fs_id, pb::mds::FsInfo* fs_info) {
  // 1. query fs
  FsInfoWrapper wrapper;
  FSStatusCode ret = fsStorage_->Get(fs_id, &wrapper);
  if (ret != FSStatusCode::OK) {
    LOG(WARNING) << "GetFsInfo fail, get fs fail, fsId = " << fs_id
                 << ", errCode = " << FSStatusCode_Name(ret);
    return ret;
  }

  *fs_info = wrapper.ProtoFsInfo();
  return FSStatusCode::OK;
}

FSStatusCode FsManager::GetFsInfo(const std::string& fs_name, uint32_t fs_id,
                                  pb::mds::FsInfo* fs_info) {
  // 1. query fs by fsName
  FsInfoWrapper wrapper;
  FSStatusCode ret = fsStorage_->Get(fs_name, &wrapper);
  if (ret != FSStatusCode::OK) {
    LOG(WARNING) << "GetFsInfo fail, get fs fail, fsName = " << fs_name
                 << ", errCode = " << FSStatusCode_Name(ret);
    return ret;
  }

  // 2. check fsId
  if (wrapper.GetFsId() != fs_id) {
    LOG(WARNING) << "GetFsInfo fail, fsId missmatch, fsName = " << fs_name
                 << ", param fsId = " << fs_id
                 << ", fsInfo.fsId = " << wrapper.GetFsId();
    return FSStatusCode::PARAM_ERROR;
  }

  *fs_info = wrapper.ProtoFsInfo();
  return FSStatusCode::OK;
}

int FsManager::IsExactlySameOrCreateUnComplete(
    const std::string& fs_name, uint64_t block_size, uint64_t chunk_size,
    const pb::common::StorageInfo& storage_info) {
  FsInfoWrapper exist_fs;

  const auto& storage_type = storage_info.type();
  auto check_fs_info = [storage_type](const pb::common::StorageInfo& lhs,
                                      const pb::common::StorageInfo& rhs) {
    switch (storage_type) {
      case pb::common::StorageType::TYPE_S3:
        return MessageDifferencer::Equals(lhs.s3_info(), rhs.s3_info());
      case pb::common::StorageType::TYPE_RADOS:
        return MessageDifferencer::Equals(lhs.rados_info(), rhs.rados_info());
      default:
        LOG(ERROR) << "unknown storage type: " << storage_type;
        return false;
    }

    return false;
  };

  // assume fsname exists
  fsStorage_->Get(fs_name, &exist_fs);
  if (fs_name == exist_fs.GetFsName() &&
      storage_type == exist_fs.GetStorageType() &&
      block_size == exist_fs.GetBlockSize() &&
      chunk_size == exist_fs.GetChunkSize() &&
      check_fs_info(storage_info, exist_fs.GetStorageInfo())) {
    if (pb::mds::FsStatus::NEW == exist_fs.GetStatus()) {
      return 1;
    } else if (pb::mds::FsStatus::INITED == exist_fs.GetStatus()) {
      return 0;
    }
  }
  return -1;
}

uint64_t FsManager::GetRootId() { return ROOTINODEID; }

void FsManager::GetAllFsInfo(
    ::google::protobuf::RepeatedPtrField<pb::mds::FsInfo>* fs_info_vec) {
  std::vector<FsInfoWrapper> wrapper_vec;
  fsStorage_->GetAll(&wrapper_vec);
  for (auto const& i : wrapper_vec) {
    *fs_info_vec->Add() = i.ProtoFsInfo();
  }
  LOG(INFO) << "get all fsinfo.";
}

void FsManager::RefreshSession(const pb::mds::RefreshSessionRequest* request,
                               pb::mds::RefreshSessionResponse* response) {
  if (request->txids_size() != 0) {
    std::vector<pb::mds::topology::PartitionTxId> out;
    std::vector<pb::mds::topology::PartitionTxId> in = {
        request->txids().begin(), request->txids().end()};
    topoManager_->GetLatestPartitionsTxId(in, &out);
    *response->mutable_latesttxidlist() = {std::make_move_iterator(out.begin()),
                                           std::make_move_iterator(out.end())};
  }

  // update this client's alive time
  UpdateClientAliveTime(request->mountpoint(), request->fsname());
  FsInfoWrapper wrapper;
  FSStatusCode ret = fsStorage_->Get(request->fsname(), &wrapper);
  if (ret != FSStatusCode::OK) {
    LOG(WARNING) << "GetFsInfo fail, get fs fail, fsName = "
                 << request->fsname()
                 << ", errCode = " << FSStatusCode_Name(ret);
    return;
  }

  auto fs_info = wrapper.ProtoFsInfo();
  // update mount_count metrics
  FsMetric::GetInstance().OnUpdateMountCount(fs_info.fsname(),
                                             fs_info.mountnum());
}

void FsManager::GetLatestTxId(
    const uint32_t fs_id,
    std::vector<pb::mds::topology::PartitionTxId>* tx_ids) {
  std::list<pb::common::PartitionInfo> list;
  topoManager_->ListPartitionOfFs(fs_id, &list);
  for (const auto& item : list) {
    pb::mds::topology::PartitionTxId partition_tx_id;
    partition_tx_id.set_partitionid(item.partitionid());
    partition_tx_id.set_txid(item.txid());
    tx_ids->push_back(std::move(partition_tx_id));
  }
}

FSStatusCode FsManager::IncreaseFsTxSequence(const std::string& fs_name,
                                             const std::string& owner,
                                             uint64_t* sequence) {
  FsInfoWrapper wrapper;
  FSStatusCode rc = fsStorage_->Get(fs_name, &wrapper);
  if (rc != FSStatusCode::OK) {
    LOG(WARNING) << "Increase fs transaction sequence fail, fsName=" << fs_name
                 << ", retCode=" << FSStatusCode_Name(rc);
    return rc;
  }

  *sequence = wrapper.IncreaseFsTxSequence(owner);
  rc = fsStorage_->Update(wrapper);
  if (rc != FSStatusCode::OK) {
    LOG(WARNING) << "Increase fs transaction sequence fail, fsName=" << fs_name
                 << ", retCode=" << FSStatusCode_Name(rc);
    return rc;
  }

  return rc;
}

FSStatusCode FsManager::GetFsTxSequence(const std::string& fs_name,
                                        uint64_t* sequence) {
  FsInfoWrapper wrapper;
  FSStatusCode rc = fsStorage_->Get(fs_name, &wrapper);
  if (rc != FSStatusCode::OK) {
    LOG(WARNING) << "Get fs transaction sequence fail, fsName=" << fs_name
                 << ", retCode=" << FSStatusCode_Name(rc);
    return rc;
  }

  *sequence = wrapper.GetFsTxSequence();
  return rc;
}

void FsManager::GetLatestTxId(const pb::mds::GetLatestTxIdRequest* request,
                              pb::mds::GetLatestTxIdResponse* response) {
  std::vector<pb::mds::topology::PartitionTxId> tx_ids;
  if (!request->has_fsid()) {
    response->set_statuscode(FSStatusCode::PARAM_ERROR);
    LOG(ERROR) << "Bad GetLatestTxId request which missing fsid"
               << ", request=" << request->DebugString();
    return;
  }

  uint32_t fs_id = request->fsid();
  if (!request->lock()) {
    GetLatestTxId(fs_id, &tx_ids);
    response->set_statuscode(FSStatusCode::OK);
    *response->mutable_txids() = {tx_ids.begin(), tx_ids.end()};
    return;
  }

  // lock for multi-mount rename
  FSStatusCode rc;
  const std::string& fs_name = request->fsname();
  const std::string& uuid = request->uuid();
  LOCK_STATUS status = dlock_->Lock(fs_name, uuid);
  if (status != LOCK_STATUS::OK) {
    rc = (status == LOCK_STATUS::TIMEOUT) ? FSStatusCode::LOCK_TIMEOUT
                                          : FSStatusCode::LOCK_FAILED;
    response->set_statuscode(rc);
    LOG(WARNING) << "DLock lock failed, fsName=" << fs_name << ", uuid=" << uuid
                 << ", retCode=" << FSStatusCode_Name(rc);
    return;
  }

  // status = LOCK_STATUS::OK
  NameLockGuard lock(nameLock_, fs_name);
  if (!dlock_->CheckOwner(fs_name, uuid)) {  // double check
    LOG(WARNING) << "DLock lock failed for owner transfer"
                 << ", fsName=" << fs_name << ", owner=" << uuid;
    response->set_statuscode(FSStatusCode::LOCK_FAILED);
    return;
  }

  uint64_t tx_sequence;
  rc = IncreaseFsTxSequence(fs_name, uuid, &tx_sequence);
  if (rc == FSStatusCode::OK) {
    GetLatestTxId(fs_id, &tx_ids);
    *response->mutable_txids() = {tx_ids.begin(), tx_ids.end()};
    response->set_txsequence(tx_sequence);
    LOG(INFO) << "Acquire dlock success, fsName=" << fs_name
              << ", uuid=" << uuid << ", txSequence=" << tx_sequence;
  } else {
    LOG(ERROR) << "Increase fs txSequence failed";
  }
  response->set_statuscode(rc);
}

void FsManager::CommitTx(const pb::mds::CommitTxRequest* request,
                         pb::mds::CommitTxResponse* response) {
  std::vector<pb::mds::topology::PartitionTxId> tx_ids = {
      request->partitiontxids().begin(),
      request->partitiontxids().end(),
  };
  if (!request->lock()) {
    if (topoManager_->CommitTxId(tx_ids) == TopoStatusCode::TOPO_OK) {
      response->set_statuscode(FSStatusCode::OK);
    } else {
      LOG(ERROR) << "Commit txid failed";
      response->set_statuscode(FSStatusCode::UNKNOWN_ERROR);
    }
    return;
  }

  // lock for multi-mountpoints
  FSStatusCode rc;
  const std::string& fs_name = request->fsname();
  const std::string& uuid = request->uuid();
  LOCK_STATUS status = dlock_->Lock(fs_name, uuid);
  if (status != LOCK_STATUS::OK) {
    rc = (status == LOCK_STATUS::TIMEOUT) ? FSStatusCode::LOCK_TIMEOUT
                                          : FSStatusCode::LOCK_FAILED;
    LOG(WARNING) << "DLock lock failed, fsName=" << fs_name << ", uuid=" << uuid
                 << ", retCode=" << FSStatusCode_Name(rc);
    response->set_statuscode(rc);
    return;
  }

  // status = LOCK_STATUS::OK
  {
    NameLockGuard lock(nameLock_, fs_name);
    if (!dlock_->CheckOwner(fs_name, uuid)) {  // double check
      LOG(WARNING) << "DLock lock failed for owner transfer"
                   << ", fsName=" << fs_name << ", owner=" << uuid;
      response->set_statuscode(FSStatusCode::LOCK_FAILED);
      return;
    }

    // txSequence mismatch
    uint64_t tx_sequence;
    rc = GetFsTxSequence(fs_name, &tx_sequence);
    if (rc != FSStatusCode::OK) {
      LOG(ERROR) << "Get fs tx sequence failed";
      response->set_statuscode(rc);
      return;
    } else if (tx_sequence != request->txsequence()) {
      LOG(ERROR) << "Commit tx with txSequence mismatch, fsName=" << fs_name
                 << ", uuid=" << uuid << ", current txSequence=" << tx_sequence
                 << ", commit txSequence=" << request->txsequence();
      response->set_statuscode(FSStatusCode::COMMIT_TX_SEQUENCE_MISMATCH);
      return;
    }

    // commit txId
    if (topoManager_->CommitTxId(tx_ids) == TopoStatusCode::TOPO_OK) {
      response->set_statuscode(FSStatusCode::OK);
    } else {
      LOG(ERROR) << "Commit txid failed";
      response->set_statuscode(FSStatusCode::UNKNOWN_ERROR);
    }
  }

  // we can ignore the UnLock result for the
  // lock can releaseed automaticlly by timeout
  dlock_->UnLock(fs_name, uuid);
}

// set fs cluster statistics
void FsManager::SetFsStats(const pb::mds::SetFsStatsRequest* request,
                           pb::mds::SetFsStatsResponse* response) {
  FsMetric::GetInstance().SetFsStats(request->fsname(), request->fsstatsdata());
  response->set_statuscode(FSStatusCode::OK);
}

// get fs cluster statistics
void FsManager::GetFsStats(const pb::mds::GetFsStatsRequest* request,
                           pb::mds::GetFsStatsResponse* response) {
  FSStatusCode ret = FsMetric::GetInstance().GetFsStats(
      request->fsname(), response->mutable_fsstatsdata());
  response->set_statuscode(ret);
}

// get fs cluster persecond statistics
void FsManager::GetFsPerSecondStats(
    const pb::mds::GetFsPerSecondStatsRequest* request,
    pb::mds::GetFsPerSecondStatsResponse* response) {
  FSStatusCode ret = FsMetric::GetInstance().GetFsPerSecondStats(
      request->fsname(), response->mutable_fsstatsdata());
  response->set_statuscode(ret);
}

// after mds restart need rebuild mountpoint ttl recorder
void FsManager::RebuildTimeRecorder() {
  std::vector<FsInfoWrapper> fs_infos;
  fsStorage_->GetAll(&fs_infos);
  for (auto const& info : fs_infos) {
    for (auto const& mount : info.MountPoints()) {
      UpdateClientAliveTime(mount, info.GetFsName(), false);
    }
  }
  LOG(INFO) << "RebuildTimeRecorder size = " << mpTimeRecorder_.size();
}

FSStatusCode FsManager::AddMountPoint(const Mountpoint& mountpoint,
                                      const std::string& fs_name) {
  LOG(INFO) << "AddMountPoint mountpoint = " << mountpoint.DebugString()
            << ", fsName = " << fs_name;
  // 1. query fs
  FsInfoWrapper wrapper;
  FSStatusCode ret = fsStorage_->Get(fs_name, &wrapper);
  if (ret != FSStatusCode::OK) {
    LOG(WARNING) << "AddMountPoint fail, get fs fail, fsName = " << fs_name
                 << ", errCode = " << FSStatusCode_Name(ret);
    return ret;
  }

  // 2. insert mountpoint
  wrapper.AddMountPoint(mountpoint);
  // for persistence consider
  ret = fsStorage_->Update(wrapper);
  if (ret != FSStatusCode::OK) {
    LOG(WARNING) << "AddMountPoint update fs fail, fsName = " << fs_name
                 << ", mountpoint = " << mountpoint.ShortDebugString()
                 << ", errCode = " << FSStatusCode_Name(ret);
    return ret;
  }

  return FSStatusCode::OK;
}

void FsManager::UpdateClientAliveTime(const Mountpoint& mountpoint,
                                      const std::string& fs_name,
                                      bool add_mount_point) {
  VLOG(1) << "UpdateClientAliveTime fsName = " << fs_name
          << ", mp = " << mountpoint.DebugString()
          << ". addMountPoint = " << add_mount_point;
  std::string mountpath;
  MountPoint2Str(mountpoint, &mountpath);
  WriteLockGuard wlock(recorderMutex_);
  if (add_mount_point) {
    auto iter = mpTimeRecorder_.find(mountpath);
    // client hang timeout and recover later
    // need add mountpoint to fsInfo
    if (iter == mpTimeRecorder_.end()) {
      if (AddMountPoint(mountpoint, fs_name) != FSStatusCode::OK) {
        return;
      }
    }
  }
  mpTimeRecorder_[mountpath] =
      std::make_pair(fs_name, ::dingofs::utils::TimeUtility::GetTimeofDaySec());
}

void FsManager::DeleteClientAliveTime(const std::string& mountpoint) {
  WriteLockGuard wlock(recorderMutex_);
  auto it = mpTimeRecorder_.find(mountpoint);
  if (it != mpTimeRecorder_.end()) {
    mpTimeRecorder_.erase(it);
  }
}

// for utest
bool FsManager::GetClientAliveTime(const std::string& mountpoint,
                                   std::pair<std::string, uint64_t>* out) {
  ReadLockGuard rlock(recorderMutex_);
  auto iter = mpTimeRecorder_.find(mountpoint);
  if (iter == mpTimeRecorder_.end()) {
    return false;
  }

  *out = iter->second;
  return true;
}

void FsManager::UpdateFsMountMetrics() {
  std::vector<FsInfoWrapper> fs_info_wrappers;
  fsStorage_->GetAll(&fs_info_wrappers);
  for (auto const& wrapper : fs_info_wrappers) {
    auto fs_info = wrapper.ProtoFsInfo();
    // update mount_count metrics
    FsMetric::GetInstance().OnUpdateMountCount(fs_info.fsname(),
                                               fs_info.mountnum());
  }
}

}  // namespace mds
}  // namespace dingofs
