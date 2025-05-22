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
 * Created Date: 2021-08-31
 * Author: xuchaojie
 */

#include "metaserver/trash/trash.h"

#include <memory>

#include "common/config_mapper.h"
#include "dingofs/mds.pb.h"
#include "dingofs/metaserver.pb.h"
#include "metaserver/storage/converter.h"
#include "utils/timeutility.h"

using ::dingofs::utils::TimeUtility;

namespace dingofs {
namespace metaserver {

using pb::mds::FsInfo;
using pb::mds::FSStatusCode;
using pb::metaserver::Inode;
using pb::metaserver::MetaStatusCode;
using storage::Key4Inode;
using utils::Configuration;
using utils::LockGuard;

void TrashOption::InitTrashOptionFromConf(std::shared_ptr<Configuration> conf) {
  conf->GetValueFatalIfFail("trash.scanPeriodSec", &scanPeriodSec);
  conf->GetValueFatalIfFail("trash.expiredAfterSec", &expiredAfterSec);
}

void TrashImpl::Init(const TrashOption& option) {
  options_ = option;
  mdsClient_ = option.mdsClient;
  isStop_ = false;
}

void TrashImpl::Add(uint32_t fsId, uint64_t inodeId, uint32_t dtime) {
  TrashItem item;
  item.fsId = fsId;
  item.inodeId = inodeId;
  item.dtime = dtime;

  LockGuard lg(itemsMutex_);
  if (isStop_) {
    return;
  }
  trashItems_.push_back(item);
  VLOG(6) << "Add Trash Item success, item.fsId = " << item.fsId
          << ", item.inodeId = " << item.inodeId
          << ", item.dtime = " << item.dtime;
}

void TrashImpl::ScanTrash() {
  LockGuard lgScan(scanMutex_);
  std::list<TrashItem> temp;
  {
    LockGuard lgItems(itemsMutex_);
    trashItems_.swap(temp);
  }

  for (auto it = temp.begin(); it != temp.end();) {
    if (isStop_) {
      return;
    }
    if (NeedDelete(*it)) {
      MetaStatusCode ret = DeleteInodeAndData(*it);
      if (MetaStatusCode::NOT_FOUND == ret) {
        it = temp.erase(it);
        continue;
      }
      if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "DeleteInodeAndData fail, fsId = " << it->fsId
                   << ", inodeId = " << it->inodeId
                   << ", ret = " << MetaStatusCode_Name(ret);
        it++;
        continue;
      }
      VLOG(6) << "Trash Delete Inode, fsId = " << it->fsId
              << ", inodeId = " << it->inodeId;
      it = temp.erase(it);
    } else {
      it++;
    }
  }

  {
    LockGuard lgItems(itemsMutex_);
    trashItems_.splice(trashItems_.end(), temp);
  }
}

void TrashImpl::StopScan() { isStop_ = true; }

bool TrashImpl::IsStop() { return isStop_; }

bool TrashImpl::NeedDelete(const TrashItem& item) {
  uint32_t now = TimeUtility::GetTimeofDaySec();
  Inode inode;
  MetaStatusCode ret =
      inodeStorage_->Get(Key4Inode(item.fsId, item.inodeId), &inode);
  if (MetaStatusCode::NOT_FOUND == ret) {
    LOG(WARNING) << "GetInode find inode not exist, fsId = " << item.fsId
                 << ", inodeId = " << item.inodeId
                 << ", ret = " << MetaStatusCode_Name(ret);
    return true;
  } else if (ret != MetaStatusCode::OK) {
    LOG(WARNING) << "GetInode fail, fsId = " << item.fsId
                 << ", inodeId = " << item.inodeId
                 << ", ret = " << MetaStatusCode_Name(ret);
    return false;
  }

  // for compatibility, if fs recycleTimeHour is 0, use old trash logic
  // if fs recycleTimeHour is 0, use trash wait until expiredAfterSec
  // if fs recycleTimeHour is not 0, return true
  uint64_t recycleTimeHour = GetFsRecycleTimeHour(item.fsId);
  if (recycleTimeHour == 0) {
    return ((now - item.dtime) >= options_.expiredAfterSec);
  } else {
    return true;
  }
}

uint64_t TrashImpl::GetFsRecycleTimeHour(uint32_t fsId) {
  FsInfo fsInfo;
  uint64_t recycleTimeHour = 0;
  if (fsInfoMap_.find(fsId) == fsInfoMap_.end()) {
    auto ret = mdsClient_->GetFsInfo(fsId, &fsInfo);
    if (ret != FSStatusCode::OK) {
      if (FSStatusCode::NOT_FOUND == ret) {
        LOG(ERROR) << "The fs not exist, fsId = " << fsId;
        return 0;
      } else {
        LOG(ERROR) << "GetFsInfo failed, FSStatusCode = " << ret
                   << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                   << ", fsId = " << fsId;
        return 0;
      }
    }
    fsInfoMap_.insert({fsId, fsInfo});
  } else {
    fsInfo = fsInfoMap_.find(fsId)->second;
  }

  if (fsInfo.has_recycletimehour()) {
    recycleTimeHour = fsInfo.recycletimehour();
  } else {
    recycleTimeHour = 0;
  }
  return recycleTimeHour;
}

MetaStatusCode TrashImpl::DeleteInodeAndData(const TrashItem& item) {
  Inode inode;
  MetaStatusCode ret =
      inodeStorage_->Get(Key4Inode(item.fsId, item.inodeId), &inode);
  if (ret != MetaStatusCode::OK) {
    LOG(WARNING) << "GetInode fail, fsId = " << item.fsId
                 << ", inodeId = " << item.inodeId
                 << ", ret = " << MetaStatusCode_Name(ret);
    return ret;
  }

  {
    // get s3info from mds
    FsInfo fsInfo;
    if (fsInfoMap_.find(item.fsId) == fsInfoMap_.end()) {
      auto ret = mdsClient_->GetFsInfo(item.fsId, &fsInfo);
      if (ret != FSStatusCode::OK) {
        if (FSStatusCode::NOT_FOUND == ret) {
          LOG(ERROR) << "The fsName not exist, fsId = " << item.fsId;
          return MetaStatusCode::S3_DELETE_ERR;
        } else {
          LOG(ERROR) << "GetFsInfo failed, FSStatusCode = " << ret
                     << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                     << ", fsId = " << item.fsId;
          return MetaStatusCode::S3_DELETE_ERR;
        }
      }
      fsInfoMap_.insert({item.fsId, fsInfo});
    } else {
      fsInfo = fsInfoMap_.find(item.fsId)->second;
    }

    ret = inodeStorage_->PaddingInodeS3ChunkInfo(
        item.fsId, item.inodeId, inode.mutable_s3chunkinfomap());
    if (ret != MetaStatusCode::OK) {
      LOG(ERROR) << "GetInode chunklist fail, fsId = " << item.fsId
                 << ", inodeId = " << item.inodeId
                 << ", retCode = " << MetaStatusCode_Name(ret);
      return ret;
    }

    if (inode.s3chunkinfomap().empty()) {
      LOG(WARNING) << "GetInode chunklist empty, fsId = " << item.fsId
                   << ", inodeId = " << item.inodeId;
      return MetaStatusCode::NOT_FOUND;
    }

    VLOG(9) << "Try DeleteInodeAndData, inode: " << inode.ShortDebugString();

    S3ClientAdaptorOption client_adaptor_option =
        options_.s3_client_adaptor_option;
    client_adaptor_option.blockSize = fsInfo.block_size();
    client_adaptor_option.chunkSize = fsInfo.chunk_size();
    client_adaptor_option.block_accesser_factory =
        options_.block_accesser_factory;

    blockaccess::BlockAccessOptions block_access_options =
        options_.block_access_options;
    FillBlockAccessOption(fsInfo.storage_info(), &block_access_options);

    auto client_aptaptor = std::make_unique<S3ClientAdaptorImpl>();
    Status s =
        client_aptaptor->Init(client_adaptor_option, block_access_options);
    if (!s.ok()) {
      LOG(ERROR) << "Fail init S3ClientAdaptor, fs_id: " << item.fsId
                 << ", ino: " << item.inodeId << ", status: " << s.ToString();
      return MetaStatusCode::PARAM_ERROR;
    }

    s = client_aptaptor->Delete(inode);
    if (!s.ok()) {
      LOG(ERROR) << "Fail delete inode data, fs_id: " << item.fsId
                 << ", ino: " << item.inodeId << ", status: " << s.ToString();
      return MetaStatusCode::S3_DELETE_ERR;
    }
  }

  ret = inodeStorage_->Delete(Key4Inode(item.fsId, item.inodeId));
  if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
    LOG(ERROR) << "Delete Inode fail, fsId = " << item.fsId
               << ", inodeId = " << item.inodeId
               << ", ret = " << MetaStatusCode_Name(ret);
    return ret;
  }
  return MetaStatusCode::OK;
}

void TrashImpl::ListItems(std::list<TrashItem>* items) {
  LockGuard lgScan(scanMutex_);
  LockGuard lgItems(itemsMutex_);
  *items = trashItems_;
}

}  // namespace metaserver
}  // namespace dingofs
