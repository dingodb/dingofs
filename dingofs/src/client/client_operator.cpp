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
 * Project: Curve
 * Created Date: 2021-09-11
 * Author: Jingli Chen (Wine93)
 */

#include "client/client_operator.h"

#include <cstdint>
#include <list>

#include "proto/metaserver.pb.h"
#include "client/filesystem/error.h"
#include "client/filesystem/filesystem.h"
#include "utils/uuid.h"

namespace dingofs {
namespace client {

using client::filesystem::FileSystem;
using client::filesystem::ToFSError;
using stub::rpcclient::MdsClient;
using stub::rpcclient::MetaServerClient;
using utils::UUIDGenerator;

using pb::mds::FSStatusCode;
using pb::mds::topology::PartitionTxId;
using pb::metaserver::Dentry;
using pb::metaserver::DentryFlag;
using pb::metaserver::FsFileType;
using pb::metaserver::InodeAttr;
using pb::metaserver::MetaStatusCode;

#define LOG_ERROR(action, rc)                             \
  LOG(ERROR) << (action) << " failed, retCode = " << (rc) \
             << ", DebugString = " << DebugString();

RenameOperator::RenameOperator(
    uint32_t fs_id, const std::string& fs_name, uint64_t parent_id,
    std::string name, uint64_t new_parent_id, std::string newname,
    std::shared_ptr<DentryCacheManager> dentry_manager,
    std::shared_ptr<InodeCacheManager> inode_manager,
    std::shared_ptr<MetaServerClient> meta_client,
    std::shared_ptr<MdsClient> mds_client, bool enable_parallel)
    : fsId_(fs_id),
      fsName_(fs_name),
      parentId_(parent_id),
      name_(name),
      newParentId_(new_parent_id),
      newname_(newname),
      srcPartitionId_(0),
      dstPartitionId_(0),
      srcTxId_(0),
      dstTxId_(0),
      oldInodeId_(0),
      oldInodeSize_(-1),
      dentryManager_(dentry_manager),
      inodeManager_(inode_manager),
      metaClient_(meta_client),
      mdsClient_(mds_client),
      enableParallel_(enable_parallel),
      sequence_(0) {}

std::string RenameOperator::DebugString() {
  std::ostringstream os;
  os << "( fsId = " << fsId_ << ", fsName = " << fsName_
     << ", parentId = " << parentId_ << ", name = " << name_
     << ", newParentId = " << newParentId_ << ", newname = " << newname_
     << ", srcPartitionId = " << srcPartitionId_
     << ", dstPartitionId = " << dstPartitionId_ << ", srcTxId = " << srcTxId_
     << ", dstTxId_ = " << dstTxId_ << ", oldInodeId = " << oldInodeId_
     << ", srcDentry = [" << srcDentry_.ShortDebugString() << "]"
     << ", dstDentry = [" << dstDentry_.ShortDebugString() << "]"
     << ", prepare dentry = [" << dentry_.ShortDebugString() << "]"
     << ", prepare new dentry = [" << newDentry_.ShortDebugString() << "]"
     << ", enableParallel = " << enableParallel_ << ", uuid = " << uuid_
     << ", sequence = " << sequence_ << ")";
  return os.str();
}

DINGOFS_ERROR RenameOperator::GetTxId(uint32_t fs_id, uint64_t inode_id,
                                      uint32_t* partition_id, uint64_t* tx_id) {
  auto rc = metaClient_->GetTxId(fs_id, inode_id, partition_id, tx_id);
  if (rc != MetaStatusCode::OK) {
    LOG_ERROR("GetTxId", rc);
  }
  return ToFSError(rc);
}

DINGOFS_ERROR RenameOperator::GetLatestTxIdWithLock() {
  std::vector<PartitionTxId> tx_ids;
  uuid_ = UUIDGenerator().GenerateUUID();
  auto rc = mdsClient_->GetLatestTxIdWithLock(fsId_, fsName_, uuid_, &tx_ids,
                                              &sequence_);
  if (rc != FSStatusCode::OK) {
    return DINGOFS_ERROR::INTERNAL;
  }

  for (const auto& item : tx_ids) {
    SetTxId(item.partitionid(), item.txid());
  }
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR RenameOperator::GetTxId() {
  DINGOFS_ERROR rc;
  if (enableParallel_) {
    rc = GetLatestTxIdWithLock();
    if (rc != DINGOFS_ERROR::OK) {
      LOG_ERROR("GetLatestTxIdWithLock", rc);
      return rc;
    }
  }

  rc = GetTxId(fsId_, parentId_, &srcPartitionId_, &srcTxId_);
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("GetTxId", rc);
    return rc;
  }

  rc = GetTxId(fsId_, newParentId_, &dstPartitionId_, &dstTxId_);
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("GetTxId", rc);
  }

  return rc;
}

void RenameOperator::SetTxId(uint32_t partition_id, uint64_t tx_id) {
  metaClient_->SetTxId(partition_id, tx_id);
}

// TODO(Wine93): we should improve the check for whether a directory is empty
DINGOFS_ERROR RenameOperator::CheckOverwrite() {
  if (dstDentry_.flag() & DentryFlag::TYPE_FILE_FLAG) {
    return DINGOFS_ERROR::OK;
  }

  std::list<Dentry> dentrys;
  auto rc = dentryManager_->ListDentry(dstDentry_.inodeid(), &dentrys, 1);
  if (rc == DINGOFS_ERROR::OK && !dentrys.empty()) {
    LOG(ERROR) << "The directory is not empty" << ", dentry = ("
               << dstDentry_.ShortDebugString() << ")";
    rc = DINGOFS_ERROR::NOTEMPTY;
  }

  return rc;
}

// The rename operate must met the following 2 conditions:
//   (1) the source dentry must exist
//   (2) if the target dentry exist then it must be file or an empty directory
DINGOFS_ERROR RenameOperator::Precheck() {
  auto rc = dentryManager_->GetDentry(parentId_, name_, &srcDentry_);
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("GetDentry", rc);
    return rc;
  }

  rc = dentryManager_->GetDentry(newParentId_, newname_, &dstDentry_);
  if (rc == DINGOFS_ERROR::NOTEXIST) {
    return DINGOFS_ERROR::OK;
  } else if (rc == DINGOFS_ERROR::OK) {
    oldInodeId_ = dstDentry_.inodeid();
    return CheckOverwrite();
  }

  LOG_ERROR("GetDentry", rc);
  return rc;
}

DINGOFS_ERROR RenameOperator::RecordSrcInodeInfo() {
  DINGOFS_ERROR rc =
      inodeManager_->GetInodeAttr(srcDentry_.inodeid(), &src_inode_attr_);
  if (rc == DINGOFS_ERROR::OK) {
    return DINGOFS_ERROR::OK;
  } else {
    LOG_ERROR("GetInode", rc);
    return DINGOFS_ERROR::NOTEXIST;
  }
}

// record old inode info if overwrite
DINGOFS_ERROR RenameOperator::RecordOldInodeInfo() {
  if (oldInodeId_ != 0) {
    InodeAttr attr;
    DINGOFS_ERROR rc = inodeManager_->GetInodeAttr(oldInodeId_, &attr);
    if (rc == DINGOFS_ERROR::OK) {
      oldInodeSize_ = attr.length();
      oldInodeType_ = attr.type();
    } else {
      LOG_ERROR("GetInode", rc);
      return DINGOFS_ERROR::NOTEXIST;
    }
  }

  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR RenameOperator::PrepareRenameTx(
    const std::vector<Dentry>& dentrys) {
  auto rc = metaClient_->PrepareRenameTx(dentrys);
  if (rc != MetaStatusCode::OK) {
    LOG_ERROR("PrepareRenameTx", rc);
  }

  return ToFSError(rc);
}

DINGOFS_ERROR RenameOperator::PrepareTx() {
  dentry_ = Dentry(srcDentry_);
  dentry_.set_txid(srcTxId_ + 1);
  dentry_.set_txsequence(sequence_);
  dentry_.set_flag(dentry_.flag() | DentryFlag::DELETE_MARK_FLAG |
                   DentryFlag::TRANSACTION_PREPARE_FLAG);
  dentry_.set_type(srcDentry_.type());

  newDentry_ = Dentry(srcDentry_);
  newDentry_.set_parentinodeid(newParentId_);
  newDentry_.set_name(newname_);
  newDentry_.set_txid(dstTxId_ + 1);
  newDentry_.set_txsequence(sequence_);
  newDentry_.set_flag(newDentry_.flag() | DentryFlag::TRANSACTION_PREPARE_FLAG);
  newDentry_.set_type(srcDentry_.type());

  DINGOFS_ERROR rc;
  std::vector<Dentry> dentrys{dentry_};
  if (srcPartitionId_ == dstPartitionId_) {
    dentrys.push_back(newDentry_);
    rc = PrepareRenameTx(dentrys);
  } else {
    rc = PrepareRenameTx(dentrys);
    if (rc == DINGOFS_ERROR::OK) {
      dentrys[0] = newDentry_;
      rc = PrepareRenameTx(dentrys);
    }
  }

  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("PrepareTx", rc);
  }
  return rc;
}

DINGOFS_ERROR RenameOperator::CommitTx() {
  PartitionTxId partition_tx_id;
  std::vector<PartitionTxId> tx_ids;

  partition_tx_id.set_partitionid(srcPartitionId_);
  partition_tx_id.set_txid(srcTxId_ + 1);
  tx_ids.push_back(partition_tx_id);

  if (srcPartitionId_ != dstPartitionId_) {
    partition_tx_id.set_partitionid(dstPartitionId_);
    partition_tx_id.set_txid(dstTxId_ + 1);
    tx_ids.push_back(partition_tx_id);
  }

  FSStatusCode rc;
  if (enableParallel_) {
    rc = mdsClient_->CommitTxWithLock(tx_ids, fsName_, uuid_, sequence_);
  } else {
    rc = mdsClient_->CommitTx(tx_ids);
  }

  if (rc != FSStatusCode::OK) {
    LOG_ERROR("CommitTx", rc);
    return DINGOFS_ERROR::INTERNAL;
  }
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR RenameOperator::LinkInode(uint64_t inode_id, uint64_t parent) {
  std::shared_ptr<InodeWrapper> inode_wrapper;
  auto rc = inodeManager_->GetInode(inode_id, inode_wrapper);
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("GetInode", rc);
    return rc;
  }

  rc = inode_wrapper->Link(parent);
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("Link", rc);
    return rc;
  }

  // DINGOFS_ERROR::OK
  return rc;
}

DINGOFS_ERROR RenameOperator::UnLinkInode(uint64_t inode_id, uint64_t parent) {
  std::shared_ptr<InodeWrapper> inode_wrapper;
  auto rc = inodeManager_->GetInode(inode_id, inode_wrapper);
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("GetInode", rc);
    return rc;
  }

  rc = inode_wrapper->UnLink(parent);
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("UnLink", rc);
    return rc;
  }

  // DINGOFS_ERROR::OK
  return rc;
}

DINGOFS_ERROR RenameOperator::UpdateMCTime(uint64_t inode_id) {
  std::shared_ptr<InodeWrapper> inode_wrapper;
  auto rc = inodeManager_->GetInode(inode_id, inode_wrapper);
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("GetInode", rc);
    return rc;
  }

  dingofs::utils::UniqueLock lk = inode_wrapper->GetUniqueLock();
  inode_wrapper->UpdateTimestampLocked(kModifyTime | kChangeTime);

  rc = inode_wrapper->SyncAttr();
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("SyncAttr", rc);
    return rc;
  }
  // DINGOFS_ERROR::OK
  return rc;
}

DINGOFS_ERROR RenameOperator::LinkDestParentInode() {
  // Link action is unnecessary when met one of the following 2 conditions:
  //   (1) source and destination under same directory
  //   (2) destination already exist
  //   (3) destination is not a directory
  if (!srcDentry_.has_type()) {
    LOG(ERROR) << "srcDentry_ not have type!"
               << "Dentry: " << srcDentry_.ShortDebugString();
    return DINGOFS_ERROR::INTERNAL;
  }
  if (FsFileType::TYPE_DIRECTORY != srcDentry_.type() ||
      parentId_ == newParentId_ || oldInodeId_ != 0) {
    UpdateMCTime(newParentId_);
    return DINGOFS_ERROR::OK;
  }
  return LinkInode(newParentId_);
}

DINGOFS_ERROR RenameOperator::UnlinkSrcParentInode() {
  // UnLink action is unnecessary when met the following 2 conditions:
  //   (1) source and destination under same directory
  //   (2) destination not exist
  // or
  //    source is not a directory
  if (!srcDentry_.has_type()) {
    LOG(ERROR) << "srcDentry_ not have type!"
               << "Dentry: " << srcDentry_.ShortDebugString();
    return DINGOFS_ERROR::INTERNAL;
  }
  if (FsFileType::TYPE_DIRECTORY != srcDentry_.type() ||
      (parentId_ == newParentId_ && oldInodeId_ == 0)) {
    if (parentId_ != newParentId_) {
      UpdateMCTime(parentId_);
    }
    return DINGOFS_ERROR::OK;
  }
  auto rc = UnLinkInode(parentId_);
  LOG(INFO) << "Unlink source parent inode, retCode = " << rc;
  return rc;
}

void RenameOperator::UnlinkOldInode() {
  if (oldInodeId_ == 0) {
    return;
  }

  auto rc = UnLinkInode(oldInodeId_, newParentId_);
  LOG(INFO) << "Unlink old inode, retCode = " << rc;
}

DINGOFS_ERROR RenameOperator::UpdateInodeParent() {
  std::shared_ptr<InodeWrapper> inode_wrapper;
  auto rc = inodeManager_->GetInode(srcDentry_.inodeid(), inode_wrapper);
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("GetInode", rc);
    return rc;
  }

  rc = inode_wrapper->UpdateParent(parentId_, newParentId_);
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("UpdateInodeParent", rc);
    return rc;
  }

  LOG(INFO) << "UpdateInodeParent oldParent = " << parentId_
            << ", newParent = " << newParentId_;
  return rc;
}

DINGOFS_ERROR RenameOperator::UpdateInodeCtime() {
  std::shared_ptr<InodeWrapper> inode_wrapper;
  auto rc = inodeManager_->GetInode(srcDentry_.inodeid(), inode_wrapper);
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("GetInode", rc);
    return rc;
  }

  dingofs::utils::UniqueLock lk = inode_wrapper->GetUniqueLock();
  inode_wrapper->UpdateTimestampLocked(kChangeTime);

  rc = inode_wrapper->SyncAttr();
  if (rc != DINGOFS_ERROR::OK) {
    LOG_ERROR("UpdateInodeCtime", rc);
    return rc;
  }

  LOG(INFO) << "UpdateInodeCtime inodeid = " << srcDentry_.inodeid();
  return rc;
}

void RenameOperator::UpdateCache() {
  SetTxId(srcPartitionId_, srcTxId_ + 1);
  SetTxId(dstPartitionId_, dstTxId_ + 1);
}

void RenameOperator::GetOldInode(uint64_t* old_inode_id,
                                 int64_t* old_inode_size,
                                 FsFileType* old_inode_type) {
  *old_inode_id = oldInodeId_;
  *old_inode_size = oldInodeSize_;
  *old_inode_type = oldInodeType_;
}

// TODO: refact dir space when support dir rename
void RenameOperator::CalSrcUsage(int64_t& space, int64_t& inode) {
  if (src_inode_attr_.type() == FsFileType::TYPE_DIRECTORY) {
    space = 0;
    inode = 1;
  } else {
    space = src_inode_attr_.length();
    inode = 1;
  }
}

void RenameOperator::UpdateSrcDirUsage(
    std::shared_ptr<filesystem::FileSystem>& fs) {
  if (parentId_ != newParentId_) {
    int64_t update_space = 0;
    int64_t update_inode = 0;
    CalSrcUsage(update_space, update_inode);
    fs->UpdateDirQuotaUsage(parentId_, -update_space, -update_inode);
  }
}

void RenameOperator::RollbackUpdateSrcDirUsage(
    std::shared_ptr<filesystem::FileSystem>& fs) {
  if (parentId_ != newParentId_) {
    int64_t update_space = 0;
    int64_t update_inode = 0;
    CalSrcUsage(update_space, update_inode);
    fs->UpdateDirQuotaUsage(parentId_, update_space, update_inode);
  }
}

bool RenameOperator::CheckNewParentQuota(
    std::shared_ptr<filesystem::FileSystem>& fs) {
  if (parentId_ != newParentId_) {
    int64_t space = 0;
    int64_t inode = 0;
    CalSrcUsage(space, inode);
    return fs->CheckDirQuota(newParentId_, space, inode);
  } else {
    return true;
  }
}

void RenameOperator::FinishUpdateUsage(std::shared_ptr<FileSystem>& fs) {
  UpdateDstDirUsage(fs);
  UPdateFsStat(fs);
}

void RenameOperator::UpdateDstDirUsage(std::shared_ptr<FileSystem>& fs) {
  int64_t update_space = 0;
  int64_t update_inode = 0;

  if (parentId_ != newParentId_) {
    CalSrcUsage(update_space, update_inode);
  }

  int64_t reduce_space = 0;
  int64_t reduce_inode = 0;
  GetReduceStat(reduce_space, reduce_inode);

  update_space -= reduce_space;
  update_inode -= reduce_inode;

  fs->UpdateDirQuotaUsage(newParentId_, update_space, update_inode);
}

void RenameOperator::UPdateFsStat(std::shared_ptr<FileSystem>& fs) {
  int64_t reduce_space = 0;
  int64_t reduce_inode = 0;
  GetReduceStat(reduce_space, reduce_inode);

  fs->UpdateFsQuotaUsage(-reduce_space, -reduce_inode);
}

void RenameOperator::GetReduceStat(int64_t& reduce_space,
                                   int64_t& reduce_inode) {
  if (oldInodeId_ == 0) {
    // not exist same name
    reduce_space = 0;
    reduce_inode = 0;
  } else {
    // exist same name
    if (oldInodeType_ == FsFileType::TYPE_DIRECTORY) {
      reduce_space = 0;
      reduce_inode = 1;
    } else {
      reduce_space = oldInodeSize_;
      reduce_inode = 1;
    }
  }
}

}  // namespace client
}  // namespace dingofs
