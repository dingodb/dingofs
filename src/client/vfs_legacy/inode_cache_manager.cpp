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

#include "client/vfs_legacy/inode_cache_manager.h"

#include <glog/logging.h>

#include <cstdint>
#include <map>
#include <memory>
#include <utility>

#include "client/vfs_legacy/filesystem/error.h"
#include "client/vfs_legacy/inode_wrapper.h"
#include "dingofs/metaserver.pb.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {

using filesystem::ToFSError;
using utils::CountDownEvent;
using utils::Mutex;

using stub::rpcclient::InodeParam;

using pb::metaserver::FsFileType;
using pb::metaserver::Inode;
using pb::metaserver::InodeAttr;
using pb::metaserver::MetaStatusCode;
using pb::metaserver::MetaStatusCode_Name;
using pb::metaserver::XAttr;

using NameLockGuard = ::dingofs::utils::GenericNameLockGuard<Mutex>;

DINGOFS_ERROR InodeCacheManagerImpl::GetInodeFromCached(
    uint64_t inode_id, std::shared_ptr<InodeWrapper>& out) {
  NameLockGuard lock(nameLock_, std::to_string(inode_id));
  return GetInodeFromCachedUnlocked(inode_id, out);
}

DINGOFS_ERROR InodeCacheManagerImpl::GetInodeFromCachedUnlocked(
    uint64_t inode_id, std::shared_ptr<InodeWrapper>& out) {
  bool yes = openFiles_->IsOpened(inode_id, &out);
  if (yes) {
    VLOG(6) << "GetInode from openFiles, inodeId=" << inode_id;
    return DINGOFS_ERROR::OK;
  }

  yes = deferSync_->Get(inode_id, out);
  if (yes) {
    VLOG(6) << "GetInode from deferSync, inodeId=" << inode_id;
    return DINGOFS_ERROR::OK;
  }

  return DINGOFS_ERROR::NOTEXIST;
}

DINGOFS_ERROR InodeCacheManagerImpl::GetInode(
    uint64_t inode_id, std::shared_ptr<InodeWrapper>& out) {
  NameLockGuard lock(nameLock_, std::to_string(inode_id));

  DINGOFS_ERROR rc = GetInodeFromCachedUnlocked(inode_id, out);
  if (rc == DINGOFS_ERROR::OK) {
    return rc;
  }

  // get inode from metaserver
  Inode inode;
  bool streaming = false;

  MetaStatusCode ret =
      metaClient_->GetInode(m_fs_id, inode_id, &inode, &streaming);
  if (ret != MetaStatusCode::OK) {
    LOG_IF(ERROR, ret != MetaStatusCode::NOT_FOUND)
        << "metaClient_ GetInode failed, MetaStatusCode = " << ret
        << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
        << ", inodeId=" << inode_id;
    return ToFSError(ret);
  }

  VLOG(3) << "GetInode from metaserver, inodeId=" << inode_id;

  out = std::make_shared<InodeWrapper>(std::move(inode), metaClient_,
                                       s3ChunkInfoMetric_, option_.maxDataSize,
                                       option_.refreshDataIntervalSec);

  // refresh data
  rc = RefreshData(out, streaming);
  if (rc != DINGOFS_ERROR::OK) {
    return rc;
  }

  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR InodeCacheManagerImpl::GetInodeAttr(uint64_t inode_id,
                                                  InodeAttr* out) {
  NameLockGuard lock(nameLock_, std::to_string(inode_id));

  std::shared_ptr<InodeWrapper> inode_wrapper;
  DINGOFS_ERROR rc = GetInodeFromCachedUnlocked(inode_id, inode_wrapper);
  if (rc == DINGOFS_ERROR::OK) {
    inode_wrapper->GetInodeAttr(out);
    return rc;
  }

  std::set<uint64_t> inode_ids;
  std::list<InodeAttr> attrs;
  inode_ids.emplace(inode_id);
  MetaStatusCode ret =
      metaClient_->BatchGetInodeAttr(m_fs_id, inode_ids, &attrs);
  if (MetaStatusCode::OK != ret) {
    LOG(ERROR) << "metaClient BatchGetInodeAttr failed"
               << ", inodeId=" << inode_id << ", MetaStatusCode = " << ret
               << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret);
    return ToFSError(ret);
  }

  if (attrs.size() != 1) {
    LOG(ERROR) << "metaClient BatchGetInodeAttr error,"
               << " getSize is 1, inodeId=" << inode_id
               << "but real size = " << attrs.size();
    return DINGOFS_ERROR::INTERNAL;
  }

  *out = *attrs.begin();
  return DINGOFS_ERROR::OK;
}

// TODO: remove this function when enable sum dir is refacted
DINGOFS_ERROR InodeCacheManagerImpl::BatchGetInodeAttr(
    std::set<uint64_t>* inode_ids, std::list<InodeAttr>* attrs) {
  if (inode_ids->empty()) {
    VLOG(1) << "BatchGetInodeAttr: inode_ids is empty";
    return DINGOFS_ERROR::OK;
  }

  MetaStatusCode ret =
      metaClient_->BatchGetInodeAttr(m_fs_id, *inode_ids, attrs);
  if (MetaStatusCode::OK != ret) {
    LOG(ERROR) << "metaClient BatchGetInodeAttr failed, MetaStatusCode = "
               << ret << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret);
  }

  return ToFSError(ret);
}

// TODO: no need find inode by cache, this is call by read dir
DINGOFS_ERROR InodeCacheManagerImpl::BatchGetInodeAttrAsync(
    uint64_t parent_id, std::set<uint64_t>* inode_ids,
    std::map<uint64_t, InodeAttr>* attrs) {
  NameLockGuard lg(asyncNameLock_, std::to_string(parent_id));

  if (inode_ids->empty()) {
    return DINGOFS_ERROR::OK;
  }

  // split inodeIds by partitionId and batch limit
  std::vector<std::vector<uint64_t>> inode_groups;
  if (!metaClient_->SplitRequestInodes(m_fs_id, *inode_ids, &inode_groups)) {
    return DINGOFS_ERROR::NOTEXIST;
  }

  ::dingofs::utils::Mutex mutex;
  std::shared_ptr<CountDownEvent> cond =
      std::make_shared<CountDownEvent>(inode_groups.size());
  for (const auto& it : inode_groups) {
    VLOG(3) << "BatchGetInodeAttrAsync Send size: " << it.size()
            << ", parent inodeId=" << parent_id;
    auto* done = new BatchGetInodeAttrAsyncDone(attrs, &mutex, cond);
    MetaStatusCode ret = metaClient_->BatchGetInodeAttrAsync(m_fs_id, it, done);
    if (MetaStatusCode::OK != ret) {
      LOG(ERROR) << "Failed BatchGetInodeAsync" << " MetaStatusCode = " << ret
                 << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                 << " size:" << it.size() << ", parent inodeId=" << parent_id;
    }
  }

  // wait for all sudrequest finished
  cond->Wait();
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR InodeCacheManagerImpl::BatchGetXAttr(
    std::set<uint64_t>* inode_ids, std::list<XAttr>* xattr) {
  if (inode_ids->empty()) {
    return DINGOFS_ERROR::OK;
  }

  MetaStatusCode ret = metaClient_->BatchGetXAttr(m_fs_id, *inode_ids, xattr);
  if (MetaStatusCode::OK != ret) {
    LOG(ERROR) << "metaClient BatchGetXAttr failed, MetaStatusCode = " << ret
               << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret);
  }
  return ToFSError(ret);
}

DINGOFS_ERROR InodeCacheManagerImpl::CreateInode(
    const InodeParam& param, std::shared_ptr<InodeWrapper>& out) {
  Inode inode;
  MetaStatusCode ret = metaClient_->CreateInode(param, &inode);
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "metaClient_ CreateInode failed, MetaStatusCode = " << ret
               << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret);
    return ToFSError(ret);
  }
  out = std::make_shared<InodeWrapper>(std::move(inode), metaClient_,
                                       s3ChunkInfoMetric_, option_.maxDataSize,
                                       option_.refreshDataIntervalSec);
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR InodeCacheManagerImpl::CreateManageInode(
    const InodeParam& param, std::shared_ptr<InodeWrapper>& out) {
  Inode inode;
  MetaStatusCode ret = metaClient_->CreateManageInode(param, &inode);
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "metaClient_ CreateManageInode failed, MetaStatusCode = "
               << ret << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret);
    return ToFSError(ret);
  }
  out = std::make_shared<InodeWrapper>(std::move(inode), metaClient_,
                                       s3ChunkInfoMetric_, option_.maxDataSize,
                                       option_.refreshDataIntervalSec);
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR InodeCacheManagerImpl::DeleteInode(uint64_t inode_id) {
  NameLockGuard lock(nameLock_, std::to_string(inode_id));
  MetaStatusCode ret = metaClient_->DeleteInode(m_fs_id, inode_id);
  if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
    LOG(ERROR) << "metaClient_ DeleteInode failed, MetaStatusCode = " << ret
               << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
               << ", inodeId=" << inode_id;
    return ToFSError(ret);
  }
  return DINGOFS_ERROR::OK;
}

void InodeCacheManagerImpl::ShipToFlush(
    const std::shared_ptr<InodeWrapper>& inode) {
  deferSync_->Push(inode);
}

DINGOFS_ERROR InodeCacheManagerImpl::RefreshData(
    std::shared_ptr<InodeWrapper>& inode, bool streaming) {
  auto type = inode->GetType();
  DINGOFS_ERROR rc = DINGOFS_ERROR::OK;

  switch (type) {
    case FsFileType::TYPE_S3:
      if (streaming) {
        // NOTE: if the s3chunkinfo inside inode is too large,
        // we should invoke RefreshS3ChunkInfo() to receive s3chunkinfo
        // by streaming and padding its into inode.
        rc = inode->RefreshS3ChunkInfo();
        LOG_IF(ERROR, rc != DINGOFS_ERROR::OK)
            << "RefreshS3ChunkInfo() failed, retCode = " << rc;
      }
      break;

    case FsFileType::TYPE_FILE: {
      CHECK(false) << "not supprt, TYPE_FILE should not be here";
      break;
    }

    default:
      rc = DINGOFS_ERROR::OK;
  }

  return rc;
}

}  // namespace client
}  // namespace dingofs
