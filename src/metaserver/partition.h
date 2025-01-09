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
 * @Project: dingo
 * @Date: 2021-08-30 19:48:38
 * @Author: chenwei
 */

#ifndef DINGOFS_SRC_METASERVER_PARTITION_H_
#define DINGOFS_SRC_METASERVER_PARTITION_H_
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "dingofs/common.pb.h"
#include "dingofs/metaserver.pb.h"
#include "common/define.h"
#include "metaserver/dentry_manager.h"
#include "metaserver/dentry_storage.h"
#include "metaserver/inode_manager.h"
#include "metaserver/inode_storage.h"
#include "metaserver/storage/iterator.h"

namespace dingofs {
namespace metaserver {

// skip ROOTINODEID and RECYCLEINODEID
constexpr uint64_t kMinPartitionStartId = ROOTINODEID + 2;

class Partition {
 public:
  Partition(pb::common::PartitionInfo partition,
            std::shared_ptr<storage::KVStorage> kv_storage,
            bool start_compact = true);

  // dentry
  pb::metaserver::MetaStatusCode CreateDentry(
      const pb::metaserver::Dentry& dentry);

  pb::metaserver::MetaStatusCode LoadDentry(
      const pb::metaserver::DentryVec& vec, bool merge);

  pb::metaserver::MetaStatusCode DeleteDentry(
      const pb::metaserver::Dentry& dentry);

  pb::metaserver::MetaStatusCode GetDentry(pb::metaserver::Dentry* dentry);

  pb::metaserver::MetaStatusCode ListDentry(
      const pb::metaserver::Dentry& dentry,
      std::vector<pb::metaserver::Dentry>* dentrys, uint32_t limit,
      bool only_dir = false);

  void ClearDentry();

  pb::metaserver::MetaStatusCode HandleRenameTx(
      const std::vector<pb::metaserver::Dentry>& dentrys);

  bool InsertPendingTx(
      const pb::metaserver::PrepareRenameTxRequest& pending_tx);

  bool FindPendingTx(pb::metaserver::PrepareRenameTxRequest* pending_tx);

  // inode
  pb::metaserver::MetaStatusCode CreateInode(const InodeParam& param,
                                             pb::metaserver::Inode* inode);

  pb::metaserver::MetaStatusCode CreateRootInode(const InodeParam& param);

  pb::metaserver::MetaStatusCode CreateManageInode(
      const InodeParam& param, pb::metaserver::ManageInodeType manage_type,
      pb::metaserver::Inode* inode);

  pb::metaserver::MetaStatusCode GetInode(uint32_t fs_id, uint64_t inode_id,
                                          pb::metaserver::Inode* inode);

  pb::metaserver::MetaStatusCode GetInodeWithChunkInfo(
      uint32_t fs_id, uint64_t inode_id, pb::metaserver::Inode* inode);

  pb::metaserver::MetaStatusCode GetInodeAttr(uint32_t fs_id, uint64_t inode_id,
                                              pb::metaserver::InodeAttr* attr);

  pb::metaserver::MetaStatusCode GetXAttr(uint32_t fs_id, uint64_t inode_id,
                                          pb::metaserver::XAttr* xattr);

  pb::metaserver::MetaStatusCode DeleteInode(uint32_t fs_id, uint64_t inode_id);

  pb::metaserver::MetaStatusCode UpdateInode(
      const pb::metaserver::UpdateInodeRequest& request);

  pb::metaserver::MetaStatusCode GetOrModifyS3ChunkInfo(
      uint32_t fs_id, uint64_t inode_id, const S3ChunkInfoMap& map2add,
      const S3ChunkInfoMap& map2del, bool return_s3_chunk_info_map,
      std::shared_ptr<storage::Iterator>* iterator);

  pb::metaserver::MetaStatusCode PaddingInodeS3ChunkInfo(int32_t fs_id,
                                                         uint64_t inode_id,
                                                         S3ChunkInfoMap* m,
                                                         uint64_t limit = 0);

  pb::metaserver::MetaStatusCode UpdateVolumeExtent(
      uint32_t fs_id, uint64_t inode_id,
      const pb::metaserver::VolumeExtentList& extents);

  pb::metaserver::MetaStatusCode UpdateVolumeExtentSlice(
      uint32_t fs_id, uint64_t inode_id,
      const pb::metaserver::VolumeExtentSlice& slice);

  pb::metaserver::MetaStatusCode GetVolumeExtent(
      uint32_t fs_id, uint64_t inode_id, const std::vector<uint64_t>& slices,
      pb::metaserver::VolumeExtentList* extents);

  pb::metaserver::MetaStatusCode InsertInode(
      const pb::metaserver::Inode& inode);

  bool GetInodeIdList(std::list<uint64_t>* inode_id_list);

  // if partition has no inode or no dentry, it is deletable
  bool IsDeletable();

  // check if fsid matchs and inode range belongs to this partition
  bool IsInodeBelongs(uint32_t fs_id, uint64_t inode_id);

  // check if fsid match this partition
  bool IsInodeBelongs(uint32_t fs_id);

  uint32_t GetPartitionId() const;

  uint32_t GetPoolId() { return partitionInfo_.poolid(); }

  uint32_t GetCopySetId() { return partitionInfo_.copysetid(); }

  uint32_t GetFsId() { return partitionInfo_.fsid(); }

  pb::common::PartitionInfo GetPartitionInfo();

  // get new inode id in partition range.
  // if no available inode id in this partiton ,return UINT64_MAX
  uint64_t GetNewInodeId();

  uint32_t GetInodeNum();

  uint32_t GetDentryNum();

  bool EmptyInodeStorage();

  void SetStatus(pb::common::PartitionStatus status) {
    partitionInfo_.set_status(status);
  }

  pb::common::PartitionStatus GetStatus() { return partitionInfo_.status(); }

  void StartS3Compact();

  void CancelS3Compact();

  std::string GetInodeTablename();

  std::string GetDentryTablename();

  std::shared_ptr<storage::Iterator> GetAllInode();

  std::shared_ptr<storage::Iterator> GetAllDentry();

  std::shared_ptr<storage::Iterator> GetAllS3ChunkInfoList();

  std::shared_ptr<storage::Iterator> GetAllVolumeExtentList();

  bool Clear();

  void SetManageFlag(bool flag) { partitionInfo_.set_manageflag(flag); }

  bool GetManageFlag() {
    if (partitionInfo_.has_manageflag()) {
      return partitionInfo_.manageflag();
    } else {
      return false;
    }
  }

 private:
  std::shared_ptr<InodeStorage> inodeStorage_;
  std::shared_ptr<DentryStorage> dentryStorage_;
  std::shared_ptr<InodeManager> inodeManager_;
  std::shared_ptr<TrashImpl> trash_;
  std::shared_ptr<DentryManager> dentryManager_;
  std::shared_ptr<TxManager> txManager_;

  pb::common::PartitionInfo partitionInfo_;
};
}  // namespace metaserver
}  // namespace dingofs
#endif  // DINGOFS_SRC_METASERVER_PARTITION_H_
