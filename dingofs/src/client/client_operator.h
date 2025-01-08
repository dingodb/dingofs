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

#ifndef DINGOFS_SRC_CLIENT_CLIENT_OPERATOR_H_
#define DINGOFS_SRC_CLIENT_CLIENT_OPERATOR_H_

#include <memory>
#include <string>
#include <vector>

#include "client/dentry_cache_manager.h"
#include "client/inode_cache_manager.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace client {

// resolve cyclic dependency
namespace filesystem {
class FileSystem;
}

class RenameOperator {
 public:
  RenameOperator(uint32_t fs_id, const std::string& fs_name, uint64_t parent_id,
                 std::string name, uint64_t new_parent_id, std::string newname,
                 std::shared_ptr<DentryCacheManager> dentry_manager,
                 std::shared_ptr<InodeCacheManager> inode_manager,
                 std::shared_ptr<stub::rpcclient::MetaServerClient> meta_client,
                 std::shared_ptr<stub::rpcclient::MdsClient> mds_client,
                 bool enable_parallel);

  DINGOFS_ERROR GetTxId();
  DINGOFS_ERROR Precheck();
  DINGOFS_ERROR RecordSrcInodeInfo();
  DINGOFS_ERROR RecordOldInodeInfo();
  DINGOFS_ERROR LinkDestParentInode();
  DINGOFS_ERROR PrepareTx();
  DINGOFS_ERROR CommitTx();
  DINGOFS_ERROR UnlinkSrcParentInode();
  void UnlinkOldInode();
  DINGOFS_ERROR UpdateInodeParent();
  DINGOFS_ERROR UpdateInodeCtime();
  void UpdateCache();

  void GetOldInode(uint64_t* old_inode_id, int64_t* old_inode_size,
                   pb::metaserver::FsFileType* old_inode_type);

  // related to quota and stat
  void UpdateSrcDirUsage(std::shared_ptr<filesystem::FileSystem>& fs);
  void RollbackUpdateSrcDirUsage(std::shared_ptr<filesystem::FileSystem>& fs);
  bool CheckNewParentQuota(std::shared_ptr<filesystem::FileSystem>& fs);
  void FinishUpdateUsage(std::shared_ptr<filesystem::FileSystem>& fs);

  std::string DebugString();

 private:
  DINGOFS_ERROR CheckOverwrite();

  DINGOFS_ERROR GetLatestTxIdWithLock();

  DINGOFS_ERROR GetTxId(uint32_t fs_id, uint64_t inode_id,
                        uint32_t* partition_id, uint64_t* tx_id);

  void SetTxId(uint32_t partition_id, uint64_t tx_id);

  DINGOFS_ERROR PrepareRenameTx(
      const std::vector<pb::metaserver::Dentry>& dentrys);

  DINGOFS_ERROR LinkInode(uint64_t inode_id, uint64_t parent = 0);

  DINGOFS_ERROR UnLinkInode(uint64_t inode_id, uint64_t parent = 0);

  DINGOFS_ERROR UpdateMCTime(uint64_t inode_id);

  // related to quota and stat
  void CalSrcUsage(int64_t& space, int64_t& inode);
  void UpdateDstDirUsage(std::shared_ptr<filesystem::FileSystem>& fs);
  void UPdateFsStat(std::shared_ptr<filesystem::FileSystem>& fs);
  void GetReduceStat(int64_t& reduce_space, int64_t& reduce_inode);

  uint32_t fsId_;
  std::string fsName_;
  uint64_t parentId_;
  std::string name_;
  uint64_t newParentId_;
  std::string newname_;

  uint32_t srcPartitionId_;
  uint32_t dstPartitionId_;
  uint64_t srcTxId_;
  uint64_t dstTxId_;
  uint64_t oldInodeId_;
  // if dest exist, record the size and type of file or empty dir
  int64_t oldInodeSize_{0};
  pb::metaserver::FsFileType oldInodeType_;
  pb::metaserver::Dentry srcDentry_;
  pb::metaserver::Dentry dstDentry_;
  pb::metaserver::Dentry dentry_;
  pb::metaserver::Dentry newDentry_;

  pb::metaserver::InodeAttr src_inode_attr_;

  std::shared_ptr<DentryCacheManager> dentryManager_;
  std::shared_ptr<InodeCacheManager> inodeManager_;
  std::shared_ptr<stub::rpcclient::MetaServerClient> metaClient_;
  std::shared_ptr<stub::rpcclient::MdsClient> mdsClient_;

  // whether support execute rename with parallel
  bool enableParallel_;
  std::string uuid_;
  uint64_t sequence_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_CLIENT_OPERATOR_H_
