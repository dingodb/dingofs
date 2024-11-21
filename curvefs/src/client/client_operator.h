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

#ifndef CURVEFS_SRC_CLIENT_CLIENT_OPERATOR_H_
#define CURVEFS_SRC_CLIENT_CLIENT_OPERATOR_H_

#include <memory>
#include <string>
#include <vector>

#include "curvefs/src/client/dentry_cache_manager.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/rpcclient/mds_client.h"

namespace curvefs {
namespace client {

using rpcclient::MdsClient;

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
                 std::shared_ptr<MetaServerClient> meta_client,
                 std::shared_ptr<MdsClient> mds_client, bool enable_parallel);

  CURVEFS_ERROR GetTxId();
  CURVEFS_ERROR Precheck();
  CURVEFS_ERROR RecordSrcInodeInfo();
  CURVEFS_ERROR RecordOldInodeInfo();
  CURVEFS_ERROR LinkDestParentInode();
  CURVEFS_ERROR PrepareTx();
  CURVEFS_ERROR CommitTx();
  CURVEFS_ERROR UnlinkSrcParentInode();
  void UnlinkOldInode();
  CURVEFS_ERROR UpdateInodeParent();
  CURVEFS_ERROR UpdateInodeCtime();
  void UpdateCache();

  void GetOldInode(uint64_t* old_inode_id, int64_t* old_inode_size,
                   FsFileType* old_inode_type);

  // related to quota and stat
  void UpdateSrcDirUsage(std::shared_ptr<filesystem::FileSystem>& fs);
  void RollbackUpdateSrcDirUsage(std::shared_ptr<filesystem::FileSystem>& fs);
  bool CheckNewParentQuota(std::shared_ptr<filesystem::FileSystem>& fs);
  void FinishUpdateUsage(std::shared_ptr<filesystem::FileSystem>& fs);

  std::string DebugString();

 private:
  CURVEFS_ERROR CheckOverwrite();

  CURVEFS_ERROR GetLatestTxIdWithLock();

  CURVEFS_ERROR GetTxId(uint32_t fs_id, uint64_t inode_id,
                        uint32_t* partition_id, uint64_t* tx_id);

  void SetTxId(uint32_t partition_id, uint64_t tx_id);

  CURVEFS_ERROR PrepareRenameTx(const std::vector<Dentry>& dentrys);

  CURVEFS_ERROR LinkInode(uint64_t inode_id, uint64_t parent = 0);

  CURVEFS_ERROR UnLinkInode(uint64_t inode_id, uint64_t parent = 0);

  CURVEFS_ERROR UpdateMCTime(uint64_t inode_id);

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
  FsFileType oldInodeType_;
  Dentry srcDentry_;
  Dentry dstDentry_;
  Dentry dentry_;
  Dentry newDentry_;

  InodeAttr src_inode_attr_;

  std::shared_ptr<DentryCacheManager> dentryManager_;
  std::shared_ptr<InodeCacheManager> inodeManager_;
  std::shared_ptr<MetaServerClient> metaClient_;
  std::shared_ptr<MdsClient> mdsClient_;

  // whether support execute rename with parallel
  bool enableParallel_;
  std::string uuid_;
  uint64_t sequence_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_CLIENT_OPERATOR_H_
