/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * @Date: 2022-08-25 15:39:29
 * @Author: chenwei
 */
#ifndef DINGOFS_SRC_METASERVER_RECYCLE_CLEANER_H_
#define DINGOFS_SRC_METASERVER_RECYCLE_CLEANER_H_

#include <memory>
#include <string>

#include "metaserver/copyset/copyset_node.h"
#include "metaserver/partition.h"
#include "stub/rpcclient/metaserver_client.h"

namespace dingofs {
namespace metaserver {
class RecycleCleaner {
 public:
  explicit RecycleCleaner(const std::shared_ptr<Partition>& partition)
      : partition_(partition) {
    isStop_ = false;
    LOG(INFO) << "RecycleCleaner poolId = " << partition->GetPoolId()
              << ", fsId = " << partition->GetFsId()
              << ", partitionId = " << partition->GetPartitionId();
  }

  void SetCopysetNode(copyset::CopysetNode* copysetNode) {
    copysetNode_ = copysetNode;
  }

  void SetMdsClient(std::shared_ptr<stub::rpcclient::MdsClient> mdsClient) {
    mdsClient_ = mdsClient;
  }

  void SetMetaClient(
      std::shared_ptr<stub::rpcclient::MetaServerClient> metaClient) {
    metaClient_ = metaClient;
  }

  void SetScanLimit(uint32_t limit) { limit_ = limit; }

  // scan recycle dir and delete expired files
  bool ScanRecycle();
  // if dir is timeout, return true
  bool IsDirTimeOut(const std::string& dir);
  uint32_t GetRecycleTime() {
    return fsInfo_.has_recycletimehour() ? fsInfo_.recycletimehour() : 0;
  }
  bool GetEnableSumInDir() { return fsInfo_.enablesumindir(); }
  // delete dir and all files in dir recursively
  bool DeleteDirRecursive(const pb::metaserver::Dentry& dentry);
  // update fs info every time it's called to get lastest recycle time
  bool UpdateFsInfo();
  // delete one file or one dir directly
  bool DeleteNode(const pb::metaserver::Dentry& dentry);

  uint32_t GetPartitionId() { return partition_->GetPartitionId(); }

  uint32_t GetFsId() { return partition_->GetFsId(); }

  void Stop() { isStop_ = true; }

  bool IsStop() { return isStop_; }

  uint64_t GetTxId();

 private:
  std::shared_ptr<Partition> partition_;
  copyset::CopysetNode* copysetNode_;
  bool isStop_;
  std::shared_ptr<stub::rpcclient::MdsClient> mdsClient_;
  std::shared_ptr<stub::rpcclient::MetaServerClient> metaClient_;
  pb::mds::FsInfo fsInfo_;
  uint64_t limit_ = 1000;
};
}  // namespace metaserver
}  // namespace dingofs
#endif  // DINGOFS_SRC_METASERVER_RECYCLE_CLEANER_H_
