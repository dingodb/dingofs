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
 * Created Date: Thur Sept 2 2021
 * Author: lixiaocui
 */

#ifndef DINGOFS_SRC_CLIENT_RPCCLIENT_METACACHE_H_
#define DINGOFS_SRC_CLIENT_RPCCLIENT_METACACHE_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dingofs/common.pb.h"
#include "stub/common/common.h"
#include "stub/common/config.h"
#include "stub/common/metacache_struct.h"
#include "stub/rpcclient/cli2_client.h"
#include "stub/rpcclient/mds_client.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace stub {
namespace rpcclient {
// using Mutex = ::bthread::Mutex;

struct CopysetGroupID {
  common::LogicPoolID poolID = 0;
  common::CopysetID copysetID = 0;

  CopysetGroupID() = default;
  CopysetGroupID(const common::LogicPoolID& poolid,
                 const common::CopysetID& copysetid)
      : poolID(poolid), copysetID(copysetid) {}

  std::string ToString() const {
    return "{" + std::to_string(poolID) + "," + std::to_string(copysetID) + "}";
  }
};

struct CopysetTarget {
  // copyset id
  CopysetGroupID groupID;
  // partition id
  common::PartitionID partitionID = 0;
  uint64_t txId = 0;

  // leader info
  common::MetaserverID metaServerID = 0;
  butil::EndPoint endPoint;

  bool IsValid() const {
    return groupID.poolID != 0 && groupID.copysetID != 0 && partitionID != 0 &&
           txId != 0 && metaServerID != 0 && endPoint.ip != butil::IP_ANY &&
           endPoint.port != 0;
  }

  void Reset() {
    groupID = CopysetGroupID{};
    partitionID = 0;
    txId = 0;
    metaServerID = 0;
    endPoint = butil::EndPoint{};
  }
};

inline std::ostream& operator<<(std::ostream& os, const CopysetGroupID& g) {
  os << "[poolid:" << g.poolID << ", copysetid:" << g.copysetID << "]";
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const CopysetTarget& t) {
  os << "groupid:" << t.groupID << ", partitionid:" << t.partitionID
     << ", txid:" << t.txId << ", serverid:" << t.metaServerID
     << ", endpoint:" << butil::endpoint2str(t.endPoint).c_str();
  return os;
}

class MetaCache {
 public:
  void Init(common::MetaCacheOpt opt, std::shared_ptr<Cli2Client> cli2Client,
            std::shared_ptr<MdsClient> mdsClient) {
    metacacheopt_ = std::move(opt);
    cli2Client_ = std::move(cli2Client);
    mdsClient_ = std::move(mdsClient);
    init_ = false;
  }

  using PoolIDCopysetID = uint64_t;
  using PartitionInfoList = std::vector<pb::common::PartitionInfo>;
  using CopysetInfoMap =
      std::unordered_map<PoolIDCopysetID,
                         common::CopysetInfo<common::MetaserverID>>;

  virtual void SetTxId(uint32_t partitionId, uint64_t txId);

  virtual bool GetTxId(uint32_t fsId, uint64_t inodeId, uint32_t* partitionId,
                       uint64_t* txId);

  virtual void GetAllTxIds(
      std::vector<pb::mds::topology::PartitionTxId>* txIds);

  virtual bool GetTarget(uint32_t fsID, uint64_t inodeID, CopysetTarget* target,
                         uint64_t* applyIndex, bool refresh = false);

  virtual bool SelectTarget(uint32_t fsID, CopysetTarget* target,
                            uint64_t* applyIndex);

  virtual void UpdateApplyIndex(const CopysetGroupID& groupID,
                                uint64_t applyIndex);

  virtual uint64_t GetApplyIndex(const CopysetGroupID& groupID);

  virtual bool IsLeaderMayChange(const CopysetGroupID& groupID);

  virtual bool MarkPartitionUnavailable(common::PartitionID pid);

  virtual void UpdateCopysetInfo(
      const CopysetGroupID& groupID,
      const common::CopysetInfo<common::MetaserverID>& csinfo);

  virtual bool GetTargetLeader(CopysetTarget* target, uint64_t* applyindex,
                               bool refresh = false);

  virtual bool GetPartitionIdByInodeId(uint32_t fsID, uint64_t inodeID,
                                       common::PartitionID* pid);

  bool RefreshTxId();

  // list or create partitions for fs
  bool ListPartitions(uint32_t fsID);

 private:
  void GetTxId(uint32_t partitionId, uint64_t* txId);
  bool CreatePartitions(int currentNum, PartitionInfoList* newPartitions);
  bool DoListOrCreatePartitions(
      bool list, PartitionInfoList* partitionInfos,
      std::map<PoolIDCopysetID, common::CopysetInfo<common::MetaserverID>>*
          copysetMap);
  void DoAddOrResetPartitionAndCopyset(
      PartitionInfoList partitionInfos,
      std::map<PoolIDCopysetID, common::CopysetInfo<common::MetaserverID>>
          copysetMap,
      bool reset);

  // retry policy
  // TODO(@lixiaocui): rpc service may be split to ServiceHelper
  bool UpdateCopysetInfoFromMDS(
      const CopysetGroupID& groupID,
      common::CopysetInfo<common::MetaserverID>* targetInfo);
  bool UpdateLeaderInternal(
      const CopysetGroupID& groupID,
      common::CopysetInfo<common::MetaserverID>* toupdateCopyset);
  void UpdateCopysetInfoIfMatchCurrentLeader(
      const CopysetGroupID& groupID, const common::PeerAddr& leaderAddr);

  // select a dest parition for inode create
  // TODO(@lixiaocui): select parititon may be need SelectPolicy to support
  // more policies
  bool SelectPartition(CopysetTarget* target);

  // get info from partitionMap or copysetMap
  bool GetCopysetIDwithInodeID(uint64_t inodeID, CopysetGroupID* groupID,
                               common::PartitionID* patitionID, uint64_t* txId);

  bool GetCopysetInfowithCopySetID(
      const CopysetGroupID& groupID,
      common::CopysetInfo<common::MetaserverID>* targetInfo);

  // key tansform
  static PoolIDCopysetID CalcLogicPoolCopysetID(const CopysetGroupID& groupID) {
    return (static_cast<uint64_t>(groupID.poolID) << 32) |
           static_cast<uint64_t>(groupID.copysetID);
  }

  utils::RWLock txIdLock_;
  std::unordered_map<uint32_t, uint64_t> partitionTxId_;

  utils::RWLock rwlock4Partitions_;
  PartitionInfoList partitionInfos_;
  utils::RWLock rwlock4copysetInfoMap_;
  CopysetInfoMap copysetInfoMap_;

  ::bthread::Mutex createMutex_;

  common::MetaCacheOpt metacacheopt_;
  std::shared_ptr<Cli2Client> cli2Client_;
  std::shared_ptr<MdsClient> mdsClient_;

  uint32_t fsID_;
  std::atomic_bool init_;
};

}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_RPCCLIENT_METACACHE_H_
