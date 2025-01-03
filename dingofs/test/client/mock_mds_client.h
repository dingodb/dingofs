// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CURVEFS_TEST_CLIENT_MOCK_MDS_CLIENT_H_
#define CURVEFS_TEST_CLIENT_MOCK_MDS_CLIENT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace stub {
namespace rpcclient {

class MockMdsClient : public MdsClient {
 public:
  MOCK_METHOD(pb::mds::FSStatusCode, Init,
              (const common::MdsOption& mdsOpt, MDSBaseClient* baseclient),
              (override));

  MOCK_METHOD(pb::mds::FSStatusCode, MountFs,
              (const std::string& fsName, const pb::mds::Mountpoint& mountPt,
               pb::mds::FsInfo* fsInfo),
              (override));

  MOCK_METHOD(pb::mds::FSStatusCode, UmountFs,
              (const std::string& fsName, const pb::mds::Mountpoint& mountPt),
              (override));

  MOCK_METHOD(pb::mds::FSStatusCode, GetFsInfo,
              (const std::string& fsName, pb::mds::FsInfo* fsInfo), (override));

  MOCK_METHOD(pb::mds::FSStatusCode, GetFsInfo,
              (uint32_t fsId, pb::mds::FsInfo* fsInfo), (override));

  MOCK_METHOD(bool, GetMetaServerInfo,
              (const common::PeerAddr& addr,
               common::CopysetPeerInfo<common::MetaserverID>* metaserverInfo),
              (override));

  MOCK_METHOD(
      bool, GetMetaServerListInCopysets,
      (const common::LogicPoolID& logicalpooid,
       const std::vector<common::CopysetID>& copysetidvec,
       std::vector<common::CopysetInfo<common::MetaserverID>>* cpinfoVec),
      (override));

  MOCK_METHOD(bool, CreatePartition,
              (uint32_t fsID, uint32_t count,
               std::vector<pb::common::PartitionInfo>* partitionInfos),
              (override));

  MOCK_METHOD2(
      GetCopysetOfPartitions,
      bool(const std::vector<uint32_t>& partitionIDList,
           std::map<uint32_t, pb::mds::topology::Copyset>* copysetMap));

  MOCK_METHOD(bool, ListPartition,
              (uint32_t fsID,
               std::vector<pb::common::PartitionInfo>* partitionInfos),
              (override));

  MOCK_METHOD(bool, AllocOrGetMemcacheCluster,
              (uint32_t fsId, pb::mds::topology::MemcacheClusterInfo* cluster),
              (override));

  MOCK_METHOD(pb::mds::FSStatusCode, AllocS3ChunkId,
              (uint32_t fsId, uint32_t idNum, uint64_t* chunkId), (override));

  MOCK_METHOD(pb::mds::FSStatusCode, RefreshSession,
              (const std::vector<pb::mds::topology::PartitionTxId>& txIds,
               std::vector<pb::mds::topology::PartitionTxId>* latestTxIdList,
               const std::string& fsName, const pb::mds::Mountpoint& mountpoint,
               std::atomic<bool>* enableSumInDir),
              (override));

  MOCK_METHOD(pb::mds::FSStatusCode, GetLatestTxId,
              (uint32_t fsId,
               std::vector<pb::mds::topology::PartitionTxId>* txIds),
              (override));

  MOCK_METHOD(pb::mds::FSStatusCode, GetLatestTxIdWithLock,
              (uint32_t fsId, const std::string& fsName,
               const std::string& uuid,
               std::vector<pb::mds::topology::PartitionTxId>* txIds,
               uint64_t* sequence),
              (override));

  MOCK_METHOD(pb::mds::FSStatusCode, CommitTx,
              (const std::vector<pb::mds::topology::PartitionTxId>& txIds),
              (override));

  MOCK_METHOD(pb::mds::FSStatusCode, CommitTxWithLock,
              (const std::vector<pb::mds::topology::PartitionTxId>& txIds,
               const std::string& fsName, const std::string& uuid,
               uint64_t sequence),
              (override));

  // allocate block group
  MOCK_METHOD(SpaceErrCode, AllocateVolumeBlockGroup,
              (uint32_t fsId, uint32_t size, const std::string& owner,
               std::vector<pb::mds::space::BlockGroup>* groups),
              (override));

  // acquire block group
  MOCK_METHOD(SpaceErrCode, AcquireVolumeBlockGroup,
              (uint32_t fsId, uint64_t blockGroupOffset,
               const std::string& owner, pb::mds::space::BlockGroup* groups),
              (override));

  // release block group
  MOCK_METHOD(SpaceErrCode, ReleaseVolumeBlockGroup,
              (uint32_t fsId, const std::string& owner,
               const std::vector<pb::mds::space::BlockGroup>& blockGroups),
              (override));

  MOCK_METHOD(pb::mds::FSStatusCode, SetFsStats,
              (const std::string& fsname,
               const pb::mds::FsStatsData& fs_stat_data),
              (override));
};

}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // CURVEFS_TEST_CLIENT_MOCK_MDS_CLIENT_H_
