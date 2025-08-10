/*
 *  Copyright (c) 2020 NetEase Inc.
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

#ifndef DINGOFS_TEST_STUB_RPCCLIENT_MOCK_MDS_CLIENT_H_
#define DINGOFS_TEST_STUB_RPCCLIENT_MOCK_MDS_CLIENT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "stub/common/config.h"
#include "stub/rpcclient/mds_client.h"

using ::testing::_;
using ::testing::Return;

namespace dingofs {
namespace stub {
namespace rpcclient {

using common::CopysetID;
using common::CopysetInfo;
using common::CopysetPeerInfo;
using common::LogicPoolID;
using common::MetaserverID;
using common::PeerAddr;
using stub::common::MdsOption;
using stub::rpcclient::MDSBaseClient;

using pb::common::PartitionInfo;
using pb::mds::FsInfo;
using pb::mds::FSStatusCode;
using pb::mds::Mountpoint;
using pb::mds::cachegroup::CacheGroupErrCode;
using pb::mds::topology::Copyset;
using pb::mds::topology::PartitionTxId;

class MockMdsClient : public MdsClient {
 public:
  MockMdsClient() = default;

  ~MockMdsClient() override = default;

  MOCK_METHOD2(Init, FSStatusCode(const MdsOption& mdsOpt,
                                  MDSBaseClient* baseclient));

  MOCK_METHOD3(MountFs,
               FSStatusCode(const std::string& fsName,
                            const Mountpoint& mountPt, FsInfo* fsInfo));

  MOCK_METHOD2(UmountFs, FSStatusCode(const std::string& fsName,
                                      const Mountpoint& mountPt));

  MOCK_METHOD2(GetFsInfo,
               FSStatusCode(const std::string& fsName, FsInfo* fsInfo));

  MOCK_METHOD2(GetFsInfo, FSStatusCode(uint32_t fsId, FsInfo* fsInfo));

  MOCK_METHOD3(AllocS3ChunkId,
               FSStatusCode(uint32_t fsId, uint32_t idNum, uint64_t* chunkId));

  MOCK_METHOD2(GetLatestTxId,
               FSStatusCode(uint32_t fsId, std::vector<PartitionTxId>* txIds));

  MOCK_METHOD5(GetLatestTxIdWithLock,
               FSStatusCode(uint32_t fsId, const std::string& fsname,
                            const std::string& uuid,
                            std::vector<PartitionTxId>* txIds,
                            uint64_t* sequence));

  MOCK_METHOD1(CommitTx, FSStatusCode(const std::vector<PartitionTxId>& txIds));

  MOCK_METHOD4(CommitTxWithLock,
               FSStatusCode(const std::vector<PartitionTxId>& txIds,
                            const std::string& fsname, const std::string& uuid,
                            uint64_t sequence));

  MOCK_METHOD2(GetMetaServerInfo,
               bool(const PeerAddr& addr,
                    CopysetPeerInfo<MetaserverID>* metaserverInfo));

  MOCK_METHOD3(GetMetaServerListInCopysets,
               bool(const LogicPoolID& logicalpooid,
                    const std::vector<CopysetID>& copysetidvec,
                    std::vector<CopysetInfo<MetaserverID>>* cpinfoVec));

  MOCK_METHOD3(CreatePartition,
               bool(uint32_t fsID, uint32_t count,
                    std::vector<PartitionInfo>* partitionInfos));

  MOCK_METHOD2(GetCopysetOfPartitions,
               bool(const std::vector<uint32_t>& partitionIDList,
                    std::map<uint32_t, Copyset>* copysetMap));

  MOCK_METHOD2(ListPartition,
               bool(uint32_t fsID, std::vector<PartitionInfo>* partitionInfos));

  MOCK_METHOD(pb::mds::FSStatusCode, RefreshSession,
              (const std::vector<pb::mds::topology::PartitionTxId>& txIds,
               std::vector<pb::mds::topology::PartitionTxId>* latestTxIdList,
               const std::string& fsName,
               const pb::mds::Mountpoint& mountpoint),
              (override));

  MOCK_METHOD2(AllocOrGetMemcacheCluster,
               bool(uint32_t, pb::mds::topology::MemcacheClusterInfo*));

  MOCK_METHOD2(SetFsStats,
               FSStatusCode(const std::string& fsName,
                            const pb::mds::FsStatsData& fsStatsData));

  MOCK_METHOD(CacheGroupErrCode, JoinCacheGroup,
              (const std::string& group_name, const std::string& ip,
               uint32_t port, uint32_t weight, std::string* member_id),
              (override));

  MOCK_METHOD(CacheGroupErrCode, LeaveCacheGroup,
              (const std::string& group_name, const std::string& ip,
               uint32_t port),
              (override));

  MOCK_METHOD(CacheGroupErrCode, SendCacheGroupHeartbeat,
              (const std::string& ip, uint32_t port), (override));

  MOCK_METHOD(CacheGroupErrCode, ListCacheGroupMembers,
              (const std::string& group_name,
               std::vector<pb::mds::cachegroup::CacheGroupMember>* members),
              (override));
};
}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_TEST_STUB_RPCCLIENT_MOCK_MDS_CLIENT_H_
