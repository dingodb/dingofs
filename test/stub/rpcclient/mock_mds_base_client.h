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
 * Created Date: Thur Jun 16 2021
 * Author: lixiaocui
 */

#ifndef DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_MDS_BASE_CLIENT_H_
#define DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_MDS_BASE_CLIENT_H_

#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "stub/rpcclient/base_client.h"

namespace dingofs {
namespace stub {
namespace rpcclient {
class MockMDSBaseClient : public MDSBaseClient {
 public:
  MockMDSBaseClient() : MDSBaseClient() {}
  ~MockMDSBaseClient() = default;

  MOCK_METHOD5(MountFs, void(const std::string& fsName,
                             const pb::mds::Mountpoint& mountPt,
                             pb::mds::MountFsResponse* response,
                             brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD5(UmountFs, void(const std::string& fsName,
                              const pb::mds::Mountpoint& mountPt,
                              pb::mds::UmountFsResponse* response,
                              brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD4(GetFsInfo, void(const std::string& fsName,
                               pb::mds::GetFsInfoResponse* response,
                               brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD4(GetFsInfo,
               void(uint32_t fsId, pb::mds::GetFsInfoResponse* response,
                    brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD4(GetLatestTxId,
               void(const pb::mds::GetLatestTxIdRequest& request,
                    pb::mds::GetLatestTxIdResponse* response,
                    brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD4(CommitTx, void(const pb::mds::CommitTxRequest& request,
                              pb::mds::CommitTxResponse* response,
                              brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD5(GetMetaServerInfo,
               void(uint32_t port, std::string ip,
                    pb::mds::topology::GetMetaServerInfoResponse* response,
                    brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD5(
      GetMetaServerListInCopysets,
      void(const common::LogicPoolID& logicalpooid,
           const std::vector<common::CopysetID>& copysetidvec,
           pb::mds::topology::GetMetaServerListInCopySetsResponse* response,
           brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD5(CreatePartition,
               void(uint32_t fsID, uint32_t count,
                    pb::mds::topology::CreatePartitionResponse* response,
                    brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD4(GetCopysetOfPartitions,
               void(const std::vector<uint32_t>& partitionIDList,
                    pb::mds::topology::GetCopysetOfPartitionResponse* response,
                    brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD4(ListPartition,
               void(uint32_t fsID,
                    pb::mds::topology::ListPartitionResponse* response,
                    brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD4(RefreshSession,
               void(const pb::mds::RefreshSessionRequest& request,
                    pb::mds::RefreshSessionResponse* response,
                    brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD6(AllocateVolumeBlockGroup,
               void(uint32_t fsId, uint32_t count, const std::string& owner,
                    pb::mds::space::AllocateBlockGroupResponse* response,
                    brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD6(AcquireVolumeBlockGroup,
               void(uint32_t fsId, uint64_t blockGroupOffset,
                    const std::string& owner,
                    pb::mds::space::AcquireBlockGroupResponse* response,
                    brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD6(ReleaseVolumeBlockGroup,
               void(uint32_t fsId, const std::string& owner,
                    const std::vector<pb::mds::space::BlockGroup>& blockGroups,
                    pb::mds::space::ReleaseBlockGroupResponse* response,
                    brpc::Controller* cntl, brpc::Channel* channel));

  MOCK_METHOD4(
      AllocOrGetMemcacheCluster,
      void(uint32_t fsId,
           pb::mds::topology::AllocOrGetMemcacheClusterResponse* response,
           brpc::Controller* cntl, brpc::Channel* channel));
};
}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_MDS_BASE_CLIENT_H_
