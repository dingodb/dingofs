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
 * Created Date: Fri Jun 11 2021
 * Author: lixiaocui
 */
#ifndef DINGOFS_SRC_CLIENT_RPCCLIENT_BASE_CLIENT_H_
#define DINGOFS_SRC_CLIENT_RPCCLIENT_BASE_CLIENT_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <string>
#include <vector>

#include "dingofs/mds.pb.h"
#include "dingofs/metaserver.pb.h"
#include "dingofs/space.pb.h"
#include "dingofs/topology.pb.h"
#include "stub/common/common.h"

namespace dingofs {
namespace stub {
namespace rpcclient {

struct InodeParam {
  uint64_t fsId;
  uint64_t length;
  uint32_t uid;
  uint32_t gid;
  uint32_t mode;
  pb::metaserver::FsFileType type;
  uint64_t rdev;
  std::string symlink;
  uint64_t parent;
  pb::metaserver::ManageInodeType manageType =
      pb::metaserver::ManageInodeType::TYPE_NOT_MANAGE;
};

inline std::ostream& operator<<(std::ostream& os, const InodeParam& p) {
  os << "fsid: " << p.fsId << ", length: " << p.length << ", uid: " << p.uid
     << ", gid: " << p.gid << ", mode: " << p.mode << ", type: " << p.type
     << ", rdev: " << p.rdev << ", symlink: " << p.symlink;

  return os;
}

class MDSBaseClient {
 public:
  virtual ~MDSBaseClient() = default;

  virtual void MountFs(const std::string& fsName,
                       const pb::mds::Mountpoint& mountPt,
                       pb::mds::MountFsResponse* response,
                       brpc::Controller* cntl, brpc::Channel* channel);

  virtual void UmountFs(const std::string& fsName,
                        const pb::mds::Mountpoint& mountPt,
                        pb::mds::UmountFsResponse* response,
                        brpc::Controller* cntl, brpc::Channel* channel);

  virtual void GetFsInfo(const std::string& fsName,
                         pb::mds::GetFsInfoResponse* response,
                         brpc::Controller* cntl, brpc::Channel* channel);

  virtual void GetFsInfo(uint32_t fsId, pb::mds::GetFsInfoResponse* response,
                         brpc::Controller* cntl, brpc::Channel* channel);

  virtual void GetMetaServerInfo(
      uint32_t port, std::string ip,
      pb::mds::topology::GetMetaServerInfoResponse* response,
      brpc::Controller* cntl, brpc::Channel* channel);
  virtual void GetMetaServerListInCopysets(
      const common::LogicPoolID& logicalpooid,
      const std::vector<common::CopysetID>& copysetidvec,
      pb::mds::topology::GetMetaServerListInCopySetsResponse* response,
      brpc::Controller* cntl, brpc::Channel* channel);

  virtual void CreatePartition(
      uint32_t fsID, uint32_t count,
      pb::mds::topology::CreatePartitionResponse* response,
      brpc::Controller* cntl, brpc::Channel* channel);

  virtual void GetCopysetOfPartitions(
      const std::vector<uint32_t>& partitionIDList,
      pb::mds::topology::GetCopysetOfPartitionResponse* response,
      brpc::Controller* cntl, brpc::Channel* channel);

  virtual void ListPartition(uint32_t fsID,
                             pb::mds::topology::ListPartitionResponse* response,
                             brpc::Controller* cntl, brpc::Channel* channel);

  virtual void AllocS3ChunkId(uint32_t fsId, uint32_t idNum,
                              pb::mds::AllocateS3ChunkResponse* response,
                              brpc::Controller* cntl, brpc::Channel* channel);

  virtual void RefreshSession(const pb::mds::RefreshSessionRequest& request,
                              pb::mds::RefreshSessionResponse* response,
                              brpc::Controller* cntl, brpc::Channel* channel);

  virtual void GetLatestTxId(const pb::mds::GetLatestTxIdRequest& request,
                             pb::mds::GetLatestTxIdResponse* response,
                             brpc::Controller* cntl, brpc::Channel* channel);

  virtual void CommitTx(const pb::mds::CommitTxRequest& request,
                        pb::mds::CommitTxResponse* response,
                        brpc::Controller* cntl, brpc::Channel* channel);

  virtual void AllocateVolumeBlockGroup(
      uint32_t fsId, uint32_t count, const std::string& owner,
      pb::mds::space::AllocateBlockGroupResponse* response,
      brpc::Controller* cntl, brpc::Channel* channel);

  virtual void AcquireVolumeBlockGroup(
      uint32_t fsId, uint64_t blockGroupOffset, const std::string& owner,
      pb::mds::space::AcquireBlockGroupResponse* response,
      brpc::Controller* cntl, brpc::Channel* channel);

  virtual void ReleaseVolumeBlockGroup(
      uint32_t fsId, const std::string& owner,
      const std::vector<dingofs::pb::mds::space::BlockGroup>& blockGroups,
      pb::mds::space::ReleaseBlockGroupResponse* response,
      brpc::Controller* cntl, brpc::Channel* channel);

  virtual void AllocOrGetMemcacheCluster(
      uint32_t fsId,
      pb::mds::topology::AllocOrGetMemcacheClusterResponse* response,
      brpc::Controller* cntl, brpc::Channel* channel);

  virtual void SetFsStats(const pb::mds::SetFsStatsRequest& request,
                          pb::mds::SetFsStatsResponse* response,
                          brpc::Controller* cntl, brpc::Channel* channel);
};

}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_RPCCLIENT_BASE_CLIENT_H_
