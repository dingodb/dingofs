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
 * @Project: curve
 * @Date: 2021-06-24 11:21:45
 * @Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_METASERVERCLIENT_METASERVER_CLIENT_H_
#define CURVEFS_SRC_MDS_METASERVERCLIENT_METASERVER_CLIENT_H_

#include <brpc/channel.h>

#include <set>
#include <string>

#include "curvefs/proto/cli2.pb.h"
#include "curvefs/proto/copyset.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"

using curvefs::common::PartitionInfo;
using curvefs::common::PartitionStatus;
using curvefs::metaserver::FsFileType;
using curvefs::metaserver::ManageInodeType;
using curvefs::metaserver::copyset::CliService2_Stub;
using curvefs::metaserver::copyset::CreateCopysetRequest;
using curvefs::metaserver::copyset::CreateCopysetResponse;
using curvefs::metaserver::copyset::GetLeaderRequest2;
using curvefs::metaserver::copyset::GetLeaderResponse2;

namespace curvefs {
namespace mds {
struct MetaserverOptions {
  std::string metaserverAddr;
  uint32_t rpcTimeoutMs;
  uint32_t rpcRetryTimes;
  uint32_t rpcRetryIntervalUs;
};

struct LeaderCtx {
  std::set<std::string> addrs;
  uint32_t poolId;
  uint32_t copysetId;
};

class MetaserverClient {
 public:
  explicit MetaserverClient(const MetaserverOptions& option) {
    options_ = option;
  }

  virtual ~MetaserverClient() = default;

  virtual FSStatusCode GetLeader(const LeaderCtx& tx, std::string* leader);

  virtual FSStatusCode DeleteInode(uint32_t fs_id, uint64_t inode_id);

  virtual FSStatusCode CreateRootInode(uint32_t fs_id, uint32_t pool_id,
                                       uint32_t copyset_id,
                                       uint32_t partition_id, uint32_t uid,
                                       uint32_t gid, uint32_t mode,
                                       const std::set<std::string>& addrs);

  virtual FSStatusCode CreateManageInode(uint32_t fs_id, uint32_t pool_id,
                                         uint32_t copyset_id,
                                         uint32_t partition_id, uint32_t uid,
                                         uint32_t gid, uint32_t mode,
                                         ManageInodeType manage_type,
                                         const std::set<std::string>& addrs);

  virtual FSStatusCode CreateDentry(uint32_t fs_id, uint32_t pool_id,
                                    uint32_t copyset_id, uint32_t partition_id,
                                    uint64_t parent_inode_id,
                                    const std::string& name, uint64_t inode_id,
                                    const std::set<std::string>& addrs);

  virtual FSStatusCode DeleteDentry(uint32_t pool_id, uint32_t copyset_id,
                                    uint32_t partition_id, uint32_t fs_id,
                                    uint64_t parent_inode_id,
                                    const std::string& name,
                                    const std::set<std::string>& addrs);

  virtual FSStatusCode CreatePartition(uint32_t fs_id, uint32_t pool_id,
                                       uint32_t copyset_id,
                                       uint32_t partition_id, uint64_t id_start,
                                       uint64_t id_end,
                                       const std::set<std::string>& addrs);

  virtual FSStatusCode DeletePartition(uint32_t pool_id, uint32_t copyset_id,
                                       uint32_t partition_id,
                                       const std::set<std::string>& addrs);

  virtual FSStatusCode CreateCopySet(uint32_t pool_id, uint32_t copyset_id,
                                     const std::set<std::string>& addrs);

  virtual FSStatusCode CreateCopySetOnOneMetaserver(uint32_t pool_id,
                                                    uint32_t copyset_id,
                                                    const std::string& addr);

 private:
  template <typename T, typename Request, typename Response>
  FSStatusCode SendRpc2MetaServer(
      Request* request, Response* response, const LeaderCtx& ctx,
      void (T::*func)(google::protobuf::RpcController*, const Request*,
                      Response*, google::protobuf::Closure*));

  MetaserverOptions options_;
  brpc::Channel channel_;
};
}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_SRC_MDS_METASERVERCLIENT_METASERVER_CLIENT_H_
