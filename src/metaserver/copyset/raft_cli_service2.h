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
 * Date: Wed Sep  1 11:17:56 CST 2021
 * Author: wuhanqing
 */

#ifndef DINGOFS_SRC_METASERVER_COPYSET_RAFT_CLI_SERVICE2_H_
#define DINGOFS_SRC_METASERVER_COPYSET_RAFT_CLI_SERVICE2_H_

#include <braft/node.h>
#include <butil/memory/ref_counted.h>
#include <butil/status.h>

#include <string>

#include "dingofs/cli2.pb.h"
#include "metaserver/common/types.h"
#include "metaserver/copyset/copyset_node_manager.h"

namespace dingofs {
namespace metaserver {
namespace copyset {

class RaftCliService2 : public pb::metaserver::copyset::CliService2 {
 public:
  explicit RaftCliService2(CopysetNodeManager* manager);

  void GetLeader(google::protobuf::RpcController* controller,
                 const pb::metaserver::copyset::GetLeaderRequest2* request,
                 pb::metaserver::copyset::GetLeaderResponse2* response,
                 google::protobuf::Closure* done) override;

  void AddPeer(google::protobuf::RpcController* controller,
               const pb::metaserver::copyset::AddPeerRequest2* request,
               pb::metaserver::copyset::AddPeerResponse2* response,
               google::protobuf::Closure* done) override;

  void RemovePeer(google::protobuf::RpcController* controller,
                  const pb::metaserver::copyset::RemovePeerRequest2* request,
                  pb::metaserver::copyset::RemovePeerResponse2* response,
                  google::protobuf::Closure* done) override;

  void ChangePeers(google::protobuf::RpcController* controller,
                   const pb::metaserver::copyset::ChangePeersRequest2* request,
                   pb::metaserver::copyset::ChangePeersResponse2* response,
                   google::protobuf::Closure* done) override;

  void TransferLeader(
      google::protobuf::RpcController* controller,
      const pb::metaserver::copyset::TransferLeaderRequest2* request,
      pb::metaserver::copyset::TransferLeaderResponse2* response,
      google::protobuf::Closure* done) override;

 private:
  butil::Status GetNode(scoped_refptr<braft::NodeImpl>* node, PoolId poolId,
                        CopysetId copysetId, const std::string& peerId);

  CopysetNodeManager* nodeManager_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_COPYSET_RAFT_CLI_SERVICE2_H_
