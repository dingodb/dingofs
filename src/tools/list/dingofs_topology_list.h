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
 * Created Date: 2021-12-17
 * Author: chengyi01
 */
#ifndef DINGOFS_SRC_TOOLS_LIST_DINGOFS_TOPOLOGY_LIST_H_
#define DINGOFS_SRC_TOOLS_LIST_DINGOFS_TOPOLOGY_LIST_H_

#include <brpc/channel.h>
#include <gflags/gflags.h>
#include <json/json.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "dingofs/topology.pb.h"
#include "mds/common/mds_define.h"
#include "tools/dingofs_tool.h"
#include "tools/dingofs_tool_define.h"
#include "tools/list/dingofs_topology_tree_json.h"

namespace dingofs {
namespace tools {
using mds::topology::MetaServerIdType;
using mds::topology::PoolIdType;
using mds::topology::ServerIdType;
using mds::topology::ZoneIdType;
namespace topology {
class TopologyTreeJson;
}

namespace list {

using ClusterInfo = std::pair<std::string, std::vector<PoolIdType>>;
using PoolInfoType =
    std::pair<pb::mds::topology::PoolInfo, std::vector<ZoneIdType>>;
using ZoneInfoType =
    std::pair<pb::mds::topology::ZoneInfo, std::vector<ServerIdType>>;
using ServerInfoType =
    std::pair<pb::mds::topology::ServerInfo, std::vector<MetaServerIdType>>;
using MetaserverInfoType = pb::mds::topology::MetaServerInfo;

/**
 * @brief
 * pool redundanceandplacementpolicy
 *
 * @details
 */
struct PoolPolicy {
  uint32_t copysetNum;
  uint32_t replicaNum;
  uint32_t zoneNum;
  bool error = false;

  explicit PoolPolicy(const std::string& jsonStr);

  friend std::ostream& operator<<(std::ostream& os, const PoolPolicy& policy);
};

class TopologyListTool
    : public CurvefsToolRpc<pb::mds::topology::ListTopologyRequest,
                            pb::mds::topology::ListTopologyResponse,
                            pb::mds::topology::TopologyService_Stub> {
 public:
  explicit TopologyListTool(const std::string& cmd = kTopologyListCmd,
                            bool show = true)
      : CurvefsToolRpc(cmd) {
    show_ = show;
  }
  void PrintHelp() override;
  int Init() override;

  friend class topology::TopologyTreeJson;

 protected:
  bool OutputFile();
  void AddUpdateFlags() override;
  bool AfterSendRequestToHost(const std::string& host) override;

  /**
   * @brief Get the PoolInfo From Response, fill into poolId2PoolInfo
   * not include zoneId list (will be filled in GetZoneInfoFromResponse)
   * will fill clusterId2CLusterInfo's poolId list
   *
   * @return true
   * @return false
   * @details
   *
   */
  bool GetPoolInfoFromResponse();

  /**
   * @brief Get the ZoneInfo From Response, fill into zoneId2ZoneInfo
   * not include serverId list (will be filled in GetServerInfoFromResponse)
   * will fill poolId2PoolInfo's zoneId list
   *
   * @return true
   * @return false
   * @details
   */
  bool GetZoneInfoFromResponse();

  /**
   * @brief Get the ServerInfo From Response, fill into serverId2ServerInfo
   * not include serverId list
   * will fill zoneId2ZoneInfo's serverId list
   *
   * @return true
   * @return false
   * @details
   */
  bool GetServerInfoFromResponse();

  /**
   * @brief Get the MetaserverInfo From Response,
   * fill into metaserverId2MetaserverInfo
   * will fill serverId2ServerInfo's metaserverId list
   *
   * @return true
   * @return false
   * @details
   */
  bool GetMetaserverInfoFromResponse();

  void ShowPoolInfo(const PoolInfoType& pool) const;

  void ShowZoneInfo(const ZoneInfoType& zone) const;

  void ShowServerInfo(const ServerInfoType& server) const;

  void ShowMetaserverInfo(const MetaserverInfoType& metaserver) const;

 protected:
  std::string clusterId_;
  /**
   * poolId to clusterInfo and poolIds which belongs to pool
   */
  std::map<std::string, ClusterInfo> clusterId2CLusterInfo_;

  /**
   * @brief poolId to poolInfo and zoneIds which belongs to pool
   *
   * @details
   */
  std::map<mds::topology::PoolIdType, PoolInfoType> poolId2PoolInfo_;

  /**
   * @brief zoneId to zoneInfo and serverIds which belongs to zone
   *
   * @details
   */
  std::map<mds::topology::ZoneIdType, ZoneInfoType> zoneId2ZoneInfo_;

  /**
   * @brief serverId to serverInfo and metaserverIds which belongs to server
   *
   * @details
   */
  std::map<mds::topology::ServerIdType, ServerInfoType> serverId2ServerInfo_;

  /**
   * @brief metaserverId to metaserverInfo
   *
   * @details
   */
  std::map<mds::topology::MetaServerIdType, MetaserverInfoType>
      metaserverId2MetaserverInfo_;
};

}  // namespace list
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_LIST_DINGOFS_TOPOLOGY_LIST_H_
