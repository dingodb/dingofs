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
 * Project: dingo
 * Created Date: 2022-01-14
 * Author: chengyi01
 */

#ifndef DINGOFS_SRC_TOOLS_LIST_DINGOFS_TOPOLOGY_TREE_JSON_H_
#define DINGOFS_SRC_TOOLS_LIST_DINGOFS_TOPOLOGY_TREE_JSON_H_

#include <json/json.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "dingofs/topology.pb.h"
#include "mds/common/mds_define.h"
#include "tools/list/dingofs_topology_list.h"

namespace dingofs {
namespace tools {

namespace list {
class TopologyListTool;
}

namespace topology {

using dingofs::mds::topology::MetaServerIdType;
using dingofs::mds::topology::PoolIdType;
using dingofs::mds::topology::ServerIdType;
using dingofs::mds::topology::ZoneIdType;

using ClusterInfo = std::pair<std::string, std::vector<PoolIdType>>;
using PoolInfoType =
    std::pair<pb::mds::topology::PoolInfo, std::vector<ZoneIdType>>;
using ZoneInfoType =
    std::pair<pb::mds::topology::ZoneInfo, std::vector<ServerIdType>>;
using ServerInfoType =
    std::pair<pb::mds::topology::ServerInfo, std::vector<MetaServerIdType>>;
using MetaserverInfoType = pb::mds::topology::MetaServerInfo;

class TopologyTreeJson {
 public:
  explicit TopologyTreeJson(const list::TopologyListTool& topologyTool);

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
  std::map<PoolIdType, PoolInfoType> poolId2PoolInfo_;

  /**
   * @brief zoneId to zoneInfo and serverIds which belongs to zone
   *
   * @details
   */
  std::map<ZoneIdType, ZoneInfoType> zoneId2ZoneInfo_;

  /**
   * @brief serverId to serverInfo and metaserverIds which belongs to server
   *
   * @details
   */
  std::map<ServerIdType, ServerInfoType> serverId2ServerInfo_;

  /**
   * @brief metaserverId to metaserverInfo
   *
   * @details
   */
  std::map<MetaServerIdType, MetaserverInfoType> metaserverId2MetaserverInfo_;

 public:
  bool BuildClusterMapPools(Json::Value* pools);

  bool BuildClusterMapServers(Json::Value* servers);

  bool BuildJsonValue(Json::Value* value, const std::string& jsonType);

  bool GetClusterTree(Json::Value* cluster, const std::string& clusterId);

  bool GetPoolTree(Json::Value* pool, uint64_t poolId);

  bool GetZoneTree(Json::Value* zone, uint64_t zoneId);

  bool GetServerTree(Json::Value* server, uint64_t serverId);

  bool GetMetaserverTree(Json::Value* metaserver, uint64_t metaserverId);
};

}  // namespace topology
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_LIST_DINGOFS_TOPOLOGY_TREE_JSON_H_
