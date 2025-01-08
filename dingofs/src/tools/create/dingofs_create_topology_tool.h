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
 * Created Date: 2021-09-03
 * Author: chengyi01
 */

#ifndef DINGOFS_SRC_TOOLS_CREATE_DINGOFS_CREATE_TOPOLOGY_TOOL_H_
#define DINGOFS_SRC_TOOLS_CREATE_DINGOFS_CREATE_TOPOLOGY_TOOL_H_

#include <brpc/channel.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <json/json.h>

#include <list>
#include <string>
#include <vector>

#include "proto/topology.pb.h"
#include "mds/common/mds_define.h"
#include "tools/dingofs_tool.h"
#include "tools/dingofs_tool_define.h"
#include "utils/configuration.h"

namespace dingofs {
namespace tools {
namespace topology {

struct Server {
  std::string name;
  std::string internalIp;
  uint32_t internalPort;
  std::string externalIp;
  uint32_t externalPort;
  std::string zoneName;
  std::string poolName;
};

struct Pool {
  std::string name;
  uint32_t replicasNum;
  uint64_t copysetNum;
  uint32_t zoneNum;
};

struct Zone {
  std::string name;
  std::string poolName;
};

class CurvefsBuildTopologyTool : public dingofs::tools::CurvefsTool {
 public:
  CurvefsBuildTopologyTool()
      : dingofs::tools::CurvefsTool(
            std::string(dingofs::tools::kCreateTopologyCmd),
            std::string(dingofs::tools::kProgrameName)) {}

  ~CurvefsBuildTopologyTool() override = default;

  int Init() override;

  int InitTopoData();

  int HandleBuildCluster();

  int GetMaxTry() { return mdsAddressStr_.size(); }

  int TryAnotherMdsAddress();

  void PrintHelp() override {
    std::cout << "Example :" << std::endl;
    std::cout << programe_ << " " << command_ << " "
              << dingofs::tools::kConfPathHelp << std::endl;
  }

  int RunCommand() override;

  int Run() override {
    if (Init() < 0) {
      LOG(ERROR) << "CurvefsBuildTopologyTool init error.";
      return mds::topology::kRetCodeCommonErr;
    }
    int ret = InitTopoData();
    if (ret < 0) {
      LOG(ERROR) << "Init topo json data error.";
      return ret;
    }
    ret = RunCommand();
    return ret;
  }

 public:
  // for unit test
  const std::list<Server>& GetServerDatas() { return serverDatas_; }

  const std::list<Zone>& GetZoneDatas() { return zoneDatas_; }

  const std::list<Pool>& GetPoolDatas() { return poolDatas_; }

 private:
  int ReadClusterMap();
  int InitServerZoneData();
  int InitPoolData();
  int ScanCluster();
  int ScanPool();
  int RemovePoolsNotInNewTopo();
  int RemoveZonesNotInNewTopo();
  int RemoveServersNotInNewTopo();
  int CreatePool();
  int CreateZone();
  int CreateServer();

  int DealFailedRet(int ret, std::string operation);

  int ListPool(std::list<pb::mds::topology::PoolInfo>* pool_infos);

  int GetZonesInPool(mds::topology::PoolIdType poolid,
                     std::list<pb::mds::topology::ZoneInfo>* zone_infos);

  int GetServersInZone(mds::topology::ZoneIdType zoneid,
                       std::list<pb::mds::topology::ServerInfo>* server_infos);

  std::list<Server> serverDatas_;
  std::list<Zone> zoneDatas_;
  std::list<Pool> poolDatas_;

  std::list<mds::topology::ServerIdType> serverToDel_;
  std::list<mds::topology::ZoneIdType> zoneToDel_;
  std::list<mds::topology::PoolIdType> poolToDel_;

  std::vector<std::string> mdsAddressStr_;
  int mdsAddressIndex_;
  brpc::Channel channel_;
  Json::Value clusterMap_;
};

}  // namespace topology
}  // namespace tools
}  // namespace dingofs

#endif  // DINGOFS_SRC_TOOLS_CREATE_DINGOFS_CREATE_TOPOLOGY_TOOL_H_
