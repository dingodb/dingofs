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
 * Project: curve
 * Created Date: 2021-09-03
 * Author: wanghai01
 */

// TODO(chengyi): add out put when build sucess

#include "curvefs/src/tools/create/curvefs_create_topology_tool.h"

#include <fstream>

#include "src/common/string_util.h"

DECLARE_string(mds_addr);
DECLARE_string(cluster_map);
DECLARE_string(confPath);
DECLARE_string(op);

using ::curve::common::SplitString;

namespace curvefs {
namespace mds {
namespace topology {

void UpdateFlagsFromConf(curve::common::Configuration* conf) {
  LOG_IF(FATAL, !conf->LoadConfig());
  google::CommandLineFlagInfo info;
  if (GetCommandLineFlagInfo("mds_addr", &info) && info.is_default) {
    conf->GetStringValue("mdsAddr", &FLAGS_mds_addr);
    LOG(INFO) << "conf: " << FLAGS_mds_addr;
  }

  if (GetCommandLineFlagInfo("rpcTimeoutMs", &info) && info.is_default) {
    conf->GetUInt32Value("rpcTimeoutMs", &FLAGS_rpcTimeoutMs);
  }

  if (GetCommandLineFlagInfo("cluster_map", &info) && info.is_default) {
    conf->GetStringValue("topoFilePath", &FLAGS_cluster_map);
  }
}

int CurvefsBuildTopologyTool::Init() {
  std::string conf_path = FLAGS_confPath;
  curve::common::Configuration conf;
  conf.SetConfigPath(conf_path);
  UpdateFlagsFromConf(&conf);
  SplitString(FLAGS_mds_addr, ",", &mdsAddressStr_);
  if (mdsAddressStr_.size() <= 0) {
    LOG(ERROR) << "no avaliable mds address.";
    return kRetCodeCommonErr;
  }

  for (const auto& addr : mdsAddressStr_) {
    butil::EndPoint endpt;
    if (butil::str2endpoint(addr.c_str(), &endpt) < 0) {
      LOG(ERROR) << "Invalid sub mds ip:port provided: " << addr;
      return kRetCodeCommonErr;
    }
  }
  mdsAddressIndex_ = -1;
  return 0;
}

int CurvefsBuildTopologyTool::TryAnotherMdsAddress() {
  if (mdsAddressStr_.size() == 0) {
    LOG(ERROR) << "no avaliable mds address.";
    return kRetCodeCommonErr;
  }
  mdsAddressIndex_ = (mdsAddressIndex_ + 1) % mdsAddressStr_.size();
  std::string mds_address = mdsAddressStr_[mdsAddressIndex_];
  LOG(INFO) << "try mds address(" << mdsAddressIndex_ << "): " << mds_address;
  int ret = channel_.Init(mds_address.c_str(), nullptr);
  if (ret != 0) {
    LOG(ERROR) << "Fail to init channel to mdsAddress: " << mds_address;
  }
  return ret;
}

int CurvefsBuildTopologyTool::DealFailedRet(int ret, std::string operation) {
  if (kRetCodeRedirectMds == ret) {
    LOG(WARNING) << operation
                 << " fail on mds: " << mdsAddressStr_[mdsAddressIndex_];
  } else {
    LOG(ERROR) << operation << " fail.";
  }
  return ret;
}

int CurvefsBuildTopologyTool::InitTopoData() {
  int ret = ReadClusterMap();
  if (ret != 0) {
    return DealFailedRet(ret, "read cluster map");
  }

  ret = InitPoolData();
  if (ret != 0) {
    return DealFailedRet(ret, "init pool data");
  }

  ret = InitServerZoneData();
  if (ret != 0) {
    return DealFailedRet(ret, "init server data");
  }

  return ret;
}

int CurvefsBuildTopologyTool::HandleBuildCluster() {
  int ret = ScanCluster();
  if (ret != 0) {
    return DealFailedRet(ret, "scan cluster");
  }

  ret = RemoveServersNotInNewTopo();
  if (ret != 0) {
    return DealFailedRet(ret, "remove server");
  }

  ret = RemoveZonesNotInNewTopo();
  if (ret != 0) {
    return DealFailedRet(ret, "remove zone");
  }

  ret = RemovePoolsNotInNewTopo();
  if (ret != 0) {
    return DealFailedRet(ret, "remove pool");
  }

  ret = CreatePool();
  if (ret != 0) {
    return DealFailedRet(ret, "create pool");
  }

  ret = CreateZone();
  if (ret != 0) {
    return DealFailedRet(ret, "create zone");
  }

  ret = CreateServer();
  if (ret != 0) {
    return DealFailedRet(ret, "create server");
  }

  return ret;
}

int CurvefsBuildTopologyTool::ReadClusterMap() {
  std::ifstream fin;
  fin.open(FLAGS_cluster_map.c_str(), std::ios::in);
  if (fin.is_open()) {
    Json::CharReaderBuilder reader;
    JSONCPP_STRING errs;
    bool ok = Json::parseFromStream(reader, fin, &clusterMap_, &errs);
    fin.close();
    if (!ok) {
      LOG(ERROR) << "Parse cluster map file " << FLAGS_cluster_map
                 << " fail: " << errs;
      return -1;
    }
  } else {
    LOG(ERROR) << "open cluster map file : " << FLAGS_cluster_map << " fail.";
    return -1;
  }
  return 0;
}

int CurvefsBuildTopologyTool::InitPoolData() {
  if (clusterMap_[kPools].isNull()) {
    LOG(ERROR) << "No pools in cluster map";
    return -1;
  }
  for (const auto& pool : clusterMap_[kPools]) {
    Pool pool_data;
    if (!pool[kName].isString()) {
      LOG(ERROR) << "pool name must be string";
      return -1;
    }
    pool_data.name = pool[kName].asString();
    if (!pool[kReplicasNum].isUInt()) {
      LOG(ERROR) << "pool replicasnum must be uint";
      return -1;
    }
    pool_data.replicasNum = pool[kReplicasNum].asUInt();
    if (!pool[kCopysetNum].isUInt64()) {
      LOG(ERROR) << "pool copysetnum must be uint64";
      return -1;
    }
    pool_data.copysetNum = pool[kCopysetNum].asUInt64();
    if (!pool[kZoneNum].isUInt64()) {
      LOG(ERROR) << "pool zonenum must be uint64";
      return -1;
    }
    pool_data.zoneNum = pool[kZoneNum].asUInt();

    poolDatas_.emplace_back(pool_data);
  }
  return 0;
}

int CurvefsBuildTopologyTool::InitServerZoneData() {
  if (clusterMap_[kServers].isNull()) {
    LOG(ERROR) << "No servers in cluster map";
    return -1;
  }
  for (const auto& server : clusterMap_[kServers]) {
    Server server_data;
    Zone zone_data;
    if (!server[kName].isString()) {
      LOG(ERROR) << "server name must be string";
      return -1;
    }
    server_data.name = server[kName].asString();
    if (!server[kInternalIp].isString()) {
      LOG(ERROR) << "server internal ip must be string";
      return -1;
    }
    server_data.internalIp = server[kInternalIp].asString();
    if (!server[kInternalPort].isUInt()) {
      LOG(ERROR) << "server internal port must be uint";
      return -1;
    }
    server_data.internalPort = server[kInternalPort].asUInt();
    if (!server[kExternalIp].isString()) {
      LOG(ERROR) << "server internal port must be string";
      return -1;
    }
    server_data.externalIp = server[kExternalIp].asString();
    if (!server[kExternalPort].isUInt()) {
      LOG(ERROR) << "server internal port must be string";
      return -1;
    }
    server_data.externalPort = server[kExternalPort].asUInt();
    if (!server[kZone].isString()) {
      LOG(ERROR) << "server zone must be string";
      return -1;
    }
    server_data.zoneName = server[kZone].asString();
    zone_data.name = server[kZone].asString();
    if (!server[kPool].isString()) {
      LOG(ERROR) << "server pool must be string";
      return -1;
    }
    server_data.poolName = server[kPool].asString();
    zone_data.poolName = server[kPool].asString();

    serverDatas_.emplace_back(server_data);

    if (std::find_if(zoneDatas_.begin(), zoneDatas_.end(),
                     [server_data](Zone& data) {
                       return (data.poolName == server_data.poolName) &&
                              (data.name == server_data.zoneName);
                     }) == zoneDatas_.end()) {
      zoneDatas_.emplace_back(zone_data);
    }
  }
  return 0;
}

int CurvefsBuildTopologyTool::ScanCluster() {
  // get pools and compare
  // De-duplication
  std::list<PoolInfo> pool_infos;
  int ret = ListPool(&pool_infos);
  if (ret != 0) {
    return ret;
  }

  for (auto& pool_info : pool_infos) {
    auto ix = std::find_if(
        poolDatas_.begin(), poolDatas_.end(),
        [&pool_info](Pool& data) { return data.name == pool_info.poolname(); });
    if (ix != poolDatas_.end()) {
      poolDatas_.erase(ix);
    } else {
      poolToDel_.emplace_back(pool_info.poolid());
    }
  }

  // get zone and compare
  // De-duplication
  std::list<ZoneInfo> zone_infos;
  for (const auto& pool : pool_infos) {
    ret = GetZonesInPool(pool.poolid(), &zone_infos);
    if (ret != 0) {
      return ret;
    }
  }

  for (auto& zone_info : zone_infos) {
    auto ix = std::find_if(zoneDatas_.begin(), zoneDatas_.end(),
                           [&zone_info](Zone& data) {
                             return (data.poolName == zone_info.poolname()) &&
                                    (data.name == zone_info.zonename());
                           });
    if (ix != zoneDatas_.end()) {
      zoneDatas_.erase(ix);
    } else {
      zoneToDel_.emplace_back(zone_info.zoneid());
    }
  }

  // get server and compare
  // De-duplication
  std::list<ServerInfo> server_infos;
  for (const auto& zone : zone_infos) {
    ret = GetServersInZone(zone.zoneid(), &server_infos);
    if (ret != 0) {
      return ret;
    }
  }

  for (auto& server_info : server_infos) {
    auto ix = std::find_if(serverDatas_.begin(), serverDatas_.end(),
                           [&server_info](Server& data) {
                             return (data.name == server_info.hostname()) &&
                                    (data.zoneName == server_info.zonename()) &&
                                    (data.poolName == server_info.poolname());
                           });
    if (ix != serverDatas_.end()) {
      serverDatas_.erase(ix);
    } else {
      serverToDel_.emplace_back(server_info.serverid());
    }
  }

  return 0;
}

int CurvefsBuildTopologyTool::ListPool(std::list<PoolInfo>* pool_infos) {
  TopologyService_Stub stub(&channel_);
  ListPoolRequest request;
  ListPoolResponse response;
  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

  LOG(INFO) << "ListPool send request: " << request.DebugString();
  stub.ListPool(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    return kRetCodeRedirectMds;
  }

  if (response.statuscode() != TopoStatusCode::TOPO_OK) {
    LOG(ERROR) << "ListPool Rpc response fail. "
               << "Message is :" << response.DebugString();
    return response.statuscode();
  } else {
    LOG(INFO) << "Received ListPool rpc response success, "
              << response.DebugString();
  }

  for (int i = 0; i < response.poolinfos_size(); i++) {
    pool_infos->emplace_back(response.poolinfos(i));
  }
  return 0;
}

int CurvefsBuildTopologyTool::GetZonesInPool(PoolIdType poolid,
                                             std::list<ZoneInfo>* zone_infos) {
  TopologyService_Stub stub(&channel_);
  ListPoolZoneRequest request;
  ListPoolZoneResponse response;
  request.set_poolid(poolid);

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

  LOG(INFO) << "ListZoneInPool, send request: " << request.DebugString();

  stub.ListPoolZone(&cntl, &request, &response, nullptr);

  if (cntl.Failed()) {
    return kRetCodeRedirectMds;
  }
  if (response.statuscode() != TopoStatusCode::TOPO_OK) {
    LOG(ERROR) << "ListPoolZone rpc response fail. "
               << "Message is :" << response.DebugString()
               << " , physicalpoolid = " << poolid;
    return response.statuscode();
  } else {
    LOG(INFO) << "Received ListPoolZone rpc response success, "
              << response.DebugString();
  }

  for (int i = 0; i < response.zones_size(); i++) {
    zone_infos->emplace_back(response.zones(i));
  }
  return 0;
}

int CurvefsBuildTopologyTool::GetServersInZone(
    ZoneIdType zoneid, std::list<ServerInfo>* server_infos) {
  TopologyService_Stub stub(&channel_);
  ListZoneServerRequest request;
  ListZoneServerResponse response;
  request.set_zoneid(zoneid);
  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

  LOG(INFO) << "ListZoneServer, send request: " << request.DebugString();

  stub.ListZoneServer(&cntl, &request, &response, nullptr);

  if (cntl.Failed()) {
    return kRetCodeRedirectMds;
  }
  if (response.statuscode() != TopoStatusCode::TOPO_OK) {
    LOG(ERROR) << "ListZoneServer rpc response fail. "
               << "Message is :" << response.DebugString()
               << " , zoneid = " << zoneid;
    return response.statuscode();
  } else {
    LOG(INFO) << "ListZoneServer rpc response success, "
              << response.DebugString();
  }

  for (int i = 0; i < response.serverinfo_size(); i++) {
    server_infos->emplace_back(response.serverinfo(i));
  }
  return 0;
}

int CurvefsBuildTopologyTool::RemovePoolsNotInNewTopo() {
  TopologyService_Stub stub(&channel_);
  for (auto it : poolToDel_) {
    DeletePoolRequest request;
    DeletePoolResponse response;
    request.set_poolid(it);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

    LOG(INFO) << "ClearPool, send request: " << request.DebugString();

    stub.DeletePool(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN || cntl.ErrorCode() == brpc::ELOGOFF) {
      return kRetCodeRedirectMds;
    } else if (cntl.Failed()) {
      LOG(ERROR) << "ClearPool errcorde = " << response.statuscode()
                 << ", error content:" << cntl.ErrorText()
                 << " , poolId = " << it;
      return kRetCodeCommonErr;
    }

    if (response.statuscode() != TopoStatusCode::TOPO_OK) {
      LOG(ERROR) << "ClearPool rpc response fail. "
                 << "Message is :" << response.DebugString()
                 << " , poolId =" << it;
      return response.statuscode();
    } else {
      LOG(INFO) << "Received ClearPool response success, "
                << response.DebugString();
    }
  }
  return 0;
}

int CurvefsBuildTopologyTool::RemoveZonesNotInNewTopo() {
  TopologyService_Stub stub(&channel_);
  for (auto it : zoneToDel_) {
    DeleteZoneRequest request;
    DeleteZoneResponse response;
    request.set_zoneid(it);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

    LOG(INFO) << "ClearZone, send request: " << request.DebugString();

    stub.DeleteZone(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN || cntl.ErrorCode() == brpc::ELOGOFF) {
      return kRetCodeRedirectMds;
    } else if (cntl.Failed()) {
      LOG(ERROR) << "ClearZone, errcorde = " << response.statuscode()
                 << ", error content:" << cntl.ErrorText()
                 << " , zoneId = " << it;
      return kRetCodeCommonErr;
    }
    if (response.statuscode() != TopoStatusCode::TOPO_OK) {
      LOG(ERROR) << "ClearZone Rpc response fail. "
                 << "Message is :" << response.DebugString()
                 << " , zoneId = " << it;
      return response.statuscode();
    } else {
      LOG(INFO) << "Received ClearZone Rpc success, " << response.DebugString();
    }
  }
  return 0;
}

int CurvefsBuildTopologyTool::RemoveServersNotInNewTopo() {
  TopologyService_Stub stub(&channel_);
  for (auto it : serverToDel_) {
    DeleteServerRequest request;
    DeleteServerResponse response;
    request.set_serverid(it);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

    LOG(INFO) << "ClearServer, send request: " << request.DebugString();

    stub.DeleteServer(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN || cntl.ErrorCode() == brpc::ELOGOFF) {
      return kRetCodeRedirectMds;
    } else if (cntl.Failed()) {
      LOG(ERROR) << "ClearServer, errcorde = " << response.statuscode()
                 << ", error content : " << cntl.ErrorText()
                 << " , serverId = " << it;
      return kRetCodeCommonErr;
    }
    if (response.statuscode() != TopoStatusCode::TOPO_OK) {
      LOG(ERROR) << "ClearServer Rpc response fail. "
                 << "Message is :" << response.DebugString()
                 << " , serverId = " << it;
      return response.statuscode();
    } else {
      LOG(INFO) << "Received ClearServer Rpc success, "
                << response.DebugString();
    }
  }
  return 0;
}

int CurvefsBuildTopologyTool::CreatePool() {
  TopologyService_Stub stub(&channel_);
  for (auto it : poolDatas_) {
    CreatePoolRequest request;
    CreatePoolResponse response;
    request.set_poolname(it.name);
    std::string replica_num_str = std::to_string(it.replicasNum);
    std::string copyset_num_str = std::to_string(it.copysetNum);
    std::string zone_num_str = std::to_string(it.zoneNum);
    std::string rap_string = "{\"replicaNum\":" + replica_num_str +
                             ", \"copysetNum\":" + copyset_num_str +
                             ", \"zoneNum\":" + zone_num_str + "}";
    request.set_redundanceandplacementpolicy(rap_string);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

    LOG(INFO) << "CreatePool, send request: " << request.DebugString();

    stub.CreatePool(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN || cntl.ErrorCode() == brpc::ELOGOFF) {
      return kRetCodeRedirectMds;
    } else if (cntl.Failed()) {
      LOG(ERROR) << "CreatePool errcorde = " << response.statuscode()
                 << ", error content:" << cntl.ErrorText()
                 << " , poolName = " << it.name;
      return kRetCodeCommonErr;
    }

    if (response.statuscode() != TopoStatusCode::TOPO_OK) {
      LOG(ERROR) << "CreatePool rpc response fail. "
                 << "Message is :" << response.DebugString()
                 << " , poolName =" << it.name;
      return response.statuscode();
    } else {
      LOG(INFO) << "Received CreatePool response success, "
                << response.DebugString();
    }
  }
  return 0;
}

int CurvefsBuildTopologyTool::CreateZone() {
  TopologyService_Stub stub(&channel_);
  for (auto it : zoneDatas_) {
    CreateZoneRequest request;
    CreateZoneResponse response;
    request.set_zonename(it.name);
    request.set_poolname(it.poolName);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

    LOG(INFO) << "CreateZone, send request: " << request.DebugString();

    stub.CreateZone(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN || cntl.ErrorCode() == brpc::ELOGOFF) {
      return kRetCodeRedirectMds;
    } else if (cntl.Failed()) {
      LOG(ERROR) << "CreateZone, errcorde = " << response.statuscode()
                 << ", error content:" << cntl.ErrorText()
                 << " , zoneName = " << it.name;
      return kRetCodeCommonErr;
    }
    if (response.statuscode() != TopoStatusCode::TOPO_OK) {
      LOG(ERROR) << "CreateZone Rpc response fail. "
                 << "Message is :" << response.DebugString()
                 << " , zoneName = " << it.name;
      return response.statuscode();
    } else {
      LOG(INFO) << "Received CreateZone Rpc success, "
                << response.DebugString();
    }
  }
  return 0;
}

int CurvefsBuildTopologyTool::CreateServer() {
  TopologyService_Stub stub(&channel_);
  for (auto it : serverDatas_) {
    ServerRegistRequest request;
    ServerRegistResponse response;
    request.set_hostname(it.name);
    request.set_internalip(it.internalIp);
    request.set_internalport(it.internalPort);
    request.set_externalip(it.externalIp);
    request.set_externalport(it.externalPort);
    request.set_zonename(it.zoneName);
    request.set_poolname(it.poolName);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

    LOG(INFO) << "CreateServer, send request: " << request.DebugString();

    stub.RegistServer(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN || cntl.ErrorCode() == brpc::ELOGOFF) {
      return kRetCodeRedirectMds;
    } else if (cntl.Failed()) {
      LOG(ERROR) << "RegistServer, errcorde = " << response.statuscode()
                 << ", error content : " << cntl.ErrorText()
                 << " , serverName = " << it.name;
      return kRetCodeCommonErr;
    }
    if (response.statuscode() == TopoStatusCode::TOPO_OK) {
      LOG(INFO) << "Received RegistServer Rpc response success, "
                << response.DebugString();
    } else if (response.statuscode() ==
               TopoStatusCode::TOPO_IP_PORT_DUPLICATED) {
      LOG(INFO) << "Server already exist";
    } else {
      LOG(ERROR) << "RegistServer Rpc response fail. "
                 << "Message is :" << response.DebugString()
                 << " , serverName = " << it.name;
      return response.statuscode();
    }
  }
  return 0;
}

int CurvefsBuildTopologyTool::RunCommand() {
  int ret = 0;
  int max_try = GetMaxTry();
  int retry = 0;
  for (; retry < max_try; retry++) {
    ret = TryAnotherMdsAddress();
    if (ret < 0) {
      return kRetCodeCommonErr;
    }

    ret = HandleBuildCluster();
    if (ret != kRetCodeRedirectMds) {
      break;
    }
  }
  if (retry >= max_try) {
    LOG(ERROR) << "rpc retry times exceed.";
    return kRetCodeCommonErr;
  }
  if (ret != 0) {
    LOG(ERROR) << "exec fail, ret = " << ret;
  } else {
    LOG(INFO) << "exec success, ret = " << ret;
  }
  return ret;
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
