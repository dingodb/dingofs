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
 * Created Date: 2021-09-16
 * Author: chenwei
 */

#include "curvefs/src/mds/heartbeat/heartbeat_manager.h"

#include <glog/logging.h>

#include <list>
#include <set>

#include "curvefs/src/mds/topology/deal_peerid.h"

using ::curvefs::mds::topology::CopySetIdType;
using ::curvefs::mds::topology::CopySetKey;
using ::curvefs::mds::topology::MetaServer;
using ::curvefs::mds::topology::MetaServerSpace;
using ::curvefs::mds::topology::PoolIdType;
using ::curvefs::mds::topology::TopoStatusCode;
using ::curvefs::mds::topology::UNINITIALIZE_ID;

namespace curvefs {
namespace mds {
namespace heartbeat {
HeartbeatManager::HeartbeatManager(
    const HeartbeatOption& option, const std::shared_ptr<Topology>& topology,
    const std::shared_ptr<Coordinator>& coordinator)
    : topology_(topology) {
  healthyChecker_ =
      std::make_shared<MetaserverHealthyChecker>(option, topology);

  topoUpdater_ = std::make_shared<TopoUpdater>(topology);

  copysetConfGenerator_ = std::make_shared<CopysetConfGenerator>(
      topology, coordinator, option.mdsStartTime, option.cleanFollowerAfterMs);

  isStop_ = true;
  metaserverHealthyCheckerRunInter_ = option.heartbeatMissTimeOutMs;
}

void HeartbeatManager::Init() {
  for (auto value : topology_->GetMetaServerInCluster()) {
    healthyChecker_->UpdateLastReceivedHeartbeatTime(value,
                                                     steady_clock::now());
  }
  LOG(INFO) << "init heartbeatManager ok!";
}

void HeartbeatManager::Run() {
  if (isStop_.exchange(false)) {
    backEndThread_ = Thread(&HeartbeatManager::MetaServerHealthyChecker, this);
    LOG(INFO) << "heartbeatManager start running";
  } else {
    LOG(INFO) << "heartbeatManager already is running";
  }
}

void HeartbeatManager::Stop() {
  if (!isStop_.exchange(true)) {
    LOG(INFO) << "stop heartbeatManager...";
    sleeper_.interrupt();
    backEndThread_.join();
    LOG(INFO) << "stop heartbeatManager ok.";
  } else {
    LOG(INFO) << "heartbeatManager not running.";
  }
}

void HeartbeatManager::MetaServerHealthyChecker() {
  while (sleeper_.wait_for(
      std::chrono::milliseconds(metaserverHealthyCheckerRunInter_))) {
    healthyChecker_->CheckHeartBeatInterval();
  }
}

void HeartbeatManager::UpdateMetaServerSpace(
    const MetaServerHeartbeatRequest& request) {
  MetaServerSpace space(request.spacestatus());
  TopoStatusCode ret =
      topology_->UpdateMetaServerSpace(space, request.metaserverid());
  if (ret != TopoStatusCode::TOPO_OK) {
    LOG(ERROR) << "heartbeat UpdateMetaServerSpace fail, ret = "
               << TopoStatusCode_Name(ret);
  }
}

void HeartbeatManager::MetaServerHeartbeat(
    const MetaServerHeartbeatRequest& request,
    MetaServerHeartbeatResponse* response) {
  response->set_statuscode(HeartbeatStatusCode::hbOK);
  // check validity of heartbeat request
  HeartbeatStatusCode ret = CheckRequest(request);
  if (ret != HeartbeatStatusCode::hbOK) {
    LOG(ERROR) << "heartbeatManager get error request";
    response->set_statuscode(ret);
    return;
  }

  // record startUpTime data from metaserver to topology
  topology_->UpdateMetaServerStartUpTime(request.starttime(),
                                         request.metaserverid());

  // pass heartbeat timestamp to metaserver health checker
  healthyChecker_->UpdateLastReceivedHeartbeatTime(request.metaserverid(),
                                                   steady_clock::now());

  UpdateMetaServerSpace(request);

  // dealing with copysets included in the heartbeat request
  for (const auto& value : request.copysetinfos()) {
    // convert copysetInfo from heartbeat format to topology format
    ::curvefs::mds::topology::CopySetInfo report_copy_set_info;
    if (!TransformHeartbeatCopySetInfoToTopologyOne(value,
                                                    &report_copy_set_info)) {
      LOG(ERROR) << "heartbeatManager receive copyset(" << value.poolid() << ","
                 << value.copysetid()
                 << ") information, but can not transfer to topology one";
      response->set_statuscode(HeartbeatStatusCode::hbAnalyseCopysetError);
      continue;
    }

    // forward reported copyset info to CopysetConfGenerator
    CopySetConf conf;
    ConfigChangeInfo config_ch_info;
    if (copysetConfGenerator_->GenCopysetConf(
            request.metaserverid(), report_copy_set_info,
            value.configchangeinfo(), &conf)) {
      CopySetConf* res = response->add_needupdatecopysets();
      *res = conf;
    }

    // convert partitionInfo from heartbeat format to topology format
    std::list<::curvefs::mds::topology::Partition> partition_list;
    for (int32_t i = 0; i < value.partitioninfolist_size(); i++) {
      partition_list.emplace_back(value.partitioninfolist(i));
    }

    // if a copyset is the leader, update (e.g. epoch) topology according
    // to its info
    if (request.metaserverid() == report_copy_set_info.GetLeader()) {
      topoUpdater_->UpdateCopysetTopo(report_copy_set_info);
      if (!value.has_iscopysetloading() || !value.iscopysetloading()) {
        topoUpdater_->UpdatePartitionTopo(report_copy_set_info.GetId(),
                                          partition_list);
      }
    }
  }
}

HeartbeatStatusCode HeartbeatManager::CheckRequest(
    const MetaServerHeartbeatRequest& request) {
  MetaServer meta_server;

  // check for validity of metaserver id
  if (!topology_->GetMetaServer(request.metaserverid(), &meta_server)) {
    LOG(ERROR) << "heartbeatManager receive heartbeat from metaServer: "
               << request.metaserverid() << ", ip:" << request.ip()
               << ", port:" << request.port()
               << ", but topology do not contain this one";
    return HeartbeatStatusCode::hbMetaServerUnknown;
  }

  // mismatch ip address reported by metaserver and mds record
  if (request.ip() != meta_server.GetInternalIp() ||
      request.port() != meta_server.GetInternalPort()) {
    LOG(ERROR) << "heartbeatManager receive heartbeat from metaServer: "
               << request.metaserverid()
               << ", but find report ip:" << request.ip()
               << ", report port:" << request.port()
               << " do not consistent with topo record ip:"
               << meta_server.GetInternalIp()
               << ", record port:" << meta_server.GetInternalPort();
    return HeartbeatStatusCode::hbMetaServerIpPortNotMatch;
  }

  // mismatch token reported by metaserver and mds record
  if (request.token() != meta_server.GetToken()) {
    LOG(ERROR) << "heartbeatManager receive heartbeat from metaServer"
               << request.metaserverid()
               << ", but fine report token:" << request.token()
               << " do not consistent with topo record token:"
               << meta_server.GetToken();
    return HeartbeatStatusCode::hbMetaServerTokenNotMatch;
  }
  return HeartbeatStatusCode::hbOK;
}

bool HeartbeatManager::TransformHeartbeatCopySetInfoToTopologyOne(
    const ::curvefs::mds::heartbeat::CopySetInfo& info,
    ::curvefs::mds::topology::CopySetInfo* out) {
  ::curvefs::mds::topology::CopySetInfo topo_copyset_info(
      static_cast<PoolIdType>(info.poolid()), info.copysetid());
  // set epoch
  topo_copyset_info.SetEpoch(info.epoch());

  // set peers
  std::set<MetaServerIdType> peers;
  MetaServerIdType leader = UNINITIALIZE_ID;
  for (const auto& value : info.peers()) {
    MetaServerIdType res = GetMetaserverIdByPeerStr(value.address());
    if (UNINITIALIZE_ID == res) {
      LOG(ERROR) << "heartbeat manager can not get metaServerInfo"
                    " according to report ipPort: "
                 << value.address();
      return false;
    }

    if (value.address() == info.leaderpeer().address()) {
      leader = res;
    }
    peers.emplace(res);
  }
  topo_copyset_info.SetCopySetMembers(peers);

  if (leader == UNINITIALIZE_ID) {
    LOG(WARNING) << "leader not found, poolid: " << info.poolid()
                 << ", copysetid: " << info.copysetid();
  }

  // set leader
  topo_copyset_info.SetLeader(leader);

  // set info of configuration changes
  if (info.configchangeinfo().IsInitialized()) {
    MetaServerIdType res =
        GetMetaserverIdByPeerStr(info.configchangeinfo().peer().address());
    if (res == UNINITIALIZE_ID) {
      LOG(ERROR) << "heartbeat manager can not get metaInfo"
                    "according to report candidate ipPort: "
                 << info.configchangeinfo().peer().address();
      return false;
    }
    topo_copyset_info.SetCandidate(res);
  }

  *out = topo_copyset_info;
  return true;
}

MetaServerIdType HeartbeatManager::GetMetaserverIdByPeerStr(
    const std::string& peer) {
  // resolute peer string for ip, port and metaserverid
  std::string ip;
  uint32_t port, id;
  bool ok = curvefs::mds::topology::SplitPeerId(peer, &ip, &port, &id);
  if (!ok) {
    LOG(ERROR) << "report [" << peer << "] is not a valid ip:port:id form";
    return false;
  }

  // fetch metaserverId according to ip:port pair
  MetaServer meta_server;
  if (topology_->GetMetaServer(ip, port, &meta_server)) {
    return meta_server.GetId();
  }

  LOG(ERROR) << "heartbeatManager can not get metaServer ip: " << ip
             << ", port: " << port << " from topology";
  return UNINITIALIZE_ID;
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curvefs
