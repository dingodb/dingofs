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
 * @Project: dingo
 * @Date: 2021-11-8 11:01:48
 * @Author: chenwei
 */

#include "mds/schedule/topoAdapter.h"

#include <glog/logging.h>

#include <cfloat>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "dingofs/topology.pb.h"
#include "mds/common/mds_define.h"

namespace dingofs {
namespace mds {
namespace schedule {

using mds::topology::PoolIdType;
using mds::topology::TopoStatusCode;

PeerInfo::PeerInfo(MetaServerIdType id, ZoneIdType zoneId, ServerIdType sid,
                   const std::string& ip, uint32_t port) {
  this->id = id;
  this->zoneId = zoneId;
  this->serverId = sid;
  this->ip = ip;
  this->port = port;
}

CopySetConf::CopySetConf(const CopySetKey& key, EpochType epoch,
                         const std::vector<PeerInfo>& peers,
                         ConfigChangeType type, MetaServerIdType item,
                         MetaServerIdType oldOne) {
  this->id.first = key.first;
  this->id.second = key.second;
  this->epoch = epoch;
  this->peers = peers;
  this->type = type;
  this->configChangeItem = item;
  this->oldOne = oldOne;
}

bool CopySetInfo::ContainPeer(MetaServerIdType id) const {
  for (auto& peerId : peers) {
    if (id == peerId.id) {
      return true;
    }
  }
  return false;
}

bool CopySetInfo::HasCandidate() const {
  return candidatePeerInfo.id != UNINITIALIZE_ID;
}

std::string CopySetInfo::CopySetInfoStr() const {
  std::string res = "[copysetId:(" + std::to_string(id.first) + "," +
                    std::to_string(id.second) +
                    "), epoch:" + std::to_string(epoch) +
                    ", leader:" + std::to_string(leader) + ", peers:(";
  for (auto& peer : peers) {
    res += std::to_string(peer.id) + ",";
  }

  res += "), canidate:" + std::to_string(candidatePeerInfo.id) +
         ", has configChangeInfo:" +
         std::to_string(configChangeInfo.IsInitialized()) + "]";
  return res;
}

bool MetaServerInfo::IsOnline() const { return state == OnlineState::ONLINE; }

bool MetaServerInfo::IsOffline() const { return state == OnlineState::OFFLINE; }

bool MetaServerInfo::IsUnstable() const {
  return state == OnlineState::UNSTABLE;
}

bool MetaServerInfo::IsHealthy() const { return state == OnlineState::ONLINE; }

bool MetaServerInfo::IsResourceOverload() const {
  if (space.IsResourceOverload()) {
    LOG(ERROR) << "metaserver: " << info.serverId << ", ip: " << info.ip
               << ", port: " << info.port
               << " is resource overload, diskThresholdByte: "
               << space.GetDiskThreshold()
               << ", diskUsedByte: " << space.GetDiskUsed();
    return true;
  }

  return false;
}

double MetaServerInfo::GetResourceUseRatioPercent() const {
  return space.GetResourceUseRatioPercent();
}

bool MetaServerInfo::IsMetaserverResourceAvailable() const {
  if (!IsHealthy()) {
    LOG(ERROR) << "metaserver: " << info.serverId << ", ip: " << info.ip
               << ", port: " << info.port << " is offline";
    return false;
  }
  return space.IsMetaserverResourceAvailable();
}

TopoAdapterImpl::TopoAdapterImpl(std::shared_ptr<Topology> topo,
                                 std::shared_ptr<TopologyManager> manager) {
  this->topo_ = topo;
  this->topoManager_ = manager;
}

std::vector<PoolIdType> TopoAdapterImpl::Getpools() {
  return topo_->GetPoolInCluster();
}

bool TopoAdapterImpl::GetCopySetInfo(const CopySetKey& id, CopySetInfo* info) {
  mds::topology::CopySetInfo csInfo;
  // cannot get copyset info
  if (!topo_->GetCopySet(id, &csInfo)) {
    return false;
  }

  if (!CopySetFromTopoToSchedule(csInfo, info)) {
    return false;
  }

  return true;
}

std::vector<CopySetInfo> TopoAdapterImpl::GetCopySetInfos() {
  std::vector<CopySetInfo> infos;
  for (auto& copySetKey : topo_->GetCopySetsInCluster()) {
    CopySetInfo copySetInfo;
    if (GetCopySetInfo(copySetKey, &copySetInfo)) {
      infos.push_back(std::move(copySetInfo));
    }
  }
  return infos;
}

std::vector<CopySetInfo> TopoAdapterImpl::GetCopySetInfosInMetaServer(
    MetaServerIdType id) {
  std::vector<CopySetKey> keys = topo_->GetCopySetsInMetaServer(id);

  std::vector<CopySetInfo> out;
  for (auto& key : keys) {
    CopySetInfo info;
    if (GetCopySetInfo(key, &info)) {
      out.emplace_back(std::move(info));
    }
  }
  return out;
}

std::vector<CopySetInfo> TopoAdapterImpl::GetCopySetInfosInPool(PoolIdType id) {
  std::vector<dingofs::mds::topology::CopySetInfo> copysetsInTopo =
      topo_->GetCopySetInfosInPool(id);

  std::vector<CopySetInfo> out;
  for (auto& csInfo : copysetsInTopo) {
    CopySetInfo info;
    if (CopySetFromTopoToSchedule(csInfo, &info)) {
      out.emplace_back(std::move(info));
    }
  }
  return out;
}

bool TopoAdapterImpl::GetMetaServerInfo(MetaServerIdType id,
                                        MetaServerInfo* out) {
  assert(out != nullptr);

  mds::topology::MetaServer ms;
  if (!topo_->GetMetaServer(id, &ms)) {
    LOG(ERROR) << "can not get metaServer:" << id << " from topology";
    return false;
  }
  return MetaServerFromTopoToSchedule(ms, out);
}

std::vector<MetaServerInfo> TopoAdapterImpl::GetMetaServerInfos() {
  std::vector<MetaServerInfo> infos;
  for (auto metaServerId : topo_->GetMetaServerInCluster()) {
    MetaServerInfo info;
    if (GetMetaServerInfo(metaServerId, &info)) {
      infos.push_back(std::move(info));
    }
  }

  return infos;
}

std::vector<MetaServerInfo> TopoAdapterImpl::GetMetaServersInPool(
    PoolIdType poolId) {
  std::vector<MetaServerInfo> infos;
  auto ids = topo_->GetMetaServerInPool(poolId);
  for (auto id : ids) {
    MetaServerInfo out;
    if (GetMetaServerInfo(id, &out)) {
      infos.emplace_back(std::move(out));
    }
  }
  return infos;
}

std::vector<MetaServerInfo> TopoAdapterImpl::GetMetaServersInZone(
    ZoneIdType zoneId) {
  std::vector<MetaServerInfo> infos;
  auto ids = topo_->GetMetaServerInZone(zoneId);
  for (auto id : ids) {
    MetaServerInfo out;
    if (GetMetaServerInfo(id, &out)) {
      infos.emplace_back(std::move(out));
    }
  }
  return infos;
}

std::list<ZoneIdType> TopoAdapterImpl::GetZoneInPool(PoolIdType poolId) {
  return topo_->GetZoneInPool(poolId);
}

uint16_t TopoAdapterImpl::GetStandardZoneNumInPool(PoolIdType id) {
  mds::topology::Pool pool;
  if (topo_->GetPool(id, &pool)) {
    return pool.GetRedundanceAndPlaceMentPolicy().zoneNum;
  }
  return 0;
}

uint16_t TopoAdapterImpl::GetStandardReplicaNumInPool(PoolIdType id) {
  mds::topology::Pool pool;
  if (topo_->GetPool(id, &pool)) {
    return pool.GetReplicaNum();
  }
  return 0;
}

bool TopoAdapterImpl::GetPeerInfo(MetaServerIdType id, PeerInfo* peerInfo) {
  mds::topology::MetaServer ms;
  mds::topology::Server server;

  bool canGetMetaServer, canGetServer;
  if ((canGetMetaServer = topo_->GetMetaServer(id, &ms)) &&
      (canGetServer = topo_->GetServer(ms.GetServerId(), &server))) {
    *peerInfo = PeerInfo(ms.GetId(), server.GetZoneId(), server.GetId(),
                         ms.GetInternalIp(), ms.GetInternalPort());
  } else {
    LOG(ERROR) << "topoAdapter can not find metaServer(" << id
               << ", res:" << canGetMetaServer
               << ") or Server(res:" << canGetServer << ")";
    return false;
  }
  return true;
}

bool TopoAdapterImpl::CopySetFromTopoToSchedule(
    const mds::topology::CopySetInfo& origin, mds::schedule::CopySetInfo* out) {
  assert(out != nullptr);

  out->id.first = origin.GetPoolId();
  out->id.second = origin.GetId();
  out->epoch = origin.GetEpoch();
  out->leader = origin.GetLeader();

  for (auto id : origin.GetCopySetMembers()) {
    PeerInfo peerInfo;
    if (GetPeerInfo(id, &peerInfo)) {
      out->peers.emplace_back(std::move(peerInfo));
    } else {
      return false;
    }
  }

  if (origin.HasCandidate()) {
    PeerInfo peerInfo;
    if (GetPeerInfo(origin.GetCandidate(), &peerInfo)) {
      out->candidatePeerInfo = peerInfo;
    } else {
      return false;
    }
  }
  return true;
}

bool TopoAdapterImpl::MetaServerFromTopoToSchedule(
    const mds::topology::MetaServer& origin,
    mds::schedule::MetaServerInfo* out) {
  assert(out != nullptr);

  mds::topology::Server server;
  if (topo_->GetServer(origin.GetServerId(), &server)) {
    out->info = PeerInfo{origin.GetId(), server.GetZoneId(), server.GetId(),
                         origin.GetInternalIp(), origin.GetInternalPort()};
  } else {
    LOG(ERROR) << "can not get server:" << origin.GetServerId()
               << ", ip:" << origin.GetInternalIp()
               << ", port:" << origin.GetInternalPort() << " from topology";

    return false;
  }

  out->startUpTime = origin.GetStartUpTime();
  out->state = origin.GetOnlineState();
  out->space = origin.GetMetaServerSpace();
  out->leaderNum = topo_->GetLeaderNumInMetaserver(origin.GetId());
  out->copysetNum = topo_->GetCopysetNumInMetaserver(origin.GetId());
  return true;
}

bool TopoAdapterImpl::CreateCopySetAtMetaServer(CopySetKey id,
                                                MetaServerIdType msId) {
  return topoManager_->CreateCopysetNodeOnMetaServer(id.first, id.second, msId);
}

bool TopoAdapterImpl::ChooseNewMetaServerForCopyset(
    PoolIdType poolId, const std::set<ZoneIdType>& excludeZones,
    const std::set<MetaServerIdType>& excludeMetaservers,
    MetaServerIdType* target) {
  TopoStatusCode ret = topo_->ChooseNewMetaServerForCopyset(
      poolId, excludeZones, excludeMetaservers, target);
  return ret == TopoStatusCode::TOPO_OK;
}

}  // namespace schedule
}  // namespace mds
}  // namespace dingofs
