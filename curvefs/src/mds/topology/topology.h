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
 * Created Date: 2021-08-24
 * Author: wanghai01
 */
#ifndef CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_H_

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/topology/topology_id_generator.h"
#include "curvefs/src/mds/topology/topology_item.h"
#include "curvefs/src/mds/topology/topology_storge.h"
#include "curvefs/src/mds/topology/topology_token_generator.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/interruptible_sleeper.h"

namespace curvefs {
namespace mds {
namespace topology {

using RWLock = ::curve::common::BthreadRWLock;
using ::curve::common::InterruptibleSleeper;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

using PartitionFilter = std::function<bool(const Partition&)>;
using CopySetFilter = std::function<bool(const CopySetInfo&)>;
using MetaServerFilter = std::function<bool(const MetaServer&)>;
using ServerFilter = std::function<bool(const Server&)>;
using ZoneFilter = std::function<bool(const Zone&)>;
using PoolFilter = std::function<bool(const Pool&)>;

class CopysetCreateInfo {
 public:
  CopysetCreateInfo() = default;
  CopysetCreateInfo(uint32_t poolid, uint32_t copysetid,
                    std::set<MetaServerIdType> ids)
      : poolId(poolid), copysetId(copysetid), metaServerIds(ids) {}

  std::string ToString() const {
    std::string str;
    str = "poolId = " + std::to_string(poolId) +
          ", copysetId = " + std::to_string(copysetId) +
          ", metaserver list = [";
    for (auto it : metaServerIds) {
      str = str + std::to_string(it) + ", ";
    }
    str = str + "]";
    return str;
  }

  uint32_t poolId;
  uint32_t copysetId;
  std::set<MetaServerIdType> metaServerIds;
};

class Topology {
 public:
  Topology() = default;
  virtual ~Topology() = default;

  virtual bool GetClusterInfo(ClusterInformation* info) = 0;

  virtual PoolIdType AllocatePoolId() = 0;
  virtual ZoneIdType AllocateZoneId() = 0;
  virtual ServerIdType AllocateServerId() = 0;
  virtual MetaServerIdType AllocateMetaServerId() = 0;
  virtual CopySetIdType AllocateCopySetId(PoolIdType pool_id) = 0;
  virtual PartitionIdType AllocatePartitionId() = 0;
  virtual MemcacheClusterIdType AllocateMemCacheClusterId() = 0;

  virtual std::string AllocateToken() = 0;

  virtual TopoStatusCode AddPool(const Pool& data) = 0;
  virtual TopoStatusCode AddZone(const Zone& data) = 0;
  virtual TopoStatusCode AddServer(const Server& data) = 0;
  virtual TopoStatusCode AddMetaServer(const MetaServer& data) = 0;
  virtual TopoStatusCode AddCopySet(const CopySetInfo& data) = 0;
  virtual TopoStatusCode AddCopySetCreating(const CopySetKey& key) = 0;
  virtual TopoStatusCode AddPartition(const Partition& data) = 0;
  virtual TopoStatusCode AddMemcacheCluster(const MemcacheCluster& data) = 0;
  virtual TopoStatusCode AddMemcacheCluster(MemcacheCluster&& data) = 0;

  virtual TopoStatusCode RemovePool(PoolIdType id) = 0;
  virtual TopoStatusCode RemoveZone(ZoneIdType id) = 0;
  virtual TopoStatusCode RemoveServer(ServerIdType id) = 0;
  virtual TopoStatusCode RemoveMetaServer(MetaServerIdType id) = 0;
  virtual TopoStatusCode RemoveCopySet(CopySetKey key) = 0;
  virtual void RemoveCopySetCreating(CopySetKey key) = 0;
  virtual TopoStatusCode RemovePartition(PartitionIdType id) = 0;

  virtual TopoStatusCode UpdatePool(const Pool& data) = 0;
  virtual TopoStatusCode UpdateZone(const Zone& data) = 0;
  virtual TopoStatusCode UpdateServer(const Server& data) = 0;
  virtual TopoStatusCode UpdateMetaServerOnlineState(
      const OnlineState& online_state, MetaServerIdType id) = 0;
  virtual TopoStatusCode UpdateMetaServerSpace(const MetaServerSpace& space,
                                               MetaServerIdType id) = 0;
  virtual TopoStatusCode UpdateMetaServerStartUpTime(uint64_t time,
                                                     MetaServerIdType id) = 0;
  virtual TopoStatusCode UpdateCopySetTopo(const CopySetInfo& data) = 0;
  virtual TopoStatusCode UpdatePartition(const Partition& data) = 0;
  virtual TopoStatusCode UpdatePartitionStatistic(
      uint32_t partition_id, PartitionStatistic statistic) = 0;
  virtual TopoStatusCode UpdatePartitionTxIds(
      std::vector<PartitionTxId> tx_ids) = 0;
  virtual TopoStatusCode UpdatePartitionStatus(PartitionIdType partition_id,
                                               PartitionStatus status) = 0;

  virtual TopoStatusCode SetCopySetAvalFlag(const CopySetKey& key,
                                            bool aval) = 0;

  virtual PoolIdType FindPool(const std::string& pool_name) const = 0;
  virtual ZoneIdType FindZone(const std::string& zone_name,
                              const std::string& pool_name) const = 0;
  virtual ZoneIdType FindZone(const std::string& zone_name,
                              PoolIdType poolid) const = 0;
  virtual ServerIdType FindServerByHostName(
      const std::string& host_name) const = 0;
  virtual ServerIdType FindServerByHostIpPort(const std::string& host_ip,
                                              uint32_t port) const = 0;
  virtual bool GetPool(PoolIdType pool_id, Pool* out) const = 0;
  virtual bool GetZone(ZoneIdType zone_id, Zone* out) const = 0;
  virtual bool GetServer(ServerIdType server_id, Server* out) const = 0;
  virtual bool GetMetaServer(MetaServerIdType metaserver_id,
                             MetaServer* out) const = 0;
  virtual bool GetMetaServer(const std::string& host_ip, uint32_t port,
                             MetaServer* out) const = 0;
  virtual bool GetCopySet(CopySetKey key, CopySetInfo* out) const = 0;
  virtual bool GetCopysetOfPartition(PartitionIdType id,
                                     CopySetInfo* out) const = 0;
  virtual uint32_t GetCopysetNumInMetaserver(MetaServerIdType id) const = 0;
  virtual uint32_t GetLeaderNumInMetaserver(MetaServerIdType id) const = 0;
  virtual bool GetAvailableCopyset(CopySetInfo* out) const = 0;
  virtual int GetAvailableCopysetNum() const = 0;
  virtual std::list<CopySetKey> GetAvailableCopysetKeyList() const = 0;
  virtual std::vector<CopySetInfo> GetAvailableCopysetList() const = 0;
  virtual bool GetPartition(PartitionIdType partition_id, Partition* out) = 0;

  virtual bool GetPool(const std::string& pool_name, Pool* out) const = 0;
  virtual bool GetZone(const std::string& zone_name,
                       const std::string& pool_name, Zone* out) const = 0;
  virtual bool GetZone(const std::string& zone_name, PoolIdType pool_id,
                       Zone* out) const = 0;
  virtual bool GetServerByHostName(const std::string& host_name,
                                   Server* out) const = 0;
  virtual bool GetServerByHostIpPort(const std::string& host_ip, uint32_t port,
                                     Server* out) const = 0;

  virtual std::vector<MetaServerIdType> GetMetaServerInCluster(
      MetaServerFilter filter = [](const MetaServer&) {
        return true;
      }) const = 0;
  virtual std::vector<ServerIdType> GetServerInCluster(
      ServerFilter filter = [](const Server&) { return true; }) const = 0;
  virtual std::vector<ZoneIdType> GetZoneInCluster(
      ZoneFilter filter = [](const Zone&) { return true; }) const = 0;
  virtual std::vector<PoolIdType> GetPoolInCluster(
      PoolFilter filter = [](const Pool&) { return true; }) const = 0;

  // get metaserver list
  virtual std::list<MetaServerIdType> GetMetaServerInServer(
      ServerIdType id, MetaServerFilter filter = [](const MetaServer&) {
        return true;
      }) const = 0;
  virtual std::list<MetaServerIdType> GetMetaServerInZone(
      ZoneIdType id, MetaServerFilter filter = [](const MetaServer&) {
        return true;
      }) const = 0;
  virtual std::list<MetaServerIdType> GetMetaServerInPool(
      PoolIdType id, MetaServerFilter filter = [](const MetaServer&) {
        return true;
      }) const = 0;
  virtual uint32_t GetMetaServerNumInPool(
      PoolIdType id, MetaServerFilter filter = [](const MetaServer&) {
        return true;
      }) const = 0;

  virtual void GetAvailableMetaserversUnlock(
      std::vector<const MetaServer*>* vec) = 0;

  // get server list
  virtual std::list<ServerIdType> GetServerInZone(
      ZoneIdType id,
      ServerFilter filter = [](const Server&) { return true; }) const = 0;

  // get zone list
  virtual std::list<ZoneIdType> GetZoneInPool(
      PoolIdType id,
      ZoneFilter filter = [](const Zone&) { return true; }) const = 0;

  // get copyset list
  virtual std::vector<CopySetIdType> GetCopySetsInPool(
      PoolIdType pool_id,
      CopySetFilter filter = [](const CopySetInfo&) { return true; }) const = 0;

  virtual uint32_t GetCopySetNumInPool(
      PoolIdType pool_id,
      CopySetFilter filter = [](const CopySetInfo&) { return true; }) const = 0;

  virtual std::vector<CopySetInfo> GetCopySetInfosInPool(
      PoolIdType pool_id,
      CopySetFilter filter = [](const CopySetInfo&) { return true; }) const = 0;

  virtual std::vector<CopySetKey> GetCopySetsInCluster(
      CopySetFilter filter = [](const CopySetInfo&) { return true; }) const = 0;

  virtual std::vector<CopySetKey> GetCopySetsInMetaServer(
      MetaServerIdType id,
      CopySetFilter filter = [](const CopySetInfo&) { return true; }) const = 0;

  // get partition list
  virtual std::list<Partition> GetPartitionOfFs(
      FsIdType id,
      PartitionFilter filter = [](const Partition&) { return true; }) const = 0;

  virtual std::list<Partition> GetPartitionInfosInPool(
      PoolIdType pool_id,
      PartitionFilter filter = [](const Partition&) { return true; }) const = 0;

  virtual TopoStatusCode ChooseNewMetaServerForCopyset(
      PoolIdType pool_id, const std::set<ZoneIdType>& unavailable_zones,
      const std::set<MetaServerIdType>& unavailable_ms,
      MetaServerIdType* target) = 0;

  virtual std::list<Partition> GetPartitionInfosInCopyset(
      CopySetIdType copyset_id) const = 0;

  virtual TopoStatusCode GenCopysetAddrBatch(
      uint32_t need_create_num, std::list<CopysetCreateInfo>* copyset_list) = 0;

  virtual void GenCopysetIfPoolEmptyUnlocked(
      std::list<CopysetCreateInfo>* copyset_list) = 0;

  virtual TopoStatusCode GenSubsequentCopysetAddrBatchUnlocked(
      uint32_t need_create_num, std::list<CopysetCreateInfo>* copyset_list) = 0;

  virtual TopoStatusCode GenCopysetAddrBatchForPool(
      PoolIdType pool_id, uint16_t replica_num,
      std::list<CopysetCreateInfo>* copyset_list) = 0;

  virtual uint32_t GetPartitionIndexOfFS(FsIdType fs_id) = 0;

  virtual std::vector<CopySetInfo> ListCopysetInfo() const = 0;

  virtual void GetMetaServersSpace(
      ::google::protobuf::RepeatedPtrField<
          curvefs::mds::topology::MetadataUsage>* spaces) = 0;

  virtual std::string GetHostNameAndPortById(MetaServerIdType ms_id) = 0;

  virtual bool IsCopysetCreating(const CopySetKey& key) const = 0;

  virtual std::list<MemcacheServer> ListMemcacheServers() const = 0;
  virtual std::list<MemcacheCluster> ListMemcacheClusters() const = 0;
  virtual TopoStatusCode AllocOrGetMemcacheCluster(
      FsIdType fs_id, MemcacheClusterInfo* cluster) = 0;
};

class TopologyImpl : public Topology {
 public:
  TopologyImpl(std::shared_ptr<TopologyIdGenerator> id_generator,
               std::shared_ptr<TopologyTokenGenerator> token_generator,
               std::shared_ptr<TopologyStorage> storage)
      : idGenerator_(id_generator),
        tokenGenerator_(token_generator),
        storage_(storage),
        isStop_(true) {}

  ~TopologyImpl() override { Stop(); }

  TopoStatusCode Init(const TopologyOption& option);

  int Run();
  int Stop();

  bool GetClusterInfo(ClusterInformation* info) override;

  PoolIdType AllocatePoolId() override;
  ZoneIdType AllocateZoneId() override;
  ServerIdType AllocateServerId() override;
  MetaServerIdType AllocateMetaServerId() override;
  CopySetIdType AllocateCopySetId(PoolIdType pool_id) override;
  PartitionIdType AllocatePartitionId() override;
  MemcacheClusterIdType AllocateMemCacheClusterId() override;

  std::string AllocateToken() override;

  TopoStatusCode AddPool(const Pool& data) override;
  TopoStatusCode AddZone(const Zone& data) override;
  TopoStatusCode AddServer(const Server& data) override;
  TopoStatusCode AddMetaServer(const MetaServer& data) override;
  TopoStatusCode AddCopySet(const CopySetInfo& data) override;
  TopoStatusCode AddCopySetCreating(const CopySetKey& key) override;
  TopoStatusCode AddPartition(const Partition& data) override;
  TopoStatusCode AddMemcacheCluster(const MemcacheCluster& data) override;
  TopoStatusCode AddMemcacheCluster(MemcacheCluster&& data) override;

  TopoStatusCode RemovePool(PoolIdType id) override;
  TopoStatusCode RemoveZone(ZoneIdType id) override;
  TopoStatusCode RemoveServer(ServerIdType id) override;
  TopoStatusCode RemoveMetaServer(MetaServerIdType id) override;
  TopoStatusCode RemoveCopySet(CopySetKey key) override;
  void RemoveCopySetCreating(CopySetKey key) override;
  TopoStatusCode RemovePartition(PartitionIdType id) override;

  TopoStatusCode UpdatePool(const Pool& data) override;
  TopoStatusCode UpdateZone(const Zone& data) override;
  TopoStatusCode UpdateServer(const Server& data) override;
  TopoStatusCode UpdateMetaServerOnlineState(const OnlineState& online_state,
                                             MetaServerIdType id) override;
  TopoStatusCode UpdateMetaServerSpace(const MetaServerSpace& space,
                                       MetaServerIdType id) override;
  TopoStatusCode UpdateMetaServerStartUpTime(uint64_t time,
                                             MetaServerIdType id) override;
  TopoStatusCode UpdateCopySetTopo(const CopySetInfo& data) override;
  TopoStatusCode SetCopySetAvalFlag(const CopySetKey& key, bool aval) override;
  TopoStatusCode UpdatePartition(const Partition& data) override;
  TopoStatusCode UpdatePartitionStatistic(
      uint32_t partition_id, PartitionStatistic statistic) override;
  TopoStatusCode UpdatePartitionTxIds(
      std::vector<PartitionTxId> tx_ids) override;
  TopoStatusCode UpdatePartitionStatus(PartitionIdType partition_id,
                                       PartitionStatus status) override;

  PoolIdType FindPool(const std::string& pool_name) const override;
  ZoneIdType FindZone(const std::string& zone_name,
                      const std::string& pool_name) const override;
  ZoneIdType FindZone(const std::string& zone_name,
                      PoolIdType pool_id) const override;
  ServerIdType FindServerByHostName(
      const std::string& host_name) const override;
  ServerIdType FindServerByHostIpPort(const std::string& host_ip,
                                      uint32_t port) const override;
  bool GetPool(PoolIdType pool_id, Pool* out) const override;
  bool GetZone(ZoneIdType zone_id, Zone* out) const override;
  bool GetServer(ServerIdType server_id, Server* out) const override;
  bool GetMetaServer(MetaServerIdType metaserver_id,
                     MetaServer* out) const override;
  bool GetMetaServer(const std::string& host_ip, uint32_t port,
                     MetaServer* out) const override;
  bool GetCopySet(CopySetKey key, CopySetInfo* out) const override;
  bool GetCopysetOfPartition(PartitionIdType id,
                             CopySetInfo* out) const override;
  uint32_t GetCopysetNumInMetaserver(MetaServerIdType id) const override;
  uint32_t GetLeaderNumInMetaserver(MetaServerIdType id) const override;
  bool GetAvailableCopyset(CopySetInfo* out) const override;
  int GetAvailableCopysetNum() const override;

  std::list<CopySetKey> GetAvailableCopysetKeyList() const override;
  std::vector<CopySetInfo> GetAvailableCopysetList() const override;

  bool GetPartition(PartitionIdType partition_id, Partition* out) override;

  bool GetPool(const std::string& pool_name, Pool* out) const override {
    return GetPool(FindPool(pool_name), out);
  }
  bool GetZone(const std::string& zone_name, const std::string& pool_name,
               Zone* out) const override {
    return GetZone(FindZone(zone_name, pool_name), out);
  }
  bool GetZone(const std::string& zone_name, PoolIdType pool_id,
               Zone* out) const override {
    return GetZone(FindZone(zone_name, pool_id), out);
  }
  bool GetServerByHostName(const std::string& host_name,
                           Server* out) const override {
    return GetServer(FindServerByHostName(host_name), out);
  }
  bool GetServerByHostIpPort(const std::string& host_ip, uint32_t port,
                             Server* out) const override {
    return GetServer(FindServerByHostIpPort(host_ip, port), out);
  }

  std::vector<MetaServerIdType> GetMetaServerInCluster(
      MetaServerFilter filter = [](const MetaServer&) {
        return true;
      }) const override;

  std::vector<ServerIdType> GetServerInCluster(
      ServerFilter filter = [](const Server&) { return true; }) const override;

  std::vector<ZoneIdType> GetZoneInCluster(ZoneFilter filter = [](const Zone&) {
    return true;
  }) const override;

  std::vector<PoolIdType> GetPoolInCluster(PoolFilter filter = [](const Pool&) {
    return true;
  }) const override;

  // get metasever list
  std::list<MetaServerIdType> GetMetaServerInServer(
      ServerIdType id, MetaServerFilter filter = [](const MetaServer&) {
        return true;
      }) const override;
  std::list<MetaServerIdType> GetMetaServerInZone(
      ZoneIdType id, MetaServerFilter filter = [](const MetaServer&) {
        return true;
      }) const override;
  std::list<MetaServerIdType> GetMetaServerInPool(
      PoolIdType id, MetaServerFilter filter = [](const MetaServer&) {
        return true;
      }) const override;
  uint32_t GetMetaServerNumInPool(
      PoolIdType id, MetaServerFilter filter = [](const MetaServer&) {
        return true;
      }) const override;
  void GetAvailableMetaserversUnlock(
      std::vector<const MetaServer*>* vec) override;

  // get server list
  std::list<ServerIdType> GetServerInZone(
      ZoneIdType id,
      ServerFilter filter = [](const Server&) { return true; }) const override;

  // get zone list
  std::list<ZoneIdType> GetZoneInPool(
      PoolIdType id,
      ZoneFilter filter = [](const Zone&) { return true; }) const override;

  // get copyset list
  std::vector<CopySetKey> GetCopySetsInCluster(CopySetFilter filter =
                                                   [](const CopySetInfo&) {
                                                     return true;
                                                   }) const override;

  std::vector<CopySetIdType> GetCopySetsInPool(
      PoolIdType pool_id, CopySetFilter filter = [](const CopySetInfo&) {
        return true;
      }) const override;

  uint32_t GetCopySetNumInPool(
      PoolIdType pool_id, CopySetFilter filter = [](const CopySetInfo&) {
        return true;
      }) const override;

  std::vector<CopySetInfo> GetCopySetInfosInPool(
      PoolIdType pool_id, CopySetFilter filter = [](const CopySetInfo&) {
        return true;
      }) const override;

  std::vector<CopySetKey> GetCopySetsInMetaServer(
      MetaServerIdType id, CopySetFilter filter = [](const CopySetInfo&) {
        return true;
      }) const override;

  // get partition list
  std::list<Partition> GetPartitionOfFs(
      FsIdType id, PartitionFilter filter = [](const Partition&) {
        return true;
      }) const override;

  std::list<Partition> GetPartitionInfosInPool(
      PoolIdType pool_id, PartitionFilter filter = [](const Partition&) {
        return true;
      }) const override;
  std::list<Partition> GetPartitionInfosInCopyset(
      CopySetIdType copyset_id) const override;

  TopoStatusCode ChooseNewMetaServerForCopyset(
      PoolIdType pool_id, const std::set<ZoneIdType>& unavailable_zones,
      const std::set<MetaServerIdType>& unavailable_ms,
      MetaServerIdType* target) override;

  TopoStatusCode GenCopysetAddrBatch(
      uint32_t need_create_num,
      std::list<CopysetCreateInfo>* copyset_list) override;

  void GenCopysetIfPoolEmptyUnlocked(
      std::list<CopysetCreateInfo>* copyset_list) override;

  TopoStatusCode GenSubsequentCopysetAddrBatchUnlocked(
      uint32_t need_create_num,
      std::list<CopysetCreateInfo>* copyset_list) override;

  TopoStatusCode GenCopysetAddrBatchForPool(
      PoolIdType pool_id, uint16_t replica_num,
      std::list<CopysetCreateInfo>* copyset_list) override;

  uint32_t GetPartitionIndexOfFS(FsIdType fs_id) override;

  TopoStatusCode GetPoolIdByMetaserverId(MetaServerIdType id,
                                         PoolIdType* pool_id_out);

  TopoStatusCode GetPoolIdByServerId(ServerIdType id, PoolIdType* pool_id_out);

  std::vector<CopySetInfo> ListCopysetInfo() const override;

  void GetMetaServersSpace(
      ::google::protobuf::RepeatedPtrField<
          curvefs::mds::topology::MetadataUsage>* spaces) override;

  std::string GetHostNameAndPortById(MetaServerIdType ms_id) override;

  bool IsCopysetCreating(const CopySetKey& key) const override;

  std::list<MemcacheServer> ListMemcacheServers() const override;
  std::list<MemcacheCluster> ListMemcacheClusters() const override;
  TopoStatusCode AllocOrGetMemcacheCluster(
      FsIdType fs_id, MemcacheClusterInfo* cluster) override;

 private:
  TopoStatusCode LoadClusterInfo();

  void BackEndFunc();

  void FlushCopySetToStorage();

  void FlushMetaServerToStorage();

  int GetOneRandomNumber(int start, int end) const;

  TopoStatusCode GenCandidateMapUnlock(
      PoolIdType pool_id,
      std::map<ZoneIdType, std::vector<MetaServerIdType>>* candidate_map);

  bool RefreshPartitionIndexOfFS(
      const std::unordered_map<PartitionIdType, Partition>& partition_map);

  std::unordered_map<PoolIdType, Pool> poolMap_;
  std::unordered_map<ZoneIdType, Zone> zoneMap_;
  std::unordered_map<ServerIdType, Server> serverMap_;
  std::unordered_map<MetaServerIdType, MetaServer> metaServerMap_;
  std::map<CopySetKey, CopySetInfo> copySetMap_;
  std::unordered_map<PartitionIdType, Partition> partitionMap_;
  std::set<CopySetKey> copySetCreating_;
  std::unordered_map<MemcacheClusterIdType, MemcacheCluster>
      memcacheClusterMap_;
  std::unordered_map<FsIdType, MemcacheClusterIdType> fs2MemcacheCluster_;

  // cluster info
  ClusterInformation clusterInfo_;
  mutable RWLock clusterMutex_;

  std::shared_ptr<TopologyIdGenerator> idGenerator_;
  std::shared_ptr<TopologyTokenGenerator> tokenGenerator_;
  std::shared_ptr<TopologyStorage> storage_;

  // fetch lock in the order below to avoid deadlock
  mutable RWLock poolMutex_;
  mutable RWLock zoneMutex_;
  mutable RWLock serverMutex_;
  mutable RWLock metaServerMutex_;
  mutable RWLock copySetMutex_;
  mutable RWLock partitionMutex_;
  mutable RWLock copySetCreatingMutex_;
  mutable RWLock memcacheClusterMutex_;
  mutable RWLock fs2MemcacheClusterMutex_;

  TopologyOption option_;
  curve::common::Thread backEndThread_;
  curve::common::Atomic<bool> isStop_;
  InterruptibleSleeper sleeper_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_H_
