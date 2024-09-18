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
 * Created Date: 2021-08-25
 * Author: wanghai01
 */

#include "curvefs/src/mds/topology/topology.h"

#include <glog/logging.h>

#include <chrono>
#include <cstdint>
#include <random>
#include <utility>

#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/topology/topology_item.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/uuid.h"

namespace curvefs {
namespace mds {
namespace topology {

using ::curve::common::UUIDGenerator;

PoolIdType TopologyImpl::AllocatePoolId() { return idGenerator_->GenPoolId(); }

ZoneIdType TopologyImpl::AllocateZoneId() { return idGenerator_->GenZoneId(); }

ServerIdType TopologyImpl::AllocateServerId() {
  return idGenerator_->GenServerId();
}

MetaServerIdType TopologyImpl::AllocateMetaServerId() {
  return idGenerator_->GenMetaServerId();
}

CopySetIdType TopologyImpl::AllocateCopySetId(PoolIdType pool_id) {
  return idGenerator_->GenCopySetId(pool_id);
}

PartitionIdType TopologyImpl::AllocatePartitionId() {
  return idGenerator_->GenPartitionId();
}

std::string TopologyImpl::AllocateToken() {
  return tokenGenerator_->GenToken();
}

MemcacheClusterIdType TopologyImpl::AllocateMemCacheClusterId() {
  return idGenerator_->GenMemCacheClusterId();
}

TopoStatusCode TopologyImpl::AddPool(const Pool& data) {
  WriteLockGuard wlock_pool(poolMutex_);
  if (poolMap_.find(data.GetId()) == poolMap_.end()) {
    if (!storage_->StoragePool(data)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    poolMap_[data.GetId()] = data;
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_ID_DUPLICATED;
  }
}

TopoStatusCode TopologyImpl::AddZone(const Zone& data) {
  ReadLockGuard rlock_pool(poolMutex_);
  WriteLockGuard wlock_zone(zoneMutex_);
  auto it = poolMap_.find(data.GetPoolId());
  if (it != poolMap_.end()) {
    if (zoneMap_.find(data.GetId()) == zoneMap_.end()) {
      if (!storage_->StorageZone(data)) {
        return TopoStatusCode::TOPO_STORGE_FAIL;
      }
      it->second.AddZone(data.GetId());
      zoneMap_[data.GetId()] = data;
      return TopoStatusCode::TOPO_OK;
    } else {
      return TopoStatusCode::TOPO_ID_DUPLICATED;
    }
  } else {
    return TopoStatusCode::TOPO_POOL_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::AddServer(const Server& data) {
  ReadLockGuard rlock_zone(zoneMutex_);
  WriteLockGuard wlock_server(serverMutex_);
  auto it = zoneMap_.find(data.GetZoneId());
  if (it != zoneMap_.end()) {
    if (serverMap_.find(data.GetId()) == serverMap_.end()) {
      if (!storage_->StorageServer(data)) {
        return TopoStatusCode::TOPO_STORGE_FAIL;
      }
      it->second.AddServer(data.GetId());
      serverMap_[data.GetId()] = data;
      return TopoStatusCode::TOPO_OK;
    } else {
      return TopoStatusCode::TOPO_ID_DUPLICATED;
    }
  } else {
    return TopoStatusCode::TOPO_ZONE_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::AddMetaServer(const MetaServer& data) {
  // find the pool that the meatserver belongs to
  PoolIdType pool_id = UNINITIALIZE_ID;
  TopoStatusCode ret = GetPoolIdByServerId(data.GetServerId(), &pool_id);
  if (ret != TopoStatusCode::TOPO_OK) {
    return ret;
  }

  // fetch lock on pool, server and metaserver
  WriteLockGuard wlock_pool(poolMutex_);
  uint64_t metaserver_threshold = 0;
  {
    ReadLockGuard rlock_server(serverMutex_);
    WriteLockGuard wlock_meta_server(metaServerMutex_);
    auto it = serverMap_.find(data.GetServerId());
    if (it != serverMap_.end()) {
      if (metaServerMap_.find(data.GetId()) == metaServerMap_.end()) {
        if (!storage_->StorageMetaServer(data)) {
          return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        it->second.AddMetaServer(data.GetId());
        metaServerMap_[data.GetId()] = data;
        metaserver_threshold = data.GetMetaServerSpace().GetDiskThreshold();
      } else {
        return TopoStatusCode::TOPO_ID_DUPLICATED;
      }
    } else {
      return TopoStatusCode::TOPO_SERVER_NOT_FOUND;
    }
  }

  // update pool
  auto it = poolMap_.find(pool_id);
  if (it != poolMap_.end()) {
    uint64_t total_threshold = it->second.GetDiskThreshold();
    total_threshold += metaserver_threshold;
    it->second.SetDiskThreshold(total_threshold);
  } else {
    return TopoStatusCode::TOPO_POOL_NOT_FOUND;
  }

  return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyImpl::RemovePool(PoolIdType id) {
  WriteLockGuard wlock_pool(poolMutex_);
  auto it = poolMap_.find(id);
  if (it != poolMap_.end()) {
    if (it->second.GetZoneList().size() != 0) {
      return TopoStatusCode::TOPO_CANNOT_REMOVE_WHEN_NOT_EMPTY;
    }
    // TODO(wanghai): remove copysets and partition of this pool
    if (!storage_->DeletePool(id)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    poolMap_.erase(it);
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_POOL_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::RemoveZone(ZoneIdType id) {
  WriteLockGuard wlock_pool(poolMutex_);
  WriteLockGuard wlock_zone(zoneMutex_);
  auto it = zoneMap_.find(id);
  if (it != zoneMap_.end()) {
    if (it->second.GetServerList().size() != 0) {
      return TopoStatusCode::TOPO_CANNOT_REMOVE_WHEN_NOT_EMPTY;
    }
    if (!storage_->DeleteZone(id)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    auto ix = poolMap_.find(it->second.GetPoolId());
    if (ix != poolMap_.end()) {
      ix->second.RemoveZone(id);
    }
    zoneMap_.erase(it);
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_ZONE_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::RemoveServer(ServerIdType id) {
  WriteLockGuard wlock_zone(zoneMutex_);
  WriteLockGuard wlock_server(serverMutex_);
  auto it = serverMap_.find(id);
  if (it != serverMap_.end()) {
    if (it->second.GetMetaServerList().size() != 0) {
      return TopoStatusCode::TOPO_CANNOT_REMOVE_WHEN_NOT_EMPTY;
    }
    if (!storage_->DeleteServer(id)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    auto ix = zoneMap_.find(it->second.GetZoneId());
    if (ix != zoneMap_.end()) {
      ix->second.RemoveServer(id);
    }
    serverMap_.erase(it);
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_SERVER_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::RemoveMetaServer(MetaServerIdType id) {
  WriteLockGuard wlock_server(serverMutex_);
  WriteLockGuard wlock_meta_server(metaServerMutex_);
  auto it = metaServerMap_.find(id);
  if (it != metaServerMap_.end()) {
    uint64_t metaserver_threshold =
        it->second.GetMetaServerSpace().GetDiskThreshold();
    if (!storage_->DeleteMetaServer(id)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    auto ix = serverMap_.find(it->second.GetServerId());
    if (ix != serverMap_.end()) {
      ix->second.RemoveMetaServer(id);
    }
    metaServerMap_.erase(it);

    // update pool
    WriteLockGuard wlock_pool(poolMutex_);
    PoolIdType pool_id = ix->second.GetPoolId();
    auto it = poolMap_.find(pool_id);
    if (it != poolMap_.end()) {
      it->second.SetDiskThreshold(it->second.GetDiskThreshold() -
                                  metaserver_threshold);
    } else {
      return TopoStatusCode::TOPO_POOL_NOT_FOUND;
    }
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::UpdatePool(const Pool& data) {
  WriteLockGuard wlock_pool(poolMutex_);
  auto it = poolMap_.find(data.GetId());
  if (it != poolMap_.end()) {
    if (!storage_->UpdatePool(data)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    it->second = data;
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_POOL_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::UpdateZone(const Zone& data) {
  WriteLockGuard wlock_zone(zoneMutex_);
  auto it = zoneMap_.find(data.GetId());
  if (it != zoneMap_.end()) {
    if (!storage_->UpdateZone(data)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    it->second = data;
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_ZONE_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::UpdateServer(const Server& data) {
  WriteLockGuard wlock_server(serverMutex_);
  auto it = serverMap_.find(data.GetId());
  if (it != serverMap_.end()) {
    if (!storage_->UpdateServer(data)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    it->second = data;
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_SERVER_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::UpdateMetaServerOnlineState(
    const OnlineState& online_state, MetaServerIdType id) {
  ReadLockGuard rlock_meta_server_map(metaServerMutex_);
  auto it = metaServerMap_.find(id);
  if (it != metaServerMap_.end()) {
    if (online_state != it->second.GetOnlineState()) {
      WriteLockGuard wlock_meta_server(it->second.GetRWLockRef());
      it->second.SetOnlineState(online_state);
    }
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::GetPoolIdByMetaserverId(MetaServerIdType id,
                                                     PoolIdType* pool_id_out) {
  *pool_id_out = UNINITIALIZE_ID;
  MetaServer metaserver;
  if (!GetMetaServer(id, &metaserver)) {
    LOG(ERROR) << "TopologyImpl::GetPoolIdByMetaserverId "
               << "Fail On GetMetaServer, " << "metaserverId = " << id;
    return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
  }
  return GetPoolIdByServerId(metaserver.GetServerId(), pool_id_out);
}

TopoStatusCode TopologyImpl::GetPoolIdByServerId(ServerIdType id,
                                                 PoolIdType* pool_id_out) {
  *pool_id_out = UNINITIALIZE_ID;
  Server server;
  if (!GetServer(id, &server)) {
    LOG(ERROR) << "TopologyImpl::GetPoolIdByServerId " << "Fail On GetServer, "
               << "serverId = " << id;
    return TopoStatusCode::TOPO_SERVER_NOT_FOUND;
  }

  *pool_id_out = server.GetPoolId();
  return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyImpl::UpdateMetaServerSpace(const MetaServerSpace& space,
                                                   MetaServerIdType id) {
  // find pool it belongs to
  PoolIdType belong_pool_id = UNINITIALIZE_ID;
  TopoStatusCode ret = GetPoolIdByMetaserverId(id, &belong_pool_id);
  if (ret != TopoStatusCode::TOPO_OK) {
    return ret;
  }

  // fetch write lock of the pool and read lock of metaserver map
  WriteLockGuard wlockl_pool(poolMutex_);
  int64_t diff_threshold = 0;
  {
    ReadLockGuard rlock_meta_server_map(metaServerMutex_);
    auto it = metaServerMap_.find(id);
    if (it != metaServerMap_.end()) {
      WriteLockGuard wlock_meta_server(it->second.GetRWLockRef());
      const MetaServerSpace& old_server_space = it->second.GetMetaServerSpace();

      diff_threshold =
          space.GetDiskThreshold() - old_server_space.GetDiskThreshold();
      int64_t diff_used = space.GetDiskUsed() - old_server_space.GetDiskUsed();
      int64_t diff_disk_min_require =
          space.GetDiskMinRequire() - old_server_space.GetDiskMinRequire();
      int64_t diff_memory_threshold =
          space.GetMemoryThreshold() - old_server_space.GetMemoryThreshold();
      int64_t diff_memory_used =
          space.GetMemoryUsed() - old_server_space.GetMemoryUsed();
      int64_t diff_memory_min_require =
          space.GetMemoryMinRequire() - old_server_space.GetMemoryMinRequire();

      if (diff_threshold != 0 || diff_used != 0 || diff_disk_min_require != 0 ||
          diff_memory_threshold != 0 || diff_memory_used != 0 ||
          diff_memory_min_require != 0) {
        DVLOG(6) << "update metaserver, diffThreshold = " << diff_threshold
                 << ", diffUsed = " << diff_used
                 << ", diffDiskMinRequire = " << diff_disk_min_require
                 << ", diffMemoryThreshold = " << diff_memory_threshold
                 << ", diffMemoryUsed = " << diff_memory_used
                 << ", diffMemoryMinRequire = " << diff_memory_min_require;
        it->second.SetMetaServerSpace(space);
        it->second.SetDirtyFlag(true);
      } else {
        return TopoStatusCode::TOPO_OK;
      }

    } else {
      return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
    }
  }

  if (diff_threshold != 0) {
    // update pool
    auto it = poolMap_.find(belong_pool_id);
    if (it != poolMap_.end()) {
      uint64_t total_threshold = it->second.GetDiskThreshold();
      total_threshold += diff_threshold;
      DVLOG(6) << "update pool to " << total_threshold;
      it->second.SetDiskThreshold(total_threshold);
    } else {
      return TopoStatusCode::TOPO_POOL_NOT_FOUND;
    }
  }

  return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyImpl::UpdateMetaServerStartUpTime(uint64_t time,
                                                         MetaServerIdType id) {
  ReadLockGuard rlock_meta_server_map(metaServerMutex_);
  auto it = metaServerMap_.find(id);
  if (it != metaServerMap_.end()) {
    WriteLockGuard wlock_meta_server(it->second.GetRWLockRef());
    it->second.SetStartUpTime(time);
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
  }
}

PoolIdType TopologyImpl::FindPool(const std::string& pool_name) const {
  ReadLockGuard rlock_pool(poolMutex_);
  for (const auto& it : poolMap_) {
    if (it.second.GetName() == pool_name) {
      return it.first;
    }
  }
  return static_cast<PoolIdType>(UNINITIALIZE_ID);
}

ZoneIdType TopologyImpl::FindZone(const std::string& zone_name,
                                  const std::string& pool_name) const {
  PoolIdType pool_id = FindPool(pool_name);
  return FindZone(zone_name, pool_id);
}

ZoneIdType TopologyImpl::FindZone(const std::string& zone_name,
                                  PoolIdType pool_id) const {
  ReadLockGuard rlock_zone(zoneMutex_);
  for (const auto& it : zoneMap_) {
    if ((it.second.GetPoolId() == pool_id) &&
        (it.second.GetName() == zone_name)) {
      return it.first;
    }
  }
  return static_cast<ZoneIdType>(UNINITIALIZE_ID);
}

ServerIdType TopologyImpl::FindServerByHostName(
    const std::string& host_name) const {
  ReadLockGuard rlock_server(serverMutex_);
  for (const auto& it : serverMap_) {
    if (it.second.GetHostName() == host_name) {
      return it.first;
    }
  }
  return static_cast<ServerIdType>(UNINITIALIZE_ID);
}

ServerIdType TopologyImpl::FindServerByHostIpPort(const std::string& host_ip,
                                                  uint32_t port) const {
  ReadLockGuard rlock_server(serverMutex_);
  for (const auto& it : serverMap_) {
    if (it.second.GetInternalIp() == host_ip) {
      if (0 == it.second.GetInternalPort()) {
        return it.first;
      } else if (port == it.second.GetInternalPort()) {
        return it.first;
      }
    } else if (it.second.GetExternalIp() == host_ip) {
      if (0 == it.second.GetExternalPort()) {
        return it.first;
      } else if (port == it.second.GetExternalPort()) {
        return it.first;
      }
    }
  }
  return static_cast<ServerIdType>(UNINITIALIZE_ID);
}

bool TopologyImpl::GetPool(PoolIdType pool_id, Pool* out) const {
  ReadLockGuard rlock_pool(poolMutex_);
  auto it = poolMap_.find(pool_id);
  if (it != poolMap_.end()) {
    *out = it->second;
    return true;
  }
  return false;
}

bool TopologyImpl::GetZone(ZoneIdType zone_id, Zone* out) const {
  ReadLockGuard rlock_zone(zoneMutex_);
  auto it = zoneMap_.find(zone_id);
  if (it != zoneMap_.end()) {
    *out = it->second;
    return true;
  }
  return false;
}

bool TopologyImpl::GetServer(ServerIdType server_id, Server* out) const {
  ReadLockGuard rlock_server(serverMutex_);
  auto it = serverMap_.find(server_id);
  if (it != serverMap_.end()) {
    *out = it->second;
    return true;
  }
  return false;
}

bool TopologyImpl::GetMetaServer(MetaServerIdType metaserver_id,
                                 MetaServer* out) const {
  ReadLockGuard rlock_meta_server_map(metaServerMutex_);
  auto it = metaServerMap_.find(metaserver_id);
  if (it != metaServerMap_.end()) {
    ReadLockGuard rlock_meta_server(it->second.GetRWLockRef());
    *out = it->second;
    return true;
  }
  return false;
}

bool TopologyImpl::GetMetaServer(const std::string& host_ip, uint32_t port,
                                 MetaServer* out) const {
  ReadLockGuard rlock_meta_server_map(metaServerMutex_);
  for (const auto& it : metaServerMap_) {
    ReadLockGuard rlock_meta_server(it.second.GetRWLockRef());
    if (it.second.GetInternalIp() == host_ip &&
        it.second.GetInternalPort() == port) {
      *out = it.second;
      return true;
    }
  }
  return false;
}

TopoStatusCode TopologyImpl::AddPartition(const Partition& data) {
  WriteLockGuard wlock_cluster(clusterMutex_);
  ReadLockGuard rlock_pool(poolMutex_);
  WriteLockGuard wlock_copyset(copySetMutex_);
  WriteLockGuard wlock_partition(partitionMutex_);
  PartitionIdType id = data.GetPartitionId();

  if (poolMap_.find(data.GetPoolId()) != poolMap_.end()) {
    CopySetKey key(data.GetPoolId(), data.GetCopySetId());
    auto it = copySetMap_.find(key);
    if (it != copySetMap_.end()) {
      if (partitionMap_.find(id) == partitionMap_.end()) {
        if (!storage_->StoragePartition(data)) {
          return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        partitionMap_[id] = data;

        // copyset partitionId only in memory
        it->second.AddPartitionId(id);

        // update fs partition number
        clusterInfo_.AddPartitionIndexOfFs(data.GetFsId());
        if (!storage_->StorageClusterInfo(clusterInfo_)) {
          LOG(ERROR) << "AddPartitionIndexOfFs failed, fsId = "
                     << data.GetFsId();
          return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        return TopoStatusCode::TOPO_OK;
      } else {
        return TopoStatusCode::TOPO_ID_DUPLICATED;
      }
    } else {
      return TopoStatusCode::TOPO_COPYSET_NOT_FOUND;
    }
  } else {
    return TopoStatusCode::TOPO_POOL_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::RemovePartition(PartitionIdType id) {
  WriteLockGuard wlock_copy_set(copySetMutex_);
  WriteLockGuard wlock_partition(partitionMutex_);
  auto it = partitionMap_.find(id);
  if (it != partitionMap_.end()) {
    if (!storage_->DeletePartition(id)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    CopySetKey key(it->second.GetPoolId(), it->second.GetCopySetId());
    auto ix = copySetMap_.find(key);
    if (ix != copySetMap_.end()) {
      // copyset partitionId list only in memory
      ix->second.RemovePartitionId(id);
    }
    partitionMap_.erase(it);
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_PARTITION_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::UpdatePartition(const Partition& data) {
  WriteLockGuard wlock_partition(partitionMutex_);
  auto it = partitionMap_.find(data.GetPartitionId());
  if (it != partitionMap_.end()) {
    if (!storage_->UpdatePartition(data)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    it->second = data;
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_PARTITION_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::UpdatePartitionStatistic(
    uint32_t partition_id, PartitionStatistic statistic) {
  WriteLockGuard wlock_partition(partitionMutex_);
  auto it = partitionMap_.find(partition_id);
  if (it != partitionMap_.end()) {
    Partition temp = it->second;
    temp.SetStatus(statistic.status);
    temp.SetInodeNum(statistic.inodeNum);
    temp.SetDentryNum(statistic.dentryNum);
    temp.SetFileType2InodeNum(statistic.fileType2InodeNum);
    temp.SetIdNext(statistic.nextId);
    if (!storage_->UpdatePartition(temp)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    it->second = temp;
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_PARTITION_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::UpdatePartitionStatus(PartitionIdType partition_id,
                                                   PartitionStatus status) {
  WriteLockGuard wlock_partition(partitionMutex_);
  auto it = partitionMap_.find(partition_id);
  if (it != partitionMap_.end()) {
    Partition temp = it->second;
    temp.SetStatus(status);
    if (!storage_->UpdatePartition(temp)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    it->second = temp;
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_PARTITION_NOT_FOUND;
  }
}

bool TopologyImpl::GetPartition(PartitionIdType partition_id, Partition* out) {
  ReadLockGuard rlock_partition(partitionMutex_);
  auto it = partitionMap_.find(partition_id);
  if (it != partitionMap_.end()) {
    *out = it->second;
    return true;
  }
  return false;
}

bool TopologyImpl::GetCopysetOfPartition(PartitionIdType id,
                                         CopySetInfo* out) const {
  ReadLockGuard rlock_partition(partitionMutex_);
  auto it = partitionMap_.find(id);
  if (it != partitionMap_.end()) {
    PoolIdType pool_id = it->second.GetPoolId();
    CopySetIdType cs_id = it->second.GetCopySetId();
    CopySetKey key(pool_id, cs_id);
    ReadLockGuard rlock_copyset(copySetMutex_);
    auto iter = copySetMap_.find(key);
    if (iter != copySetMap_.end()) {
      *out = iter->second;
      return true;
    }
  }
  return false;
}

std::list<CopySetKey> TopologyImpl::GetAvailableCopysetKeyList() const {
  ReadLockGuard rlock_copy_set(copySetMutex_);
  std::list<CopySetKey> result;
  for (auto const& it : copySetMap_) {
    if (it.second.GetPartitionNum() >= option_.maxPartitionNumberInCopyset) {
      continue;
    }
    result.push_back(it.first);
  }

  return result;
}

std::vector<CopySetInfo> TopologyImpl::GetAvailableCopysetList() const {
  ReadLockGuard rlock_copy_set(copySetMutex_);
  std::vector<CopySetInfo> result;
  for (auto const& it : copySetMap_) {
    if (it.second.GetPartitionNum() >= option_.maxPartitionNumberInCopyset) {
      continue;
    }
    result.push_back(it.second);
  }

  return result;
}

// choose random
int TopologyImpl::GetOneRandomNumber(int start, int end) const {
  thread_local static std::random_device rd;
  thread_local static std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(start, end);
  return dis(gen);
}

bool TopologyImpl::GetAvailableCopyset(CopySetInfo* out) const {
  std::list<CopySetKey> copyset_list = GetAvailableCopysetKeyList();
  if (copyset_list.size() == 0) {
    return false;
  }

  // random select one copyset
  int random_value = GetOneRandomNumber(0, copyset_list.size() - 1);
  auto iter = copyset_list.begin();
  std::advance(iter, random_value);
  auto it = copySetMap_.find(*iter);
  if (it != copySetMap_.end()) {
    *out = it->second;
    return true;
  }
  return false;
}

int TopologyImpl::GetAvailableCopysetNum() const {
  ReadLockGuard rlock_copy_set(copySetMutex_);
  int num = 0;
  for (auto const& it : copySetMap_) {
    if (it.second.GetPartitionNum() >= option_.maxPartitionNumberInCopyset) {
      continue;
    }
    num++;
  }
  return num;
}

std::list<Partition> TopologyImpl::GetPartitionOfFs(
    FsIdType id, PartitionFilter filter) const {
  std::list<Partition> ret;
  ReadLockGuard rlock_partition_map(partitionMutex_);
  for (const auto& it : partitionMap_) {
    if (it.second.GetFsId() == id && filter(it.second)) {
      ret.push_back(it.second);
    }
  }
  return ret;
}

std::list<Partition> TopologyImpl::GetPartitionInfosInPool(
    PoolIdType pool_id, PartitionFilter filter) const {
  std::list<Partition> ret;
  ReadLockGuard rlock_partition_map(partitionMutex_);
  for (const auto& it : partitionMap_) {
    if (it.second.GetPoolId() == pool_id && filter(it.second)) {
      ret.push_back(it.second);
    }
  }
  return ret;
}

std::list<Partition> TopologyImpl::GetPartitionInfosInCopyset(
    CopySetIdType copyset_id) const {
  std::list<Partition> ret;
  ReadLockGuard rlock_partition_map(partitionMutex_);
  for (const auto& it : partitionMap_) {
    if (it.second.GetCopySetId() == copyset_id) {
      ret.push_back(it.second);
    }
  }
  return ret;
}

// getList
std::vector<MetaServerIdType> TopologyImpl::GetMetaServerInCluster(
    MetaServerFilter filter) const {
  std::vector<MetaServerIdType> ret;
  ReadLockGuard rlock_meta_server_map(metaServerMutex_);
  for (const auto& it : metaServerMap_) {
    ReadLockGuard rlock_meta_server(it.second.GetRWLockRef());
    if (filter(it.second)) {
      ret.push_back(it.first);
    }
  }
  return ret;
}

std::vector<ServerIdType> TopologyImpl::GetServerInCluster(
    ServerFilter filter) const {
  std::vector<ServerIdType> ret;
  ReadLockGuard rlock_server(serverMutex_);
  for (const auto& it : serverMap_) {
    if (filter(it.second)) {
      ret.push_back(it.first);
    }
  }
  return ret;
}

std::vector<ZoneIdType> TopologyImpl::GetZoneInCluster(
    ZoneFilter filter) const {
  std::vector<ZoneIdType> ret;
  ReadLockGuard rlock_zone(zoneMutex_);
  for (const auto& it : zoneMap_) {
    if (filter(it.second)) {
      ret.push_back(it.first);
    }
  }
  return ret;
}

std::vector<PoolIdType> TopologyImpl::GetPoolInCluster(
    PoolFilter filter) const {
  std::vector<PoolIdType> ret;
  ReadLockGuard rlock_pool(poolMutex_);
  for (const auto& it : poolMap_) {
    if (filter(it.second)) {
      ret.push_back(it.first);
    }
  }
  return ret;
}

std::list<MetaServerIdType> TopologyImpl::GetMetaServerInServer(
    ServerIdType id, MetaServerFilter filter) const {
  std::list<MetaServerIdType> ret;
  ReadLockGuard rlock_meta_server_map(metaServerMutex_);
  for (const auto& it : metaServerMap_) {
    ReadLockGuard rlock_meta_server(it.second.GetRWLockRef());
    if (it.second.GetServerId() == id && filter(it.second)) {
      ret.push_back(it.first);
    }
  }
  return ret;
}

std::list<MetaServerIdType> TopologyImpl::GetMetaServerInZone(
    ZoneIdType id, MetaServerFilter filter) const {
  std::list<MetaServerIdType> ret;
  std::list<ServerIdType> server_list = GetServerInZone(id);
  for (ServerIdType s : server_list) {
    std::list<MetaServerIdType> temp = GetMetaServerInServer(s, filter);
    ret.splice(ret.begin(), temp);
  }
  return ret;
}

std::list<MetaServerIdType> TopologyImpl::GetMetaServerInPool(
    PoolIdType id, MetaServerFilter filter) const {
  std::list<MetaServerIdType> ret;
  std::list<ZoneIdType> zone_list = GetZoneInPool(id);
  for (ZoneIdType z : zone_list) {
    std::list<MetaServerIdType> temp = GetMetaServerInZone(z, filter);
    ret.splice(ret.begin(), temp);
  }
  return ret;
}

uint32_t TopologyImpl::GetMetaServerNumInPool(PoolIdType id,
                                              MetaServerFilter filter) const {
  return GetMetaServerInPool(id, filter).size();
}

std::list<ServerIdType> TopologyImpl::GetServerInZone(
    ZoneIdType id, ServerFilter filter) const {
  std::list<ServerIdType> ret;
  ReadLockGuard rlock_server(serverMutex_);
  for (const auto& it : serverMap_) {
    if (it.second.GetZoneId() == id && filter(it.second)) {
      ret.push_back(it.first);
    }
  }
  return ret;
}

std::list<ZoneIdType> TopologyImpl::GetZoneInPool(PoolIdType id,
                                                  ZoneFilter filter) const {
  std::list<ZoneIdType> ret;
  ReadLockGuard rlock_zone(zoneMutex_);
  for (const auto& it : zoneMap_) {
    if (it.second.GetPoolId() == id && filter(it.second)) {
      ret.push_back(it.first);
    }
  }
  return ret;
}

std::vector<CopySetIdType> TopologyImpl::GetCopySetsInPool(
    PoolIdType pool_id, CopySetFilter filter) const {
  std::vector<CopySetIdType> ret;
  ReadLockGuard rlock_copy_set(copySetMutex_);
  for (const auto& it : copySetMap_) {
    if (it.first.first == pool_id && filter(it.second)) {
      ret.push_back(it.first.second);
    }
  }
  return ret;
}

uint32_t TopologyImpl::GetCopySetNumInPool(PoolIdType pool_id,
                                           CopySetFilter filter) const {
  return GetCopySetsInPool(pool_id, filter).size();
}

std::vector<CopySetKey> TopologyImpl::GetCopySetsInCluster(
    CopySetFilter filter) const {
  std::vector<CopySetKey> ret;
  ReadLockGuard rlock_copy_set(copySetMutex_);
  for (const auto& it : copySetMap_) {
    if (filter(it.second)) {
      ret.push_back(it.first);
    }
  }
  return ret;
}

std::vector<CopySetInfo> TopologyImpl::GetCopySetInfosInPool(
    PoolIdType pool_id, CopySetFilter filter) const {
  std::vector<CopySetInfo> ret;
  ReadLockGuard rlock_copy_set(copySetMutex_);
  for (const auto& it : copySetMap_) {
    if (it.first.first == pool_id && filter(it.second)) {
      ret.push_back(it.second);
    }
  }
  return ret;
}

std::vector<CopySetKey> TopologyImpl::GetCopySetsInMetaServer(
    MetaServerIdType id, CopySetFilter filter) const {
  std::vector<CopySetKey> ret;
  ReadLockGuard rlock_copy_set(copySetMutex_);
  for (const auto& it : copySetMap_) {
    if (it.second.GetCopySetMembers().count(id) > 0 && filter(it.second)) {
      ret.push_back(it.first);
    }
  }
  return ret;
}

TopoStatusCode TopologyImpl::Init(const TopologyOption& option) {
  option_ = option;
  TopoStatusCode ret = LoadClusterInfo();
  if (ret != TopoStatusCode::TOPO_OK) {
    LOG(ERROR) << "[TopologyImpl::init], LoadClusterInfo fail.";
    return ret;
  }

  PoolIdType max_pool_id;
  if (!storage_->LoadPool(&poolMap_, &max_pool_id)) {
    LOG(ERROR) << "[TopologyImpl::init], LoadPool fail.";
    return TopoStatusCode::TOPO_STORGE_FAIL;
  }
  idGenerator_->initPoolIdGenerator(max_pool_id);
  LOG(INFO) << "[TopologyImpl::init], LoadPool success, "
            << "pool num = " << poolMap_.size();

  ZoneIdType max_zone_id;
  if (!storage_->LoadZone(&zoneMap_, &max_zone_id)) {
    LOG(ERROR) << "[TopologyImpl::init], LoadZone fail.";
    return TopoStatusCode::TOPO_STORGE_FAIL;
  }
  idGenerator_->initZoneIdGenerator(max_zone_id);
  LOG(INFO) << "[TopologyImpl::init], LoadZone success, "
            << "zone num = " << zoneMap_.size();

  ServerIdType max_server_id;
  if (!storage_->LoadServer(&serverMap_, &max_server_id)) {
    LOG(ERROR) << "[TopologyImpl::init], LoadServer fail.";
    return TopoStatusCode::TOPO_STORGE_FAIL;
  }
  idGenerator_->initServerIdGenerator(max_server_id);
  LOG(INFO) << "[TopologyImpl::init], LoadServer success, "
            << "server num = " << serverMap_.size();

  MetaServerIdType max_meta_server_id;
  if (!storage_->LoadMetaServer(&metaServerMap_, &max_meta_server_id)) {
    LOG(ERROR) << "[TopologyImpl::init], LoadMetaServer fail.";
    return TopoStatusCode::TOPO_STORGE_FAIL;
  }
  idGenerator_->initMetaServerIdGenerator(max_meta_server_id);
  LOG(INFO) << "[TopologyImpl::init], LoadMetaServer success, "
            << "metaserver num = " << metaServerMap_.size();

  // update pool capacity
  for (const auto& pair : metaServerMap_) {
    PoolIdType pool_id = UNINITIALIZE_ID;
    TopoStatusCode ret = GetPoolIdByMetaserverId(pair.second.GetId(), &pool_id);
    if (ret != TopoStatusCode::TOPO_OK) {
      return ret;
    }

    auto it = poolMap_.find(pool_id);
    if (it != poolMap_.end()) {
      uint64_t total_threshold =
          it->second.GetDiskThreshold() +
          pair.second.GetMetaServerSpace().GetDiskThreshold();
      it->second.SetDiskThreshold(total_threshold);
    } else {
      LOG(ERROR) << "TopologyImpl::Init Fail On Get Pool, "
                 << "poolId = " << pool_id;
      return TopoStatusCode::TOPO_POOL_NOT_FOUND;
    }
  }
  LOG(INFO) << "Calc Pool capacity success.";

  std::map<PoolIdType, CopySetIdType> copy_set_id_max_map;
  if (!storage_->LoadCopySet(&copySetMap_, &copy_set_id_max_map)) {
    LOG(ERROR) << "[TopologyImpl::init], LoadCopySet fail.";
    return TopoStatusCode::TOPO_STORGE_FAIL;
  }
  idGenerator_->initCopySetIdGenerator(copy_set_id_max_map);
  LOG(INFO) << "[TopologyImpl::init], LoadCopySet success, "
            << "copyset num = " << copySetMap_.size();

  PartitionIdType max_partition_id;
  if (!storage_->LoadPartition(&partitionMap_, &max_partition_id)) {
    LOG(ERROR) << "[TopologyImpl::init], LoadPartition fail.";
    return TopoStatusCode::TOPO_STORGE_FAIL;
  }
  idGenerator_->initPartitionIdGenerator(max_partition_id);

  // MemcacheCluster
  MemcacheClusterIdType max_memcache_cluster_id;
  if (!storage_->LoadMemcacheCluster(&memcacheClusterMap_,
                                     &max_memcache_cluster_id)) {
    LOG(ERROR) << "[TopologyImpl::init], LoadMemcacheCluster fail.";
    return TopoStatusCode::TOPO_STORGE_FAIL;
  }
  idGenerator_->initMemcacheClusterIdGenerator(max_memcache_cluster_id);

  // Fs2MemcacheCLuster
  if (!storage_->LoadFs2MemcacheCluster(&fs2MemcacheCluster_)) {
    LOG(ERROR) << "[TopologyImpl::init], LoadFs2MemcacheCluster fail.";
    return TopoStatusCode::TOPO_STORGE_FAIL;
  }

  // for upgrade and keep compatibility
  // the old version have no partitionIndex in etcd, so need update here of
  // upgrade  // NOLINT if the fs in old cluster already delete some
  // partitions, it is incompatible.    // NOLINT
  if (!RefreshPartitionIndexOfFS(partitionMap_)) {
    LOG(ERROR) << "[TopologyImpl::init],  RefreshPartitionIndexOfFS fail.";
    return TopoStatusCode::TOPO_STORGE_FAIL;
  }
  LOG(INFO) << "[TopologyImpl::init], LoadPartition success, "
            << "partition num = " << partitionMap_.size();

  for (const auto& it : partitionMap_) {
    CopySetKey key(it.second.GetPoolId(), it.second.GetCopySetId());
    copySetMap_[key].AddPartitionId(it.first);
  }

  for (const auto& it : zoneMap_) {
    PoolIdType poolid = it.second.GetPoolId();
    poolMap_[poolid].AddZone(it.first);
  }

  for (const auto& it : serverMap_) {
    ZoneIdType zid = it.second.GetZoneId();
    zoneMap_[zid].AddServer(it.first);
  }

  for (const auto& it : metaServerMap_) {
    ServerIdType s_id = it.second.GetServerId();
    serverMap_[s_id].AddMetaServer(it.first);
  }

  return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyImpl::AddCopySet(const CopySetInfo& data) {
  ReadLockGuard rlock_pool(poolMutex_);
  WriteLockGuard wlock_copy_set_map(copySetMutex_);
  auto it = poolMap_.find(data.GetPoolId());
  if (it != poolMap_.end()) {
    CopySetKey key(data.GetPoolId(), data.GetId());
    if (copySetMap_.find(key) == copySetMap_.end()) {
      if (!storage_->StorageCopySet(data)) {
        return TopoStatusCode::TOPO_STORGE_FAIL;
      }
      copySetMap_[key] = data;
      return TopoStatusCode::TOPO_OK;
    } else {
      return TopoStatusCode::TOPO_ID_DUPLICATED;
    }
  } else {
    return TopoStatusCode::TOPO_POOL_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::AddCopySetCreating(const CopySetKey& key) {
  WriteLockGuard wlock_copy_set_creating(copySetCreatingMutex_);
  auto iter = copySetCreating_.insert(key);
  return iter.second ? TopoStatusCode::TOPO_OK
                     : TopoStatusCode::TOPO_ID_DUPLICATED;
}

TopoStatusCode TopologyImpl::RemoveCopySet(CopySetKey key) {
  WriteLockGuard wlock_copy_set_map(copySetMutex_);
  auto it = copySetMap_.find(key);
  if (it != copySetMap_.end()) {
    if (!storage_->DeleteCopySet(key)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    copySetMap_.erase(key);
    return TopoStatusCode::TOPO_OK;
  } else {
    return TopoStatusCode::TOPO_COPYSET_NOT_FOUND;
  }
}

void TopologyImpl::RemoveCopySetCreating(CopySetKey key) {
  WriteLockGuard wlock_copy_set_creating(copySetCreatingMutex_);
  copySetCreating_.erase(key);
}

TopoStatusCode TopologyImpl::UpdateCopySetTopo(const CopySetInfo& data) {
  ReadLockGuard rlock_copy_set_map(copySetMutex_);
  CopySetKey key(data.GetPoolId(), data.GetId());
  auto it = copySetMap_.find(key);
  if (it != copySetMap_.end()) {
    WriteLockGuard wlock_copy_set(it->second.GetRWLockRef());
    it->second.SetLeader(data.GetLeader());
    it->second.SetEpoch(data.GetEpoch());
    it->second.SetCopySetMembers(data.GetCopySetMembers());
    it->second.SetDirtyFlag(true);
    if (data.HasCandidate()) {
      it->second.SetCandidate(data.GetCandidate());
    } else {
      it->second.ClearCandidate();
    }
    return TopoStatusCode::TOPO_OK;
  } else {
    LOG(WARNING) << "UpdateCopySetTopo can not find copyset, "
                 << "poolId = " << data.GetPoolId()
                 << ", copysetId = " << data.GetId();
    return TopoStatusCode::TOPO_COPYSET_NOT_FOUND;
  }
}

TopoStatusCode TopologyImpl::SetCopySetAvalFlag(const CopySetKey& key,
                                                bool aval) {
  ReadLockGuard rlock_copy_set_map(copySetMutex_);
  auto it = copySetMap_.find(key);
  if (it != copySetMap_.end()) {
    WriteLockGuard wlock_copy_set(it->second.GetRWLockRef());
    auto copyset_info = it->second;
    copyset_info.SetAvailableFlag(aval);
    bool ret = storage_->UpdateCopySet(copyset_info);
    if (!ret) {
      LOG(ERROR) << "UpdateCopySet met storage error";
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    it->second.SetAvailableFlag(aval);
    return TopoStatusCode::TOPO_OK;
  } else {
    LOG(WARNING) << "SetCopySetAvalFlag can not find copyset, "
                 << "poolId = " << key.first << ", copysetId = " << key.second;
    return TopoStatusCode::TOPO_COPYSET_NOT_FOUND;
  }
}

bool TopologyImpl::GetCopySet(CopySetKey key, CopySetInfo* out) const {
  ReadLockGuard rlock_copy_set_map(copySetMutex_);
  auto it = copySetMap_.find(key);
  if (it != copySetMap_.end()) {
    ReadLockGuard rlock_copy_set(it->second.GetRWLockRef());
    *out = it->second;
    return true;
  } else {
    return false;
  }
}

int TopologyImpl::Run() {
  if (isStop_.exchange(false)) {
    backEndThread_ = curve::common::Thread(&TopologyImpl::BackEndFunc, this);
  }
  return 0;
}

int TopologyImpl::Stop() {
  if (!isStop_.exchange(true)) {
    LOG(INFO) << "stop TopologyImpl...";
    sleeper_.interrupt();
    backEndThread_.join();
    LOG(INFO) << "stop TopologyImpl ok.";
  }
  return 0;
}

void TopologyImpl::BackEndFunc() {
  while (sleeper_.wait_for(
      std::chrono::seconds(option_.topologyUpdateToRepoSec))) {
    FlushCopySetToStorage();
    FlushMetaServerToStorage();
  }
}

void TopologyImpl::FlushCopySetToStorage() {
  std::vector<PoolIdType> pools = GetPoolInCluster();
  for (const auto pool_id : pools) {
    ReadLockGuard rlock_copy_set_map(copySetMutex_);
    for (auto& c : copySetMap_) {
      WriteLockGuard wlock_copy_set(c.second.GetRWLockRef());
      if (c.second.GetDirtyFlag() && c.second.GetPoolId() == pool_id) {
        c.second.SetDirtyFlag(false);
        if (!storage_->UpdateCopySet(c.second)) {
          LOG(WARNING) << "update copyset(" << c.second.GetPoolId() << ","
                       << c.second.GetId() << ") to repo fail";
        }
      }
    }
  }
}

void TopologyImpl::FlushMetaServerToStorage() {
  std::vector<MetaServer> to_update;
  {
    ReadLockGuard rlock_meta_server_map(metaServerMutex_);
    for (auto& c : metaServerMap_) {
      // update DirtyFlag only, thus only read lock is needed
      ReadLockGuard rlock_meta_server(c.second.GetRWLockRef());
      if (c.second.GetDirtyFlag()) {
        c.second.SetDirtyFlag(false);
        to_update.push_back(c.second);
      }
    }
  }
  for (const auto& v : to_update) {
    if (!storage_->UpdateMetaServer(v)) {
      LOG(WARNING) << "update metaserver to repo fail"
                   << ", metaserverid = " << v.GetId();
    }
  }
}

TopoStatusCode TopologyImpl::LoadClusterInfo() {
  std::vector<ClusterInformation> infos;
  if (!storage_->LoadClusterInfo(&infos)) {
    return TopoStatusCode::TOPO_STORGE_FAIL;
  }
  if (infos.empty()) {
    std::string uuid = UUIDGenerator().GenerateUUID();
    ClusterInformation info(uuid);
    if (!storage_->StorageClusterInfo(info)) {
      return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    clusterInfo_ = info;
  } else {
    clusterInfo_ = infos[0];
  }
  return TopoStatusCode::TOPO_OK;
}

bool TopologyImpl::GetClusterInfo(ClusterInformation* info) {
  ReadLockGuard rlock(clusterMutex_);
  *info = clusterInfo_;
  return true;
}

// update partition tx, and ensure atomicity
TopoStatusCode TopologyImpl::UpdatePartitionTxIds(
    std::vector<PartitionTxId> tx_ids) {
  std::vector<Partition> partitions;
  WriteLockGuard wlock_partition(partitionMutex_);
  for (const auto& item : tx_ids) {
    auto it = partitionMap_.find(item.partitionid());
    if (it != partitionMap_.end()) {
      ReadLockGuard rlock_partition(it->second.GetRWLockRef());
      Partition tmp = it->second;
      tmp.SetTxId(item.txid());
      partitions.emplace_back(tmp);
    } else {
      LOG(ERROR) << "UpdatePartition failed, partition not found."
                 << " partition id = " << item.partitionid();
      return TopoStatusCode::TOPO_PARTITION_NOT_FOUND;
    }
  }
  if (storage_->UpdatePartitions(partitions)) {
    // update memory
    for (const auto& item : partitions) {
      partitionMap_[item.GetPartitionId()] = item;
    }
    return TopoStatusCode::TOPO_OK;
  }
  LOG(ERROR) << "UpdatepPartition failed, storage failure.";
  return TopoStatusCode::TOPO_STORGE_FAIL;
}

TopoStatusCode TopologyImpl::ChooseNewMetaServerForCopyset(
    PoolIdType pool_id, const std::set<ZoneIdType>& unavailable_zones,
    const std::set<MetaServerIdType>& unavailable_ms,
    MetaServerIdType* target) {
  MetaServerFilter filter = [](const MetaServer& ms) {
    return ms.GetOnlineState() == OnlineState::ONLINE;
  };

  auto metaservers = GetMetaServerInPool(pool_id, filter);
  *target = UNINITIALIZE_ID;
  double temp_used_percent = 100;

  for (const auto& it : metaservers) {
    auto iter = unavailable_ms.find(it);
    if (iter != unavailable_ms.end()) {
      continue;
    }

    MetaServer metaserver;
    if (GetMetaServer(it, &metaserver)) {
      Server server;
      if (GetServer(metaserver.GetServerId(), &server)) {
        auto iter = unavailable_zones.find(server.GetZoneId());
        if (iter == unavailable_zones.end()) {
          double used =
              metaserver.GetMetaServerSpace().GetResourceUseRatioPercent();
          if (metaserver.GetMetaServerSpace().IsMetaserverResourceAvailable() &&
              used < temp_used_percent) {
            *target = it;
            temp_used_percent = used;
          }
        }
      } else {
        LOG(ERROR) << "get server failed,"
                   << " the server id = " << metaserver.GetServerId();
        return TopoStatusCode::TOPO_SERVER_NOT_FOUND;
      }
    } else {
      LOG(ERROR) << "get metaserver failed," << " the metaserver id = " << it;
      return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
    }
  }

  if (UNINITIALIZE_ID == *target) {
    return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
  }
  return TopoStatusCode::TOPO_OK;
}

uint32_t TopologyImpl::GetCopysetNumInMetaserver(MetaServerIdType id) const {
  ReadLockGuard rlock_copy_set_map(copySetMutex_);
  uint32_t num = 0;
  for (const auto& it : copySetMap_) {
    if (it.second.HasMember(id)) {
      num++;
    }
  }

  return num;
}

uint32_t TopologyImpl::GetLeaderNumInMetaserver(MetaServerIdType id) const {
  ReadLockGuard rlock_copy_set_map(copySetMutex_);
  uint32_t num = 0;
  for (const auto& it : copySetMap_) {
    if (it.second.GetLeader() == id) {
      num++;
    }
  }
  return num;
}

void TopologyImpl::GetAvailableMetaserversUnlock(
    std::vector<const MetaServer*>* vec) {
  for (const auto& it : metaServerMap_) {
    if (it.second.GetOnlineState() == OnlineState::ONLINE &&
        it.second.GetMetaServerSpace().IsMetaserverResourceAvailable() &&
        GetCopysetNumInMetaserver(it.first) <
            option_.maxCopysetNumInMetaserver) {
      vec->emplace_back(&(it.second));
    }
  }
}

TopoStatusCode TopologyImpl::GenCandidateMapUnlock(
    PoolIdType pool_id,
    std::map<ZoneIdType, std::vector<MetaServerIdType>>* candidate_map) {
  // 1. get all online and available metaserver
  std::vector<const MetaServer*> metaservers;
  GetAvailableMetaserversUnlock(&metaservers);
  for (const auto* it : metaservers) {
    ServerIdType server_id = it->GetServerId();
    Server server;
    if (!GetServer(server_id, &server)) {
      LOG(ERROR) << "get server failed when choose metaservers,"
                 << " the serverId = " << server_id;
      return TopoStatusCode::TOPO_SERVER_NOT_FOUND;
    }

    if (pool_id != server.GetPoolId()) {
      continue;
    }

    ZoneIdType zone_id = server.GetZoneId();
    (*candidate_map)[zone_id].push_back(it->GetId());
  }

  return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyImpl::GenCopysetAddrBatchForPool(
    PoolIdType pool_id, uint16_t replica_num,
    std::list<CopysetCreateInfo>* copyset_list) {
  // 1. genarate candidateMap
  std::map<ZoneIdType, std::vector<MetaServerIdType>> candidate_map;
  auto ret = GenCandidateMapUnlock(pool_id, &candidate_map);
  if (ret != TopoStatusCode::TOPO_OK) {
    LOG(ERROR) << "generate candidate map for pool " << pool_id
               << "fail, retCode = " << TopoStatusCode_Name(ret);
    return ret;
  }

  // 2. return error if candidate map has no enough replicaNum
  if (candidate_map.size() < replica_num) {
    LOG(WARNING) << "can not find available metaserver for copyset, "
                 << "poolId = " << pool_id
                 << " need replica num = " << replica_num
                 << ", but only has available zone num = "
                 << candidate_map.size();
    return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
  }

  // 3. get min size in case of the metaserver num in zone is different
  uint32_t min_size = UINT32_MAX;
  std::vector<ZoneIdType> zone_ids;
  for (auto& it : candidate_map) {
    if (it.second.size() < min_size) {
      min_size = it.second.size();
    }
    zone_ids.push_back(it.first);
  }

  // 4. generate enough copyset
  uint32_t create_count = 0;

  thread_local static std::random_device rd;
  thread_local static std::mt19937 random_generator(rd());
  while (create_count < min_size * replica_num) {
    for (auto& it : candidate_map) {
      std::shuffle(it.second.begin(), it.second.end(), random_generator);
    }

    std::shuffle(zone_ids.begin(), zone_ids.end(), random_generator);

    std::vector<MetaServerIdType> ms_ids;
    for (uint32_t i = 0; i < min_size; i++) {
      for (const auto& zone_id : zone_ids) {
        ms_ids.push_back(candidate_map[zone_id][i]);
      }
    }

    for (size_t i = 0; i < ms_ids.size() / replica_num; i++) {
      CopysetCreateInfo copyset_info;
      copyset_info.poolId = pool_id;
      copyset_info.copysetId = UNINITIALIZE_ID;
      for (int j = 0; j < replica_num; j++) {
        copyset_info.metaServerIds.insert(ms_ids[i * replica_num + j]);
      }
      copyset_list->emplace_back(copyset_info);
      create_count++;
    }
  }

  return TopoStatusCode::TOPO_OK;
}

// Check if there is no copy on the pool.
// Generate copyset on the empty copyset pools.
void TopologyImpl::GenCopysetIfPoolEmptyUnlocked(
    std::list<CopysetCreateInfo>* copyset_list) {
  for (const auto& it : poolMap_) {
    PoolIdType pool_id = it.first;
    uint32_t metaserver_num = GetMetaServerNumInPool(pool_id);
    if (metaserver_num == 0) {
      continue;
    }

    uint32_t copyset_num = GetCopySetNumInPool(pool_id);
    if (copyset_num != 0) {
      continue;
    }

    uint16_t replica_num = it.second.GetReplicaNum();
    if (replica_num == 0) {
      LOG(INFO) << "Initial Generate copyset addr, skip pool " << pool_id
                << ", replicaNum is 0";
      continue;
    }
    std::list<CopysetCreateInfo> temp_copyset_list;
    TopoStatusCode ret =
        GenCopysetAddrBatchForPool(pool_id, replica_num, &temp_copyset_list);
    if (TopoStatusCode::TOPO_OK == ret) {
      LOG(INFO) << "Initial Generate copyset addr for pool " << pool_id
                << " success, gen copyset num = " << temp_copyset_list.size();
      copyset_list->splice(copyset_list->end(), temp_copyset_list);
    } else {
      LOG(WARNING) << "Initial Generate copyset addr for pool " << pool_id
                   << " fail, statusCode = " << TopoStatusCode_Name(ret);
    }
  }
}

// generate at least needCreateNum copyset addr in this function
// 1. get all online metaserver, and divide these metaserver into different pool
// 2. sort the pool list by average copyset num ascending,
//    average copyset num in pool = copyset num in pool / metaserver num in pool
// 3. according to the pool order of step 2, generate copyset add in the pool
//    in turn until enough copyset add is generated
TopoStatusCode TopologyImpl::GenSubsequentCopysetAddrBatchUnlocked(
    uint32_t need_create_num, std::list<CopysetCreateInfo>* copyset_list) {
  LOG(INFO) << "GenSubsequentCopysetAddrBatch needCreateNum = "
            << need_create_num
            << ", copysetList size = " << copyset_list->size() << " begin";

  MetaServerFilter filter = [](const MetaServer& ms) {
    return ms.GetOnlineState() == OnlineState::ONLINE;
  };

  std::vector<Pool> pool_list;
  for (const auto& it : poolMap_) {
    if (GetMetaServerNumInPool(it.first, filter) != 0) {
      pool_list.push_back(it.second);
    }
  }

  // sort pool list by copyset average num
  std::sort(
      pool_list.begin(), pool_list.end(), [=](const Pool& a, const Pool& b) {
        PoolIdType pool_id1 = a.GetId();
        PoolIdType pool_id2 = b.GetId();
        uint32_t copyset_num1 = GetCopySetNumInPool(pool_id1);
        uint32_t copyset_num2 = GetCopySetNumInPool(pool_id2);
        uint32_t metaserver_num1 = GetMetaServerNumInPool(pool_id1, filter);
        uint32_t metaserver_num2 = GetMetaServerNumInPool(pool_id2, filter);
        double ava_copyset_num1 = copyset_num1 * 1.0 / metaserver_num1;
        double ava_copyset_num2 = copyset_num2 * 1.0 / metaserver_num2;
        return ava_copyset_num1 < ava_copyset_num2;
      });

  while (copyset_list->size() < need_create_num) {
    for (auto it = pool_list.begin(); it != pool_list.end();) {
      PoolIdType pool_id = it->GetId();
      uint16_t replica_num = it->GetReplicaNum();
      std::list<CopysetCreateInfo> temp_copyset_list;
      TopoStatusCode ret =
          GenCopysetAddrBatchForPool(pool_id, replica_num, &temp_copyset_list);
      if (TopoStatusCode::TOPO_OK == ret) {
        copyset_list->splice(copyset_list->end(), temp_copyset_list);
        if (copyset_list->size() >= need_create_num) {
          return TopoStatusCode::TOPO_OK;
        }
        it++;
      } else {
        LOG(WARNING) << "Generate " << need_create_num
                     << " copyset addr for pool " << pool_id
                     << "fail, statusCode = " << TopoStatusCode_Name(ret);
        it = pool_list.erase(it);
      }
    }

    if (copyset_list->size() == 0 || pool_list.size() == 0) {
      LOG(ERROR) << "can not find available metaserver for copyset.";
      return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
    }
  }

  return TopoStatusCode::TOPO_OK;
}

// GenCopysetAddrBatch will generate copyset create info list.
// The CopysetCreateInfo generate here with poolId and
// metaServerIds, the copyset id will be generated outside the function
// 1. Gen addr on the pool which has no copyset, if the number of gen copy addr
//    in this step is enough, return the list.
// 2. Sort the pools according to the average number of copies,
//    and traverse each pool to create copies until the number is sufficient.
TopoStatusCode TopologyImpl::GenCopysetAddrBatch(
    uint32_t need_create_num, std::list<CopysetCreateInfo>* copyset_list) {
  ReadLockGuard rlock_pool(poolMutex_);
  ReadLockGuard rlock_metaserver(metaServerMutex_);
  ReadLockGuard rlock_copyset(copySetMutex_);

  GenCopysetIfPoolEmptyUnlocked(copyset_list);
  if (copyset_list->size() > need_create_num) {
    return TopoStatusCode::TOPO_OK;
  }

  return GenSubsequentCopysetAddrBatchUnlocked(need_create_num, copyset_list);
}

uint32_t TopologyImpl::GetPartitionIndexOfFS(FsIdType fs_id) {
  ReadLockGuard rlock(clusterMutex_);
  return clusterInfo_.GetPartitionIndexOfFS(fs_id);
}

std::vector<CopySetInfo> TopologyImpl::ListCopysetInfo() const {
  std::vector<CopySetInfo> ret;
  ret.reserve(copySetMap_.size());
  for (auto const& i : copySetMap_) {
    ret.emplace_back(i.second);
  }
  return ret;
}

void TopologyImpl::GetMetaServersSpace(
    ::google::protobuf::RepeatedPtrField<curvefs::mds::topology::MetadataUsage>*
        spaces) {
  ReadLockGuard rlock_meta_server_map(metaServerMutex_);
  for (auto const& i : metaServerMap_) {
    ReadLockGuard rlock_meta_server(i.second.GetRWLockRef());
    auto* meta_server_usage = new curvefs::mds::topology::MetadataUsage();
    meta_server_usage->set_metaserveraddr(
        i.second.GetInternalIp() + ":" +
        std::to_string(i.second.GetInternalPort()));
    auto const& space = i.second.GetMetaServerSpace();
    meta_server_usage->set_total(space.GetDiskThreshold());
    meta_server_usage->set_used(space.GetDiskUsed());
    spaces->AddAllocated(meta_server_usage);
  }
}

std::string TopologyImpl::GetHostNameAndPortById(MetaServerIdType ms_id) {
  // get target metaserver
  MetaServer ms;
  if (!GetMetaServer(ms_id, &ms)) {
    LOG(INFO) << "get metaserver " << ms_id << " err";
    return "";
  }

  // get the server of the target metaserver
  Server server;
  if (!GetServer(ms.GetServerId(), &server)) {
    LOG(INFO) << "get server " << ms.GetServerId() << " err";
    return "";
  }

  // get hostName of the metaserver
  return server.GetHostName() + ":" + std::to_string(ms.GetInternalPort());
}

bool TopologyImpl::IsCopysetCreating(const CopySetKey& key) const {
  ReadLockGuard rlock_copy_set_creating(copySetCreatingMutex_);
  return copySetCreating_.count(key) != 0;
}

bool TopologyImpl::RefreshPartitionIndexOfFS(
    const std::unordered_map<PartitionIdType, Partition>& partition_map) {
  // <fsId, partitionNum>
  std::map<uint32_t, uint32_t> tmap;
  for (const auto& it : partition_map) {
    tmap[it.second.GetFsId()]++;
  }
  for (const auto& it : tmap) {
    clusterInfo_.UpdatePartitionIndexOfFs(it.first, it.second);
  }
  return storage_->StorageClusterInfo(clusterInfo_);
}

std::list<MemcacheServer> TopologyImpl::ListMemcacheServers() const {
  ReadLockGuard rlock_memcache_cluster(memcacheClusterMutex_);
  std::list<MemcacheServer> ret;
  for (auto const& cluster : memcacheClusterMap_) {
    auto const& servers = cluster.second.GetServers();
    ret.insert(ret.begin(), servers.cbegin(), servers.cend());
  }
  return ret;
}

TopoStatusCode TopologyImpl::AddMemcacheCluster(const MemcacheCluster& data) {
  WriteLockGuard wlock_memcache_cluster(memcacheClusterMutex_);
  // storage_ to storage
  TopoStatusCode ret = TopoStatusCode::TOPO_OK;
  if (!storage_->StorageMemcacheCluster(data)) {
    ret = TopoStatusCode::TOPO_STORGE_FAIL;
  } else {
    memcacheClusterMap_[data.GetId()] = data;
  }

  return ret;
}

TopoStatusCode TopologyImpl::AddMemcacheCluster(MemcacheCluster&& data) {
  WriteLockGuard wlock_memcache_cluster(memcacheClusterMutex_);
  // storage_ to storage
  TopoStatusCode ret = TopoStatusCode::TOPO_OK;
  if (!storage_->StorageMemcacheCluster(data)) {
    ret = TopoStatusCode::TOPO_STORGE_FAIL;
  } else {
    memcacheClusterMap_.insert(std::make_pair(data.GetId(), std::move(data)));
  }
  return ret;
}

std::list<MemcacheCluster> TopologyImpl::ListMemcacheClusters() const {
  std::list<MemcacheCluster> ret;
  ReadLockGuard rlock_memcache_cluster(memcacheClusterMutex_);
  for (auto const& cluster : memcacheClusterMap_) {
    ret.emplace_back(cluster.second);
  }
  return ret;
}

TopoStatusCode TopologyImpl::AllocOrGetMemcacheCluster(
    FsIdType fs_id, MemcacheClusterInfo* cluster) {
  TopoStatusCode ret = TopoStatusCode::TOPO_OK;
  WriteLockGuard wlock_fs2_memcache_cluster(fs2MemcacheClusterMutex_);
  ReadLockGuard rlock_memcache_cluster(memcacheClusterMutex_);
  if (fs2MemcacheCluster_.find(fs_id) != fs2MemcacheCluster_.end()) {
    *cluster = memcacheClusterMap_[fs2MemcacheCluster_[fs_id]];
  } else if (memcacheClusterMap_.empty()) {
    ret = TopoStatusCode::TOPO_MEMCACHECLUSTER_NOT_FOUND;
  } else {
    int rand_id =
        static_cast<int>(butil::fast_rand()) % memcacheClusterMap_.size();
    auto iter = memcacheClusterMap_.cbegin();
    std::advance(iter, rand_id);
    if (!storage_->StorageFs2MemcacheCluster(fs_id, iter->first)) {
      ret = TopoStatusCode::TOPO_STORGE_FAIL;
    } else {
      fs2MemcacheCluster_[fs_id] = iter->first;
      *cluster = iter->second;
    }
  }
  return ret;
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
