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
 * Created Date: 2021-08-24
 * Author: wanghai01
 */

#ifndef DINGOFS_SRC_MDS_TOPOLOGY_TOPOLOGY_MANAGER_H_
#define DINGOFS_SRC_MDS_TOPOLOGY_TOPOLOGY_MANAGER_H_

#include <list>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "proto/topology.pb.h"
#include "mds/metaserverclient/metaserver_client.h"
#include "mds/topology/topology.h"
#include "utils/concurrent/name_lock.h"

namespace dingofs {
namespace mds {
namespace topology {

using dingofs::mds::MetaserverClient;
using dingofs::utils::NameLock;
using std::string;

class TopologyManager {
 public:
  TopologyManager(const std::shared_ptr<Topology>& topology,
                  std::shared_ptr<MetaserverClient> metaserver_client)
      : topology_(topology), metaserverClient_(metaserver_client) {}

  virtual ~TopologyManager() = default;

  virtual void Init(const TopologyOption& option);

  virtual void RegistMetaServer(
      const pb::mds::topology::MetaServerRegistRequest* request,
      pb::mds::topology::MetaServerRegistResponse* response);

  virtual void ListMetaServer(
      const pb::mds::topology::ListMetaServerRequest* request,
      pb::mds::topology::ListMetaServerResponse* response);

  virtual void GetMetaServer(
      const pb::mds::topology::GetMetaServerInfoRequest* request,
      pb::mds::topology::GetMetaServerInfoResponse* response);

  virtual void DeleteMetaServer(
      const pb::mds::topology::DeleteMetaServerRequest* request,
      pb::mds::topology::DeleteMetaServerResponse* response);

  virtual void RegistServer(
      const pb::mds::topology::ServerRegistRequest* request,
      pb::mds::topology::ServerRegistResponse* response);

  virtual void GetServer(const pb::mds::topology::GetServerRequest* request,
                         pb::mds::topology::GetServerResponse* response);

  virtual void DeleteServer(
      const pb::mds::topology::DeleteServerRequest* request,
      pb::mds::topology::DeleteServerResponse* response);

  virtual void ListZoneServer(
      const pb::mds::topology::ListZoneServerRequest* request,
      pb::mds::topology::ListZoneServerResponse* response);

  virtual void CreateZone(const pb::mds::topology::CreateZoneRequest* request,
                          pb::mds::topology::CreateZoneResponse* response);

  virtual void DeleteZone(const pb::mds::topology::DeleteZoneRequest* request,
                          pb::mds::topology::DeleteZoneResponse* response);

  virtual void GetZone(const pb::mds::topology::GetZoneRequest* request,
                       pb::mds::topology::GetZoneResponse* response);

  virtual void ListPoolZone(
      const pb::mds::topology::ListPoolZoneRequest* request,
      pb::mds::topology::ListPoolZoneResponse* response);

  virtual void CreatePool(const pb::mds::topology::CreatePoolRequest* request,
                          pb::mds::topology::CreatePoolResponse* response);

  virtual void DeletePool(const pb::mds::topology::DeletePoolRequest* request,
                          pb::mds::topology::DeletePoolResponse* response);

  virtual void GetPool(const pb::mds::topology::GetPoolRequest* request,
                       pb::mds::topology::GetPoolResponse* response);

  virtual void ListPool(const pb::mds::topology::ListPoolRequest* request,
                        pb::mds::topology::ListPoolResponse* response);

  virtual void CreatePartitions(
      const pb::mds::topology::CreatePartitionRequest* request,
      pb::mds::topology::CreatePartitionResponse* response);

  virtual void DeletePartition(
      const pb::mds::topology::DeletePartitionRequest* request,
      pb::mds::topology::DeletePartitionResponse* response);

  virtual TopoStatusCode CreatePartitionsAndGetMinPartition(
      FsIdType fs_id, pb::common::PartitionInfo* partition);

  virtual TopoStatusCode DeletePartition(uint32_t partition_id);

  virtual TopoStatusCode CommitTxId(
      const std::vector<pb::mds::topology::PartitionTxId>& tx_ids);

  virtual void CommitTx(const pb::mds::topology::CommitTxRequest* request,
                        pb::mds::topology::CommitTxResponse* response);

  virtual void GetMetaServerListInCopysets(
      const pb::mds::topology::GetMetaServerListInCopySetsRequest* request,
      pb::mds::topology::GetMetaServerListInCopySetsResponse* response);

  virtual void ListPartition(
      const pb::mds::topology::ListPartitionRequest* request,
      pb::mds::topology::ListPartitionResponse* response);

  virtual void ListPartitionOfFs(FsIdType fs_id,
                                 std::list<pb::common::PartitionInfo>* list);

  virtual void GetLatestPartitionsTxId(
      const std::vector<pb::mds::topology::PartitionTxId>& tx_ids,
      std::vector<pb::mds::topology::PartitionTxId>* need_update);

  virtual TopoStatusCode UpdatePartitionStatus(
      PartitionIdType partition_id, pb::common::PartitionStatus status);

  virtual void GetCopysetOfPartition(
      const pb::mds::topology::GetCopysetOfPartitionRequest* request,
      pb::mds::topology::GetCopysetOfPartitionResponse* response);

  virtual TopoStatusCode GetCopysetMembers(PoolIdType pool_id,
                                           CopySetIdType copyset_id,
                                           std::set<std::string>* addrs);

  virtual bool CreateCopysetNodeOnMetaServer(PoolIdType pool_id,
                                             CopySetIdType copyset_id,
                                             MetaServerIdType meta_server_id);

  virtual void GetCopysetsInfo(
      const pb::mds::topology::GetCopysetsInfoRequest* request,
      pb::mds::topology::GetCopysetsInfoResponse* response);

  virtual void ListCopysetsInfo(
      pb::mds::topology::ListCopysetInfoResponse* response);

  virtual void GetMetaServersSpace(
      ::google::protobuf::RepeatedPtrField<pb::mds::topology::MetadataUsage>*
          spaces);

  virtual void GetTopology(pb::mds::topology::ListTopologyResponse* response);

  virtual void ListZone(pb::mds::topology::ListZoneResponse* response);

  virtual void ListServer(pb::mds::topology::ListServerResponse* response);

  virtual void ListMetaserverOfCluster(
      pb::mds::topology::ListMetaServerResponse* response);

  virtual void RegistMemcacheCluster(
      const pb::mds::topology::RegistMemcacheClusterRequest* request,
      pb::mds::topology::RegistMemcacheClusterResponse* response);

  virtual void ListMemcacheCluster(
      pb::mds::topology::ListMemcacheClusterResponse* response);

  virtual void AllocOrGetMemcacheCluster(
      const pb::mds::topology::AllocOrGetMemcacheClusterRequest* request,
      pb::mds::topology::AllocOrGetMemcacheClusterResponse* response);

 private:
  TopoStatusCode CreateEnoughCopyset(int32_t create_num);

  TopoStatusCode CreateCopyset(const CopysetCreateInfo& copyset);

  virtual void GetCopysetInfo(const uint32_t& pool_id,
                              const uint32_t& copyset_id,
                              pb::mds::topology::CopysetValue* copyset_value);

  virtual void ClearCopysetCreating(PoolIdType pool_id,
                                    CopySetIdType copyset_id);
  TopoStatusCode CreatePartitionOnCopyset(FsIdType fs_id,
                                          const CopySetInfo& copyset,
                                          pb::common::PartitionInfo* info);

  std::shared_ptr<Topology> topology_;
  std::shared_ptr<MetaserverClient> metaserverClient_;

  /**
   * @brief register mutex for metaserver, preventing duplicate registration
   *        in concurrent scenario
   */
  NameLock registMsMutex_;

  /**
   * @brief register mutex for memcachecluster,
   *           preventing duplicate registration
   *        in concurrent scenario
   */
  mutable RWLock registMemcacheClusterMutex_;

  NameLock createPartitionMutex_;

  /**
   * @brief topology options
   */
  TopologyOption option_;
};

}  // namespace topology
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_TOPOLOGY_TOPOLOGY_MANAGER_H_
