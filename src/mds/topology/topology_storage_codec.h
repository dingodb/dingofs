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
 * Created Date: 2021-08-25
 * Author: wanghai01
 */

#ifndef DINGOFS_SRC_MDS_TOPOLOGY_TOPOLOGY_STORAGE_CODEC_H_
#define DINGOFS_SRC_MDS_TOPOLOGY_TOPOLOGY_STORAGE_CODEC_H_

#include <string>

#include "mds/common/mds_define.h"
#include "mds/common/storage_key.h"
#include "mds/topology/topology_item.h"
#include "utils/encode.h"

namespace dingofs {
namespace mds {
namespace topology {

using dingofs::utils::EncodeBigEndian;
using dingofs::mds::CLUSTERINFOKEY;
using dingofs::mds::COPYSETKEYEND;
using dingofs::mds::COPYSETKEYPREFIX;
using dingofs::mds::METASERVERKEYEND;
using dingofs::mds::METASERVERKEYPREFIX;
using dingofs::mds::POOLKEYEND;
using dingofs::mds::POOLKEYPREFIX;
using dingofs::mds::SERVERKEYEND;
using dingofs::mds::SERVERKEYPREFIX;
using dingofs::mds::ZONEKEYEND;
using dingofs::mds::ZONEKEYPREFIX;

class TopologyStorageCodec {
 public:
  // there are three types of function here:
  // Encode__Key: attach item id to item prefix
  // Encode__Data: convert data structure to a string
  // Decode__Data: convert a string to data structure
  std::string EncodePoolKey(PoolIdType id);
  bool EncodePoolData(const Pool& data, std::string* value);
  bool DecodePoolData(const std::string& value, Pool* data);

  std::string EncodeZoneKey(ZoneIdType id);
  bool EncodeZoneData(const Zone& data, std::string* value);
  bool DecodeZoneData(const std::string& value, Zone* data);

  std::string EncodeServerKey(ServerIdType id);
  bool EncodeServerData(const Server& data, std::string* value);
  bool DecodeServerData(const std::string& value, Server* data);

  std::string EncodeMetaServerKey(MetaServerIdType id);
  bool EncodeMetaServerData(const MetaServer& data, std::string* value);
  bool DecodeMetaServerData(const std::string& value, MetaServer* data);

  std::string EncodeCopySetKey(const CopySetKey& id);
  bool EncodeCopySetData(const CopySetInfo& data, std::string* value);
  bool DecodeCopySetData(const std::string& value, CopySetInfo* data);

  std::string EncodePartitionKey(PartitionIdType id);
  bool EncodePartitionData(const Partition& data, std::string* value);
  bool DecodePartitionData(const std::string& value, Partition* data);

  bool EncodeClusterInfoData(const ClusterInformation& data,
                             std::string* value);
  bool DecodeClusterInfoData(const std::string& value,
                             ClusterInformation* data);

  std::string EncodeMemcacheClusterKey(MetaServerIdType id);
  bool EncodeMemcacheClusterData(const MemcacheCluster& data,
                                 std::string* value);
  bool DecodeMemcacheClusterData(const std::string& value,
                                 MemcacheCluster* data);

  std::string EncodeFs2MemcacheClusterKey(FsIdType fsId);
  bool DecodeFs2MemcacheClusterKey(const std::string& value, FsIdType* data);
};

}  // namespace topology
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_TOPOLOGY_TOPOLOGY_STORAGE_CODEC_H_
