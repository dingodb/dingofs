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
 * @Date: 2021-11-15 11:01:48
 * @Author: chenwei
 */

#include "mds/schedule/common.h"

#include <sys/time.h>

#include <set>
#include <string>
#include <vector>

#include "proto/topology.pb.h"
#include "utils/timeutility.h"

namespace dingofs {
namespace mds {
namespace schedule {
::dingofs::mds::schedule::CopySetInfo GetCopySetInfoForTest() {
  PoolIdType pooId = 1;
  CopySetIdType copysetId = 1;
  CopySetKey copySetKey;
  copySetKey.first = pooId;
  copySetKey.second = copysetId;
  EpochType epoch = 1;
  MetaServerIdType leader = 1;
  PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
  PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
  PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
  std::vector<PeerInfo> peers({peer1, peer2, peer3});

  return ::dingofs::mds::schedule::CopySetInfo(copySetKey, epoch, leader, peers,
                                               ConfigChangeInfo{});
}

void GetCopySetInMetaServersForTest(
    std::map<MetaServerIdType, std::vector<CopySetInfo>>* out) {
  PoolIdType poolId = 1;
  for (int i = 1; i <= 10; i++) {
    CopySetIdType copysetId = i;
    CopySetKey copySetKey;
    copySetKey.first = poolId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    MetaServerIdType leader = i;
    PeerInfo peer1(i, 1, 1, "192.168.10.1", 9000 + i);
    PeerInfo peer2(i + 1, 2, 2, "192.168.10.2", 9000 + i + 1);
    PeerInfo peer3(i + 2, 3, 3, "192.168.10.3", 9000 + i + 2);
    std::vector<PeerInfo> peers({peer1, peer2, peer3});
    ::dingofs::mds::schedule::CopySetInfo copyset(copySetKey, epoch, leader,
                                                  peers, ConfigChangeInfo{});
    (*out)[i].emplace_back(copyset);
    (*out)[i + 1].emplace_back(copyset);
    (*out)[i + 2].emplace_back(copyset);
  }
}

::dingofs::mds::topology::CopySetInfo GetTopoCopySetInfoForTest() {
  ::dingofs::mds::topology::CopySetInfo topoCopySet(1, 1);
  topoCopySet.SetEpoch(1);
  topoCopySet.SetLeader(1);
  topoCopySet.SetCopySetMembers(std::set<MetaServerIdType>{1, 2, 3});
  topoCopySet.SetCandidate(4);
  return topoCopySet;
}
std::vector<::dingofs::mds::topology::MetaServer> GetTopoMetaServerForTest() {
  std::vector<::dingofs::mds::topology::MetaServer> out;
  for (int i = 1; i <= 4; i++) {
    std::string ip = "192.168.10." + std::to_string(i);
    ::dingofs::mds::topology::MetaServer metaserver(i, "hostname", "token", i,
                                                    ip, 9000, ip, 9000);
    metaserver.SetMetaServerSpace(::dingofs::mds::topology::MetaServerSpace{});
    metaserver.SetStartUpTime(::dingofs::utils::TimeUtility::GetTimeofDaySec());
    out.emplace_back(metaserver);
  }
  return out;
}
std::vector<::dingofs::mds::topology::Server> GetServerForTest() {
  std::vector<::dingofs::mds::topology::Server> out;
  for (int i = 1; i <= 4; i++) {
    std::string ip = "192.168.10." + std::to_string(i);
    ::dingofs::mds::topology::Server server(i, "server", ip, 0, "", 0, i, 1);
    out.emplace_back(server);
  }
  return out;
}
::dingofs::mds::topology::Pool GetPoolForTest() {
  ::dingofs::mds::topology::Pool::RedundanceAndPlaceMentPolicy policy;
  policy.replicaNum = 3;
  policy.zoneNum = 3;
  policy.copysetNum = 10;

  ::dingofs::mds::topology::Pool pool(1, "poolname", policy, 1000);
  return pool;
}
}  // namespace schedule
}  // namespace mds
}  // namespace dingofs
