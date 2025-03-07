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
 * @Date: 2021-11-10 11:04:24
 * @Author: chenwei
 */
#ifndef DINGOFS_TEST_MDS_MOCK_MOCK_TOPOADAPTER_H_
#define DINGOFS_TEST_MDS_MOCK_MOCK_TOPOADAPTER_H_

#include <gmock/gmock.h>

#include <list>
#include <map>
#include <set>
#include <vector>

#include "mds/schedule/topoAdapter.h"

namespace dingofs {
namespace mds {
namespace schedule {
class MockTopoAdapter : public TopoAdapter {
 public:
  MockTopoAdapter() {}
  ~MockTopoAdapter() {}

  MOCK_METHOD2(GetCopySetInfo, bool(const CopySetKey& id, CopySetInfo* info));

  MOCK_METHOD0(GetCopySetInfos, std::vector<CopySetInfo>());

  MOCK_METHOD1(GetCopySetInfosInMetaServer,
               std::vector<CopySetInfo>(MetaServerIdType));

  MOCK_METHOD2(GetMetaServerInfo,
               bool(MetaServerIdType id, MetaServerInfo* info));

  MOCK_METHOD0(GetMetaServerInfos, std::vector<MetaServerInfo>());

  MOCK_METHOD1(GetStandardZoneNumInPool, uint16_t(PoolIdType id));

  MOCK_METHOD1(GetStandardReplicaNumInPool, uint16_t(PoolIdType id));

  MOCK_METHOD1(GetAvgScatterWidthInPool, int(PoolIdType id));

  MOCK_METHOD2(CreateCopySetAtMetaServer,
               bool(CopySetKey id, MetaServerIdType csID));

  MOCK_METHOD2(CopySetFromTopoToSchedule,
               bool(const ::dingofs::mds::topology::CopySetInfo& origin,
                    ::dingofs::mds::schedule::CopySetInfo* out));

  MOCK_METHOD2(MetaServerFromTopoToSchedule,
               bool(const ::dingofs::mds::topology::MetaServer& origin,
                    ::dingofs::mds::schedule::MetaServerInfo* out));

  MOCK_METHOD0(Getpools, std::vector<PoolIdType>());

  MOCK_METHOD2(GetPool,
               bool(PoolIdType id, ::dingofs::mds::topology::Pool* pool));

  MOCK_METHOD1(GetCopySetInfosInPool, std::vector<CopySetInfo>(PoolIdType));

  MOCK_METHOD1(GetMetaServersInZone,
               std::vector<MetaServerInfo>(ZoneIdType zoneId));

  MOCK_METHOD1(GetZoneInPool, std::list<ZoneIdType>(PoolIdType poolId));

  MOCK_METHOD1(GetMetaServersInPool, std::vector<MetaServerInfo>(PoolIdType));

  MOCK_METHOD4(ChooseNewMetaServerForCopyset,
               bool(PoolIdType poolId, const std::set<ZoneIdType>& excludeZones,
                    const std::set<MetaServerIdType>& excludeMetaservers,
                    MetaServerIdType* target));
};
}  // namespace schedule
}  // namespace mds
}  // namespace dingofs
#endif  // DINGOFS_TEST_MDS_MOCK_MOCK_TOPOADAPTER_H_
