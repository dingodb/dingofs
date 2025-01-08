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
 * @Date: 2021-11-12 11:26:50
 * @Author: chenwei
 */

#ifndef DINGOFS_TEST_MDS_MOCK_MOCK_COORDINATOR_H_
#define DINGOFS_TEST_MDS_MOCK_MOCK_COORDINATOR_H_

#include <gmock/gmock.h>

#include <map>
#include <vector>

#include "mds/schedule/coordinator.h"

namespace dingofs {
namespace mds {

using ::dingofs::mds::topology::CopySetKey;
using ::dingofs::mds::topology::MetaServerIdType;
using ::dingofs::mds::topology::PoolIdType;

class MockCoordinator : public ::dingofs::mds::schedule::Coordinator {
 public:
  MockCoordinator() = default;
  ~MockCoordinator() override = default;

  MOCK_METHOD3(
      CopySetHeartbeat,
      MetaServerIdType(const ::dingofs::mds::topology::CopySetInfo& originInfo,
                       const pb::mds::heartbeat::ConfigChangeInfo& configChInfo,
                       pb::mds::heartbeat::CopySetConf* newConf));

  MOCK_METHOD2(MetaserverGoingToAdd, bool(MetaServerIdType, CopySetKey));

  MOCK_METHOD2(QueryMetaServerRecoverStatus,
               pb::mds::schedule::ScheduleStatusCode(
                   const std::vector<MetaServerIdType>&,
                   std::map<MetaServerIdType, bool>*));
};
}  // namespace mds
}  // namespace dingofs
#endif  // DINGOFS_TEST_MDS_MOCK_MOCK_COORDINATOR_H_
