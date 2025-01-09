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

#ifndef DINGOFS_TEST_MDS_SCHEDULE_COMMON_H_
#define DINGOFS_TEST_MDS_SCHEDULE_COMMON_H_

#include <map>
#include <vector>

#include "proto/heartbeat.pb.h"
#include "mds/schedule/operatorStep.h"
#include "mds/schedule/topoAdapter.h"

using ::dingofs::mds::schedule::AddPeer;
using ::dingofs::mds::schedule::CopySetConf;
using ::dingofs::mds::schedule::PeerInfo;
using ::dingofs::mds::schedule::RemovePeer;
using ::dingofs::mds::schedule::TransferLeader;
using ::dingofs::mds::topology::CopySetIdType;
using ::dingofs::mds::topology::EpochType;
using ::dingofs::mds::topology::MetaServerIdType;
using ::dingofs::mds::topology::PoolIdType;
using ::dingofs::mds::topology::ServerIdType;

using ::dingofs::pb::mds::heartbeat::CandidateError;

namespace dingofs {
namespace mds {
namespace schedule {
::dingofs::mds::schedule::CopySetInfo GetCopySetInfoForTest();
void GetCopySetInMetaServersForTest(
    std::map<MetaServerIdType, std::vector<CopySetInfo>>* out);
::dingofs::mds::topology::CopySetInfo GetTopoCopySetInfoForTest();
std::vector<::dingofs::mds::topology::MetaServer> GetTopoMetaServerForTest();
std::vector<::dingofs::mds::topology::Server> GetServerForTest();
::dingofs::mds::topology::Pool GetPoolForTest();
}  // namespace schedule
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_TEST_MDS_SCHEDULE_COMMON_H_
