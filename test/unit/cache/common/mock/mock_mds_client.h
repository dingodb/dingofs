/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2026-02-02
 * Author: AI
 */

#ifndef DINGOFS_TEST_CACHE_COMMON_MOCK_MDS_CLIENT_H_
#define DINGOFS_TEST_CACHE_COMMON_MOCK_MDS_CLIENT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cache/common/mds_client.h"

namespace dingofs {
namespace cache {

class MockMDSClient : public MDSClient {
 public:
  MockMDSClient() = default;
  ~MockMDSClient() override = default;

  MOCK_METHOD(Status, Start, (), (override));
  MOCK_METHOD(Status, Shutdown, (), (override));

  MOCK_METHOD(Status, GetFSInfo, (uint64_t fs_id, pb::mds::FsInfo* fs_info),
              (override));

  MOCK_METHOD(Status, JoinCacheGroup,
              (const std::string& member_id, const std::string& ip,
               uint32_t port, const std::string& group_name, uint32_t weight),
              (override));

  MOCK_METHOD(Status, LeaveCacheGroup,
              (const std::string& member_id, const std::string& ip,
               uint32_t port, const std::string& group_name),
              (override));

  MOCK_METHOD(Status, Heartbeat,
              (const std::string& member_id, const std::string& ip,
               uint32_t port),
              (override));

  MOCK_METHOD(Status, ListMembers,
              (const std::string& group_name,
               std::vector<CacheGroupMember>* members),
              (override));
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_CACHE_COMMON_MOCK_MDS_CLIENT_H_
