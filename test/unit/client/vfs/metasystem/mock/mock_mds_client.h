/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_TEST_UNIT_CLIENT_VFS_METASYSTEM_MOCK_MDS_CLIENT_H_
#define DINGOFS_TEST_UNIT_CLIENT_VFS_METASYSTEM_MOCK_MDS_CLIENT_H_

#include <gmock/gmock.h>

#include "client/vfs/metasystem/mds/mds_client.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {
namespace test {

class MockMDSClient : public MDSClient {
 public:
  MockMDSClient(const ClientId& client_id, mds::FsInfo& fs_info, RPC&& rpc,
                TraceManager& trace_manager)
      : MDSClient(client_id, fs_info, std::move(rpc), trace_manager) {}

  MOCK_METHOD(Status, ReadDir,
              (ContextSPtr& ctx, Ino ino, uint64_t fh,
               const std::string& last_name, uint32_t limit, bool with_attr,
               std::vector<DirEntry>& entries),
              (override));
};

}  // namespace test
}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_CLIENT_VFS_METASYSTEM_MOCK_MDS_CLIENT_H_
