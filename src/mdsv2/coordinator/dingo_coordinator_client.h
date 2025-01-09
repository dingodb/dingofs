// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDSV2_DINGO_COORDINATOR_CLIENT_H_
#define DINGOFS_MDSV2_DINGO_COORDINATOR_CLIENT_H_

#include "dingosdk/client.h"
#include "dingosdk/coordinator.h"
#include "mdsv2/common/status.h"
#include "mdsv2/coordinator/coordinator_client.h"

namespace dingofs {
namespace mdsv2 {

class DingoCoordinatorClient : public CoordinatorClient {
 public:
  DingoCoordinatorClient() = default;
  ~DingoCoordinatorClient() override = default;

  static CoordinatorClientPtr New() { return std::make_shared<DingoCoordinatorClient>(); }

  bool Init(const std::string& addr) override;
  bool Destroy() override;

  Status MDSHeartbeat(const MDSMeta& mds) override;
  Status GetMDSList(std::vector<MDSMeta>& mdses) override;

  Status CreateAutoIncrement(int64_t table_id, int64_t start_id) override;
  Status DeleteAutoIncrement(int64_t table_id) override;
  Status UpdateAutoIncrement(int64_t table_id, int64_t start_id) override;
  Status GenerateAutoIncrement(int64_t table_id, int64_t count, int64_t& start_id, int64_t& end_id) override;
  Status GetAutoIncrement(int64_t table_id, int64_t& start_id) override;
  Status GetAutoIncrements(std::vector<AutoIncrement>& auto_increments) override;

 private:
  std::string coordinator_addr_;
  dingodb::sdk::Client* client_{nullptr};
  dingodb::sdk::Coordinator* coordinator_{nullptr};
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_DINGO_COORDINATOR_CLIENT_H_