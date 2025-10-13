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

#ifndef DINGOFS_MDS_DINGO_COORDINATOR_CLIENT_H_
#define DINGOFS_MDS_DINGO_COORDINATOR_CLIENT_H_

#include "dingosdk/client.h"
#include "dingosdk/coordinator.h"
#include "dingosdk/version.h"
#include "mds/common/status.h"
#include "mds/coordinator/coordinator_client.h"

namespace dingofs {
namespace mds {

class DingoCoordinatorClient : public CoordinatorClient {
 public:
  DingoCoordinatorClient() = default;
  ~DingoCoordinatorClient() override = default;

  static CoordinatorClientSPtr New() { return std::make_shared<DingoCoordinatorClient>(); }

  bool Init(const std::string& addr) override;
  bool Destroy() override;

  std::string GetAddr() override { return coordinator_addr_; }

  Status MDSHeartbeat(const MDSMeta& mds, std::vector<MDSMeta>& out_mdses) override;
  Status GetMDSList(std::vector<MDSMeta>& mdses) override;

  // auto increment
  Status CreateAutoIncrement(int64_t table_id, int64_t start_id) override;
  Status DeleteAutoIncrement(int64_t table_id) override;
  Status UpdateAutoIncrement(int64_t table_id, int64_t start_id) override;
  Status GenerateAutoIncrement(int64_t table_id, int64_t count, int64_t& start_id, int64_t& end_id) override;
  Status GetAutoIncrement(int64_t table_id, int64_t& start_id) override;
  Status GetAutoIncrements(std::vector<AutoIncrement>& auto_increments) override;

  // version
  Status KvRange(const Options& options, const Range& range, int64_t limit, std::vector<KVWithExt>& out_kvs,
                 bool& out_more, int64_t& out_count) override;
  Status KvPut(const Options& options, const KVPair& kv, KVWithExt& out_prev_kv) override;
  Status KvDeleteRange(const Options& options, const Range& range, int64_t& out_deleted,
                       std::vector<KVWithExt>& out_prev_kvs) override;

  Status KvCompaction(const Range& range, int64_t revision, int64_t& out_count) override;

  Status LeaseGrant(int64_t id, int64_t ttl, int64_t& out_id, int64_t& out_ttl) override;
  Status LeaseRevoke(int64_t id) override;
  Status LeaseRenew(int64_t id, int64_t& out_ttl) override;
  Status LeaseQuery(int64_t id, bool is_get_key, int64_t& out_ttl, int64_t& out_granted_ttl,
                    std::vector<std::string>& out_keys) override;
  Status ListLeases(std::vector<int64_t>& out_ids) override;
  Status Watch(const std::string& key, int64_t start_revision, WatchOut& out) override;

 private:
  std::string coordinator_addr_;
  dingodb::sdk::Client* client_{nullptr};
  dingodb::sdk::Coordinator* coordinator_{nullptr};
  dingodb::sdk::Version* version_{nullptr};
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_DINGO_COORDINATOR_CLIENT_H_