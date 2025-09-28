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

#include "mds/coordinator/dummy_coordinator_client.h"

#include <fmt/format.h>

#include "bthread/mutex.h"
#include "dingofs/error.pb.h"
#include "fmt/core.h"
#include "mds/common/logging.h"

namespace dingofs {
namespace mds {

DummyCoordinatorClient::DummyCoordinatorClient() {
  CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "init mutex fail.";
}

DummyCoordinatorClient::~DummyCoordinatorClient() {
  CHECK(bthread_mutex_destroy(&mutex_) == 0) << "destory mutex fail.";
}

bool DummyCoordinatorClient::Init(const std::string& addr) {
  DINGO_LOG(INFO) << fmt::format("init dummy coordinator client, addr({}).", addr);

  return true;
}
bool DummyCoordinatorClient::Destroy() { return true; }

Status DummyCoordinatorClient::MDSHeartbeat(const MDSMeta& mds, std::vector<MDSMeta>& out_mdses) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it =
      std::find_if(mdses_.begin(), mdses_.end(), [&mds](const MDSMeta& mds_meta) { return mds_meta.ID() == mds.ID(); });
  if (it == mdses_.end()) {
    mdses_.push_back(mds);
  } else {
    *it = mds;
  }

  out_mdses = mdses_;

  return Status::OK();
}

Status DummyCoordinatorClient::GetMDSList(std::vector<MDSMeta>& mdses) {
  BAIDU_SCOPED_LOCK(mutex_);

  mdses = mdses_;
  return Status::OK();
}

Status DummyCoordinatorClient::CreateAutoIncrement(int64_t table_id, int64_t start_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (auto_increments_.find(table_id) != auto_increments_.end()) {
    return Status(pb::error::EEXISTED, fmt::format("table({}) auto increament already exist", table_id));
  }

  auto_increments_[table_id] = AutoIncrement{table_id, start_id, start_id};

  return Status::OK();
}

Status DummyCoordinatorClient::DeleteAutoIncrement(int64_t table_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto_increments_.erase(table_id);
  return Status::OK();
}

Status DummyCoordinatorClient::UpdateAutoIncrement(int64_t table_id, int64_t start_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = auto_increments_.find(table_id);
  if (it == auto_increments_.end()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("table({}) auto increament not exist", table_id));
  }

  it->second.start_id = start_id;

  return Status::OK();
}

Status DummyCoordinatorClient::GenerateAutoIncrement(int64_t table_id, int64_t count, int64_t& start_id,
                                                     int64_t& end_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = auto_increments_.find(table_id);
  if (it == auto_increments_.end()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("table({}) auto increament not exist", table_id));
  }

  auto& auto_increment = it->second;

  start_id = auto_increment.alloc_id;
  end_id = auto_increment.alloc_id + count;
  auto_increment.alloc_id += count;

  return Status::OK();
}

Status DummyCoordinatorClient::GetAutoIncrement(int64_t table_id, int64_t& start_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = auto_increments_.find(table_id);
  if (it == auto_increments_.end()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("table({}) auto increament not exist", table_id));
  }

  start_id = it->second.start_id;
  return Status::OK();
}

Status DummyCoordinatorClient::GetAutoIncrements(std::vector<AutoIncrement>& auto_increments) {
  BAIDU_SCOPED_LOCK(mutex_);

  for (const auto& [table_id, auto_increment] : auto_increments_) {
    auto_increments.push_back(auto_increment);
  }

  return Status::OK();
}

// version
Status DummyCoordinatorClient::KvRange(const Options& options, const Range& range, int64_t limit,
                                       std::vector<KVWithExt>& out_kvs, bool& out_more, int64_t& out_count) {
  return Status(pb::error::ENOT_SUPPORT, "not implemented");
}

Status DummyCoordinatorClient::KvPut(const Options& options, const KVPair& kv, KVWithExt& out_prev_kv) {
  return Status(pb::error::ENOT_SUPPORT, "not implemented");
}

Status DummyCoordinatorClient::KvDeleteRange(const Options& options, const Range& range, int64_t& out_deleted,
                                             std::vector<KVWithExt>& out_prev_kvs) {
  return Status(pb::error::ENOT_SUPPORT, "not implemented");
}

Status DummyCoordinatorClient::KvCompaction(const Range& range, int64_t revision, int64_t& out_count) {
  return Status(pb::error::ENOT_SUPPORT, "not implemented");
}

Status DummyCoordinatorClient::LeaseGrant(int64_t id, int64_t ttl, int64_t& out_id, int64_t& out_ttl) {
  return Status(pb::error::ENOT_SUPPORT, "not implemented");
}

Status DummyCoordinatorClient::LeaseRevoke(int64_t id) { return Status(pb::error::ENOT_SUPPORT, "not implemented"); }

Status DummyCoordinatorClient::LeaseRenew(int64_t id, int64_t& out_ttl) {
  return Status(pb::error::ENOT_SUPPORT, "not implemented");
}

Status DummyCoordinatorClient::LeaseQuery(int64_t id, bool is_get_key, int64_t& out_ttl, int64_t& out_granted_ttl,
                                          std::vector<std::string>& out_keys) {
  return Status(pb::error::ENOT_SUPPORT, "not implemented");
}

Status DummyCoordinatorClient::ListLeases(std::vector<int64_t>& out_ids) {
  return Status(pb::error::ENOT_SUPPORT, "not implemented");
}

Status DummyCoordinatorClient::Watch(const std::string& key, int64_t start_revision, WatchOut& out) {
  return Status(pb::error::ENOT_SUPPORT, "not implemented");
}

}  // namespace mds
}  // namespace dingofs