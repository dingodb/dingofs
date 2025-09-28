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

#ifndef DINGOFS_MDS_COORDINATOR_CLIENT_H_
#define DINGOFS_MDS_COORDINATOR_CLIENT_H_

#include <cstdint>
#include <memory>
#include <string>

#include "mds/common/status.h"
#include "mds/mds/mds_meta.h"

namespace dingofs {
namespace mds {

class CoordinatorClient {
 public:
  struct AutoIncrement {
    int64_t table_id{0};
    int64_t start_id{0};
    int64_t alloc_id{0};
  };

  CoordinatorClient() = default;
  virtual ~CoordinatorClient() = default;

  virtual bool Init(const std::string& addr) = 0;
  virtual bool Destroy() = 0;

  virtual std::string GetAddr() = 0;

  virtual Status MDSHeartbeat(const MDSMeta& mds, std::vector<MDSMeta>& out_mdses) = 0;
  virtual Status GetMDSList(std::vector<MDSMeta>& mdses) = 0;

  virtual Status CreateAutoIncrement(int64_t table_id, int64_t start_id) = 0;
  virtual Status DeleteAutoIncrement(int64_t table_id) = 0;
  virtual Status UpdateAutoIncrement(int64_t table_id, int64_t start_id) = 0;
  virtual Status GenerateAutoIncrement(int64_t table_id, int64_t count, int64_t& start_id, int64_t& end_id) = 0;
  virtual Status GetAutoIncrement(int64_t table_id, int64_t& start_id) = 0;
  virtual Status GetAutoIncrements(std::vector<AutoIncrement>& auto_increments) = 0;

  // version

  struct Range {
    std::string start;
    std::string end;
  };

  struct KVPair {
    std::string key;
    std::string value;
  };

  struct KVWithExt {
    KVPair kv;
    int64_t create_revision;
    int64_t mod_revision;
    int64_t version;
    int64_t lease;
  };

  struct Options {
    bool need_prev_kv{false};
    bool ignore_value{false};
    bool ignore_lease{false};
    bool keys_only{false};
    bool count_only{false};
    int64_t lease_id{0};
  };

  enum EventType : uint8_t {
    kNone = 0,
    kPut = 1,
    kDelete = 2,
    kNotExists = 3,
  };

  static std::string EventTypeName(EventType type) {
    switch (type) {
      case kNone:
        return "None";
      case kPut:
        return "Put";
      case kDelete:
        return "Delete";
      case kNotExists:
        return "NotExists";
      default:
        return "Unknown";
    }
  }

  struct Event {
    EventType type;
    KVWithExt kv;
    KVWithExt prev_kv;
  };

  struct WatchOut {
    std::vector<Event> events;
  };

  virtual Status KvRange(const Options& options, const Range& range, int64_t limit, std::vector<KVWithExt>& out_kvs,
                         bool& out_more, int64_t& out_count) = 0;
  virtual Status KvPut(const Options& options, const KVPair& kv, KVWithExt& out_prev_kv) = 0;
  virtual Status KvDeleteRange(const Options& options, const Range& range, int64_t& out_deleted,
                               std::vector<KVWithExt>& out_prev_kvs) = 0;
  virtual Status KvCompaction(const Range& range, int64_t revision, int64_t& out_count) = 0;

  virtual Status LeaseGrant(int64_t id, int64_t ttl, int64_t& out_id, int64_t& out_ttl) = 0;
  virtual Status LeaseRevoke(int64_t id) = 0;
  virtual Status LeaseRenew(int64_t id, int64_t& out_ttl) = 0;
  virtual Status LeaseQuery(int64_t id, bool is_get_key, int64_t& out_ttl, int64_t& out_granted_ttl,
                            std::vector<std::string>& out_keys) = 0;
  virtual Status ListLeases(std::vector<int64_t>& out_ids) = 0;
  virtual Status Watch(const std::string& key, int64_t start_revision, WatchOut& out) = 0;
};

using CoordinatorClientSPtr = std::shared_ptr<CoordinatorClient>;

}  // namespace mds

}  // namespace dingofs

#endif  // DINGOFS_MDS_COORDINATOR_CLIENT_H_
