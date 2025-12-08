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

#ifndef DINGOFS_MDS_MDS_META_H_
#define DINGOFS_MDS_MDS_META_H_

#include <sys/types.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "json/value.h"
#include "mds/common/type.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mds {

class MDSMetaMap;
using MDSMetaMapSPtr = std::shared_ptr<MDSMetaMap>;

class MDSMeta {
 public:
  MDSMeta() = default;
  ~MDSMeta() = default;

  MDSMeta(const MdsEntry& pb_mds);
  MDSMeta(const MDSMeta& mds_meta);
  MDSMeta& operator=(const MDSMeta& mds_meta) = default;

  enum State : uint8_t {
    kInit = 0,
    kNormal = 1,
    kAbnormal = 2,
    kStop = 3,
  };

  static std::string StateName(State state) {
    switch (state) {
      case State::kInit:
        return "INIT";
      case State::kNormal:
        return "NORMAL";
      case State::kAbnormal:
        return "ABNORMAL";
      case State::kStop:
        return "STOP";
      default:
        return "UNKNOWN";
    }
  }

  uint64_t ID() const { return id_; }
  void SetID(uint64_t id) { id_ = id; }

  std::string Host() const { return host_; }
  void SetHost(const std::string& host) { host_ = host; }

  int Port() const { return port_; }
  void SetPort(int port) { port_ = port; }

  State GetState() const { return state_; }
  void SetState(State state) { state_ = state; }

  uint64_t CreateTimeMs() const { return create_time_ms_; }
  void SetCreateTimeMs(uint64_t time_ms) { create_time_ms_ = time_ms; }

  uint64_t LastOnlineTimeMs() const { return last_online_time_ms_; }
  void SetLastOnlineTimeMs(uint64_t time_ms) { last_online_time_ms_ = time_ms; }

  std::string ToString() const;
  MdsEntry ToProto() const;

  void DescribeByJson(Json::Value& value);

 private:
  uint64_t id_{0};

  std::string host_;
  int port_{0};

  State state_;

  uint64_t create_time_ms_{0};
  uint64_t last_online_time_ms_{0};
};

class MDSMetaMap {
 public:
  MDSMetaMap() = default;
  ~MDSMetaMap() = default;

  MDSMetaMap(const MDSMetaMap& mds_meta_map) = delete;
  MDSMetaMap& operator=(const MDSMetaMap& mds_meta_map) = delete;

  static MDSMetaMapSPtr New() { return std::make_shared<MDSMetaMap>(); }

  void UpsertMDSMeta(const MDSMeta& mds_meta);
  void DeleteMDSMeta(uint64_t mds_id);

  bool IsExistMDSMeta(uint64_t mds_id);
  bool IsNormalMDSMeta(uint64_t mds_id);

  bool GetMDSMeta(uint64_t mds_id, MDSMeta& mds_meta);
  std::vector<MDSMeta> GetAllMDSMeta();

  void DescribeByJson(Json::Value& value);

 private:
  utils::RWLock lock_;
  // mds id -> MDSMeta
  std::map<uint64_t, MDSMeta> mds_meta_map_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_MDS_META_H_