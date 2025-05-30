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

#ifndef DINGOFS_MDSV2_MDS_MDS_META_H_
#define DINGOFS_MDSV2_MDS_MDS_META_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mdsv2 {

class MDSMetaMap;
using MDSMetaMapSPtr = std::shared_ptr<MDSMetaMap>;

class MDSMeta {
 public:
  MDSMeta() = default;
  ~MDSMeta() = default;

  MDSMeta(const pb::mdsv2::MDS& pb_mds);
  MDSMeta(const MDSMeta& mds_meta);
  MDSMeta& operator=(const MDSMeta& mds_meta) = default;

  enum State {
    kInit = 0,
    kNormal = 1,
    kAbnormal = 2,
  };

  static std::string StateName(State state) {
    switch (state) {
      case State::kInit:
        return "INIT";
      case State::kNormal:
        return "NORMAL";
      case State::kAbnormal:
        return "ABNORMAL";
      default:
        return "UNKNOWN";
    }
  }

  int64_t ID() const { return id_; }
  void SetID(int64_t id) { id_ = id; }

  std::string Host() const { return host_; }
  void SetHost(const std::string& host) { host_ = host; }

  int Port() const { return port_; }
  void SetPort(int port) { port_ = port; }

  State GetState() const { return state_; }
  void SetState(State state) { state_ = state; }

  uint64_t LastOnlineTimeMs() const { return last_online_time_ms_; }
  void SetLastOnlineTimeMs(uint64_t time_ms) { last_online_time_ms_ = time_ms; }

  std::string ToString() const;
  pb::mdsv2::MDS ToProto() const;

 private:
  int64_t id_{0};

  std::string host_;
  int port_{0};

  State state_;

  uint64_t last_online_time_ms_;
};

class MDSMetaMap {
 public:
  MDSMetaMap() = default;
  ~MDSMetaMap() = default;

  MDSMetaMap(const MDSMetaMap& mds_meta_map) = delete;
  MDSMetaMap& operator=(const MDSMetaMap& mds_meta_map) = delete;

  static MDSMetaMapSPtr New() { return std::make_shared<MDSMetaMap>(); }

  void UpsertMDSMeta(const MDSMeta& mds_meta);
  void DeleteMDSMeta(int64_t id);

  bool GetMDSMeta(int64_t id, MDSMeta& mds_meta);
  std::vector<MDSMeta> GetAllMDSMeta();

 private:
  utils::RWLock lock_;
  std::map<int64_t, MDSMeta> mds_meta_map_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_MDS_MDS_META_H_