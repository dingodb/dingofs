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

#include "mds/mds/mds_meta.h"

#include <cstdint>
#include <vector>

#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "json/value.h"
#include "mds/common/logging.h"

namespace dingofs {
namespace mds {

static MdsEntry::State ToPbMdsState(MDSMeta::State state) {
  switch (state) {
    case MDSMeta::State::kInit:
      return pb::mds::MDS_State_INIT;
    case MDSMeta::State::kNormal:
      return pb::mds::MDS_State_NORMAL;
    case MDSMeta::State::kAbnormal:
      return pb::mds::MDS_State_ABNORMAL;
    default:
      DINGO_LOG(FATAL) << "Unknown MDSMeta state: " << static_cast<int>(state);
      break;
  }

  return pb::mds::MDS_State_INIT;
}

static MDSMeta::State ToMdsState(MdsEntry::State state) {
  switch (state) {
    case pb::mds::MDS_State_INIT:
      return MDSMeta::State::kInit;
    case pb::mds::MDS_State_NORMAL:
      return MDSMeta::State::kNormal;
    case pb::mds::MDS_State_ABNORMAL:
      return MDSMeta::State::kAbnormal;
    default:
      DINGO_LOG(FATAL) << "Unknown MDS state: " << static_cast<int>(state);
      break;
  }

  return MDSMeta::State::kInit;
}

MDSMeta::MDSMeta(const MdsEntry& pb_mds) {
  id_ = pb_mds.id();
  host_ = pb_mds.location().host();
  port_ = pb_mds.location().port();
  state_ = ToMdsState(pb_mds.state());
  create_time_ms_ = pb_mds.create_time_ms();
  last_online_time_ms_ = pb_mds.last_online_time_ms();
}

MDSMeta::MDSMeta(const MDSMeta& mds_meta) {
  id_ = mds_meta.id_;
  host_ = mds_meta.host_;
  port_ = mds_meta.port_;
  state_ = mds_meta.state_;
  create_time_ms_ = mds_meta.create_time_ms_;
  last_online_time_ms_ = mds_meta.last_online_time_ms_;
}

std::string MDSMeta::ToString() const {
  return fmt::format("MDSMeta[id={}, host={}, port={}, state={}, create_time={} last_online_time_ms={}]", id_, host_,
                     port_, static_cast<int>(state_), create_time_ms_, last_online_time_ms_);
}

MdsEntry MDSMeta::ToProto() const {
  MdsEntry pb_mds;
  pb_mds.set_id(id_);
  pb_mds.mutable_location()->set_host(host_);
  pb_mds.mutable_location()->set_port(port_);
  pb_mds.set_state(ToPbMdsState(state_));
  pb_mds.mutable_location()->set_port(port_);
  pb_mds.set_create_time_ms(create_time_ms_);
  pb_mds.set_last_online_time_ms(last_online_time_ms_);

  return pb_mds;
}

void MDSMeta::DescribeByJson(Json::Value& value) {
  value["id"] = id_;
  value["host"] = host_;
  value["port"] = port_;
  value["state"] = StateName(state_);
  value["last_online_time_ms"] = last_online_time_ms_;
}

void MDSMetaMap::UpsertMDSMeta(const MDSMeta& mds_meta) {
  utils::WriteLockGuard lk(lock_);

  mds_meta_map_[mds_meta.ID()] = mds_meta;
}

void MDSMetaMap::DeleteMDSMeta(uint64_t mds_id) {
  utils::WriteLockGuard lk(lock_);

  mds_meta_map_.erase(mds_id);
}

bool MDSMetaMap::IsExistMDSMeta(uint64_t mds_id) {
  utils::ReadLockGuard lk(lock_);

  return mds_meta_map_.find(mds_id) != mds_meta_map_.end();
}

bool MDSMetaMap::IsNormalMDSMeta(uint64_t mds_id) {
  MDSMeta mds_meta;
  if (!GetMDSMeta(mds_id, mds_meta)) {
    return false;
  }

  return mds_meta.GetState() == MDSMeta::State::kNormal;
}

bool MDSMetaMap::GetMDSMeta(uint64_t mds_id, MDSMeta& mds_meta) {
  utils::ReadLockGuard lk(lock_);

  auto it = mds_meta_map_.find(mds_id);
  if (it == mds_meta_map_.end()) {
    return false;
  }

  mds_meta = it->second;
  return true;
}

std::vector<MDSMeta> MDSMetaMap::GetAllMDSMeta() {
  utils::ReadLockGuard lk(lock_);

  std::vector<MDSMeta> mds_metas;
  mds_metas.reserve(mds_meta_map_.size());
  for (const auto& [id, mds_meta] : mds_meta_map_) {
    mds_metas.push_back(mds_meta);
  }

  return mds_metas;
}

void MDSMetaMap::DescribeByJson(Json::Value& value) {
  utils::ReadLockGuard lk(lock_);

  for (auto& [id, mds_meta] : mds_meta_map_) {
    Json::Value mds_value;
    mds_meta.DescribeByJson(mds_value);
    value.append(mds_value);
  }
}

}  // namespace mds
}  // namespace dingofs
