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

#include "mdsv2/mds/mds_meta.h"

#include <cstdint>
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "mdsv2/common/logging.h"

namespace dingofs {
namespace mdsv2 {

static pb::mdsv2::MDS::State ToPbMdsState(MDSMeta::State state) {
  switch (state) {
    case MDSMeta::State::kInit:
      return pb::mdsv2::MDS_State_INIT;
    case MDSMeta::State::kNormal:
      return pb::mdsv2::MDS_State_NORMAL;
    case MDSMeta::State::kAbnormal:
      return pb::mdsv2::MDS_State_ABNORMAL;
    default:
      DINGO_LOG(FATAL) << "Unknown MDSMeta state: " << static_cast<int>(state);
      break;
  }

  return pb::mdsv2::MDS_State_INIT;
}

static MDSMeta::State ToMdsState(pb::mdsv2::MDS::State state) {
  switch (state) {
    case pb::mdsv2::MDS_State_INIT:
      return MDSMeta::State::kInit;
    case pb::mdsv2::MDS_State_NORMAL:
      return MDSMeta::State::kNormal;
    case pb::mdsv2::MDS_State_ABNORMAL:
      return MDSMeta::State::kAbnormal;
    default:
      DINGO_LOG(FATAL) << "Unknown MDS state: " << static_cast<int>(state);
      break;
  }

  return MDSMeta::State::kInit;
}

MDSMeta::MDSMeta(const pb::mdsv2::MDS& pb_mds) {
  id_ = pb_mds.id();
  host_ = pb_mds.location().host();
  port_ = pb_mds.location().port();
  state_ = ToMdsState(pb_mds.state());
  register_time_ms_ = pb_mds.register_time_ms();
  last_online_time_ms_ = pb_mds.last_online_time_ms();
}

MDSMeta::MDSMeta(const MDSMeta& mds_meta) {
  id_ = mds_meta.id_;
  host_ = mds_meta.host_;
  port_ = mds_meta.port_;
  state_ = mds_meta.state_;
  register_time_ms_ = mds_meta.register_time_ms_;
  last_online_time_ms_ = mds_meta.last_online_time_ms_;
}

std::string MDSMeta::ToString() const {
  return fmt::format("MDSMeta[id={}, host={}, port={}, state={}, register_time_ms={}, last_online_time_ms={}]", id_,
                     host_, port_, static_cast<int>(state_), register_time_ms_, last_online_time_ms_);
}

pb::mdsv2::MDS MDSMeta::ToProto() const {
  pb::mdsv2::MDS pb_mds;
  pb_mds.set_id(id_);
  pb_mds.mutable_location()->set_host(host_);
  pb_mds.mutable_location()->set_port(port_);
  pb_mds.set_state(ToPbMdsState(state_));
  pb_mds.set_register_time_ms(register_time_ms_);
  pb_mds.set_last_online_time_ms(last_online_time_ms_);

  return std::move(pb_mds);
}

void MDSMetaMap::UpsertMDSMeta(const MDSMeta& mds_meta) {
  utils::WriteLockGuard lk(lock_);

  mds_meta_map_[mds_meta.ID()] = mds_meta;
}

void MDSMetaMap::DeleteMDSMeta(int64_t id) {
  utils::WriteLockGuard lk(lock_);

  mds_meta_map_.erase(id);
}

bool MDSMetaMap::GetMDSMeta(int64_t id, MDSMeta& mds_meta) {
  utils::ReadLockGuard lk(lock_);

  auto it = mds_meta_map_.find(id);
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

}  // namespace mdsv2
}  // namespace dingofs
