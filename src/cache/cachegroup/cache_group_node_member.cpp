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

/*
 * Project: DingoFS
 * Created Date: 2025-03-17
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/cache_group_node_member.h"

#include <glog/logging.h>

#include "base/time/time.h"
#include "cache/utils/local_filesystem.h"
#include "common/status.h"
#include "dingofs/cachegroup.pb.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using dingofs::base::time::TimeNow;
using dingofs::cache::utils::LocalFileSystem;
using dingofs::pb::mds::cachegroup::CacheGroupNodeMetadata;
using dingofs::pb::mds::cachegroup::CacheGroupOk;

CacheGroupNodeMemberImpl::CacheGroupNodeMemberImpl(
    CacheGroupNodeOption option, std::shared_ptr<MdsClient> mds_client)
    : member_id_(0), option_(option), mds_client_(mds_client) {}

Status CacheGroupNodeMemberImpl::JoinGroup() {
  uint64_t old_id;
  std::string group_name = option_.group_name();

  auto status = LoadMemberId(&old_id);
  if (!status.ok()) {
    return status;
  }

  status = RegisterMember(old_id, &member_id_);
  if (!status.ok()) {
    return status;
  }

  status = AddMember2Group(group_name, member_id_);
  if (!status.ok()) {
    return status;
  }

  return SaveMemberId(member_id_);
}

Status CacheGroupNodeMemberImpl::LeaveGroup() {
  // TODO(Wine93): set node status to offline
  return Status::OK();
}

Status CacheGroupNodeMemberImpl::LoadMemberId(uint64_t* member_id) {
  LocalFileSystem fs;
  uint64_t length;
  std::shared_ptr<char> buffer;
  CacheGroupNodeMetadata meta;
  auto filepath = option_.metadata_filepath();

  auto status = fs.ReadFile(filepath, buffer, &length);
  if (status.IsNotFound()) {
    *member_id = 0;
    LOG(INFO) << "Cache group node metadata file not found, filepath = "
              << filepath;
    return Status::OK();
  } else if (!status.ok()) {
    LOG(ERROR) << "Read cache group node metadata file (" << filepath
               << ") failed: " << status.ToString();
    return status;
  } else if (!meta.ParseFromString(buffer.get())) {
    LOG(ERROR) << "Cache group node metadata file maybe broken, filepath = "
               << filepath;
    return Status::Internal("parse cache group node metadata file failed");
  }

  *member_id = meta.member_id();
  LOG(INFO) << "Load cache group node(id=" << meta.member_id()
            << ",birth_time=" << meta.birth_time() << ") metadata success.";
  return Status::OK();
}

Status CacheGroupNodeMemberImpl::SaveMemberId(uint64_t member_id) {
  std::string buffer;
  CacheGroupNodeMetadata meta;
  meta.set_member_id(member_id);
  meta.set_birth_time(TimeNow().seconds);
  if (!meta.SerializeToString(&buffer)) {
    LOG(ERROR) << "Serialize cache group meta failed.";
    return Status::Internal("serialize cache group node meta failed");
  }

  LocalFileSystem fs;
  auto filepath = option_.metadata_filepath();
  auto status = fs.WriteFile(filepath, buffer.c_str(), buffer.length());
  if (!status.ok()) {
    LOG(ERROR) << "Write cache group node meta to file failed: "
               << "filepath = " << filepath
               << ", status = " << status.ToString();
  }

  LOG(INFO) << "Save cachegroup node meta to file(" << filepath << ") success.";
  return status;
}

Status CacheGroupNodeMemberImpl::RegisterMember(uint64_t old_id,
                                                uint64_t* member_id) {
  auto rc = mds_client_->RegisterCacheGroupMember(old_id, member_id);
  if (rc != CacheGroupOk) {
    LOG(ERROR) << "Register member(member_id=" << old_id
               << ") failed, rc = " << CacheGroupErrCode_Name(rc);
    return Status::Internal("register cache group member failed");
  }

  LOG(INFO) << "Register member success, member_id = " << (*member_id);
  return Status::OK();
}

Status CacheGroupNodeMemberImpl::AddMember2Group(const std::string& group_name,
                                                 uint64_t member_id) {
  pb::mds::cachegroup::CacheGroupMember member;
  member.set_id(member_id);
  member.set_ip(option_.listen_ip());
  member.set_port(option_.listen_port());
  member.set_weight(option_.group_weight());

  auto rc = mds_client_->AddCacheGroupMember(group_name, member);
  if (rc != CacheGroupOk) {
    LOG(ERROR) << "Add member(id=" << member_id_
               << ", weight=" << option_.group_weight()
               << ") to group(name=" << group_name
               << ") failed, rc = " << CacheGroupErrCode_Name(rc);
    return Status::Internal("add cache group member failed");
  }

  LOG(INFO) << "Add member(id=" << member_id_
            << ",weight=" << option_.group_weight()
            << ") to group(name=" << group_name << ") success.";
  return Status::OK();
}

std::string CacheGroupNodeMemberImpl::GetGroupName() {
  return option_.group_name();
}

uint64_t CacheGroupNodeMemberImpl::GetMemberId() { return member_id_; }

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs
