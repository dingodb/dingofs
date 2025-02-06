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
#include "cache/common/config.h"
#include "cache/common/errno.h"
#include "cache/common/local_filesystem.h"
#include "dingofs/cachegroup.pb.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using base::time::TimeNow;
using cache::common::Errno;
using cache::common::LocalFileSystem;
using pb::mds::cachegroup::CacheGroupNodeMeta;
using pb::mds::cachegroup::CacheGroupOk;

CacheGroupNodeMemberImpl::CacheGroupNodeMemberImpl(
    CacheGroupNodeOptions options)
    : member_id_(0), options_(options), mds_client_(options.mds_client) {}

uint64_t CacheGroupNodeMemberImpl::GetMemberId() { return member_id_; }

bool CacheGroupNodeMemberImpl::JoinGroup() {
  uint64_t old_id, member_id;
  std::string group_name = options_.group_name;
  return LoadMemberId(&old_id) && RegisterMember(old_id, &member_id_) &&
         AddMember2Group(group_name, member_id_) && SaveMemberId(member_id);
}

bool CacheGroupNodeMemberImpl::LeaveGroup() {
  // TODO
  return true;
}

bool CacheGroupNodeMemberImpl::LoadMemberId(uint64_t* member_id) {
  LocalFileSystem fs;
  size_t length;
  std::shared_ptr<char> buffer;

  auto filepath = options_.metadata_filepath;
  auto rc = fs.ReadFile(filepath, buffer, &length);
  if (rc == Errno::NOT_FOUND) {
    *member_id = 0;
    return true;
  } else if (rc != Errno::OK) {
    LOG(ERROR) << "Read member id file (" << filepath
               << ") failed: " << StrErr(rc);
    return false;
  }

  CacheGroupNodeMeta meta;
  if (!meta.ParseFromString(buffer.get())) {
    LOG(ERROR) << "Block cache node meta file maybe broken, filepath="
               << filepath;
    return false;
  }

  *member_id = meta.member_id();
  LOG(INFO) << "Load block cache node(id=" << meta.member_id() << ",birth_time"
            << meta.birth_time() << ") 's meta success.";
  return true;
}

bool CacheGroupNodeMemberImpl::SaveMemberId(uint64_t member_id) {
  std::string buffer;
  CacheGroupNodeMeta meta;
  meta.set_member_id(member_id);
  meta.set_birth_time(TimeNow().seconds);
  if (!meta.SerializeToString(&buffer)) {
    return false;
  }

  LocalFileSystem fs;
  auto filepath = options_.metadata_filepath;
  auto rc = fs.WriteFile(filepath, buffer.c_str(), buffer.length());
  return rc == Errno::OK;
}

bool CacheGroupNodeMemberImpl::RegisterMember(uint64_t old_id,
                                              uint64_t* member_id) {
  auto rc = mds_client_->RegisterCacheGroupMember(old_id, member_id);
  if (rc != CacheGroupOk) {
    LOG(ERROR) << "Register member(" << old_id
               << ") failed, rc=" << CacheGroupErrCode_Name(rc);
    return false;
  }
  LOG(INFO) << "Register member success, member_id=" << (*member_id);
  return true;
}

bool CacheGroupNodeMemberImpl::AddMember2Group(const std::string& group_name,
                                               uint64_t member_id) {
  pb::mds::cachegroup::CacheGroupMember member;
  member.set_id(member_id);
  member.set_ip(options_.listen_ip);
  member.set_port(options_.listen_port);
  member.set_weight(options_.group_weight);

  auto rc = mds_client_->AddCacheGroupMember(group_name, member);
  if (rc != CacheGroupOk) {
    LOG(ERROR) << "Add member(" << member_id_ << "," << options_.group_weight
               << ") to group(" << group_name
               << ") failed, rc=" << CacheGroupErrCode_Name(rc);
    return false;
  }
  LOG(INFO) << "Add member(" << member_id_ << "," << options_.group_weight
            << ") to group(" << group_name << ") success.";
  return true;
}

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs
