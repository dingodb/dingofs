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

#include "base/string/string.h"
#include "base/time/time.h"
#include "cache/common/common.h"
#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {

CacheGroupNodeMemberImpl::CacheGroupNodeMemberImpl(
    CacheGroupNodeOption option,
    std::shared_ptr<stub::rpcclient::MdsClient> mds_client)
    : member_id_(0),
      member_uuid_(""),
      option_(option),
      mds_client_(mds_client) {}

Status CacheGroupNodeMemberImpl::JoinGroup() {
  uint64_t old_id;
  auto status = LoadMemberId(&old_id);
  if (!status.ok()) {
    return status;
  }

  status = RegisterMember(old_id, &member_id_);
  if (!status.ok()) {
    return status;
  }

  status = AddMember2Group(option_.group_name, member_id_);
  if (!status.ok()) {
    return status;
  }

  return SaveMemberId(member_id_);
}

Status CacheGroupNodeMemberImpl::LeaveGroup() {
  // TODO: node status should be set to offline
  return Status::OK();
}

Status CacheGroupNodeMemberImpl::LoadMemberId(uint64_t* member_id) {
  std::string content;
  PB_CacheGroupNodeMetadata metadata;

  auto filepath = option_.metadata_filepath;
  auto status = Helper::ReadFile(filepath, &content);
  if (status.IsNotFound()) {
    *member_id = 0;
    LOG(INFO) << "Cache group node metadata file not found: filepath="
              << filepath;
    return Status::OK();
  } else if (!status.ok()) {
    LOG(ERROR) << "Read cache group node metadata file (path=" << filepath
               << ") failed: " << status.ToString();
    return status;
  } else if (!metadata.ParseFromString(content)) {
    LOG(ERROR) << "Cache group node metadata file maybe broken: filepath = "
               << filepath;
    return Status::Internal("parse cache group node metadata file failed");
  }

  *member_id = metadata.member_id();

  LOG(INFO) << "Load cache group node success: "
            << "id = " << *member_id
            << ", birth_time = " << metadata.birth_time();

  return Status::OK();
}

Status CacheGroupNodeMemberImpl::RegisterMember(uint64_t old_id,
                                                uint64_t* member_id) {
  auto rc = mds_client_->RegisterCacheGroupMember(old_id, member_id);
  if (rc != PB_CacheGroupErrCode::CacheGroupOk) {
    LOG(ERROR) << "Register member (id=" << old_id
               << ") failed: rc = " << CacheGroupErrCode_Name(rc);
    return Status::Internal("register cache group member failed");
  }

  LOG(INFO) << "Register member success: id = " << (*member_id);

  return Status::OK();
}

Status CacheGroupNodeMemberImpl::AddMember2Group(const std::string& group_name,
                                                 uint64_t member_id) {
  PB_CacheGroupMember member;
  member.set_id(member_id);
  member.set_ip(option_.listen_ip);
  member.set_port(option_.listen_port);
  member.set_weight(option_.group_weight);

  auto rc = mds_client_->AddCacheGroupMember(group_name, member);
  if (rc != PB_CacheGroupErrCode::CacheGroupOk) {
    LOG(ERROR) << "Add member (id=" << member_id_
               << ", weight=" << option_.group_weight
               << ") to group (name=" << group_name
               << ") failed: rc = " << CacheGroupErrCode_Name(rc);
    return Status::Internal("add cache group member failed");
  }

  LOG(INFO) << "Add member (id=" << member_id_
            << ", weight=" << option_.group_weight
            << ") to group (name=" << group_name << ") success.";

  return Status::OK();
}

Status CacheGroupNodeMemberImpl::SaveMemberId(uint64_t member_id) {
  std::string content;
  PB_CacheGroupNodeMetadata metadata;
  metadata.set_birth_time(base::time::TimeNow().seconds);
  metadata.set_member_id(member_id);
  metadata.set_member_uuid(GenMemberUuid());
  if (!metadata.SerializeToString(&content)) {
    LOG(ERROR) << "Serialize cache group metadata failed.";
    return Status::Internal("serialize cache group node metadata failed");
  }

  auto filepath = option_.metadata_filepath;
  auto status = Helper::WriteFile(filepath, content);
  if (!status.ok()) {
    LOG(ERROR) << "Write cache group node metadata to file failed: "
               << "filepath = " << filepath
               << ", status = " << status.ToString();
  }

  LOG(INFO) << "Save cachegroup node metadata to file (path=" << filepath
            << ") success.";

  return status;
}

std::string CacheGroupNodeMemberImpl::GenMemberUuid() {
  member_uuid_ = base::string::GenUuid();
  return member_uuid_;
}

std::string CacheGroupNodeMemberImpl::GetGroupName() const {
  return option_.group_name;
}

uint64_t CacheGroupNodeMemberImpl::GetMemberId() const { return member_id_; }

std::string CacheGroupNodeMemberImpl::GetMemberUuid() const {
  return member_uuid_;
}

// void CacheGroupNodeMemberImpl::SetLocalAccessOption(LocalAccessOption* o,
//                                                     uint64_t member_id) {
//   auto disk_cache_options =
//   option_.block_cache_option().disk_cache_options(); const auto&
//   disk_cache_option = disk_cache_options[0]; if
//   (!option_.local_access_enable()) {
//     return;
//   } else if (disk_cache_options.size() != 1) {
//     LOG(WARNING) << "Local access option only support one disk cache now.";
//     return;
//   }
//
//   auto cache_dir = disk_cache_option.cache_dir();
//   auto layout =
//       blockcache::DiskCacheLayout(blockcache::RootDir(cache_dir,
//       member_id_));
//
//   o->set_root_dir(layout.GetRootDir());
//   // o->set_lock_path(layout.GetLockPath());
//   if (disk_cache_option.filesystem_type() == "3fs") {
//     o->set_filesystem_type(FileSystemType::FileSystemType3FS);
//   } else {
//     o->set_filesystem_type(FileSystemType::FileSystemTypeLocal);
//   }
// }

}  // namespace cache
}  // namespace dingofs
