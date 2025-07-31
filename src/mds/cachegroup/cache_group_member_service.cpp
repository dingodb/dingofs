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
 * Created Date: 2025-02-06
 * Author: Jingli Chen (Wine93)
 */

#include "mds/cachegroup/cache_group_member_service.h"

#include <brpc/controller.h>

#include "mds/cachegroup/cache_group_member_manager.h"
#include "mds/cachegroup/helper.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

CacheGroupMemberServiceImpl::CacheGroupMemberServiceImpl(
    CacheGroupMemberManagerSPtr manager)
    : manager_(manager) {}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, JoinCacheGroup) {
  (void)controller;
  ::brpc::ClosureGuard done_guard(done);

  uint64_t member_id;
  std::string member_uuid;
  auto status = manager_->JoinCacheGroup(
      request->group_name(), request->ip(), request->port(), request->weight(),
      request->replace_id(), &member_id, &member_uuid);

  response->set_status(Helper::PBErr(status));
  response->set_member_id(member_id);
  response->set_member_uuid(member_uuid);
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, LeaveCacheGroup) {
  (void)controller;
  ::brpc::ClosureGuard done_guard(done);

  auto status = manager_->LeaveCacheGroup(request->group_name(), request->ip(),
                                          request->port());

  response->set_status(Helper::PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, Heartbeat) {
  (void)controller;
  ::brpc::ClosureGuard done_guard(done);

  auto status = manager_->Heartbeat(request->ip(), request->port());

  response->set_status(Helper::PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, LoadGroups) {  // NOLINT
  (void)controller;
  ::brpc::ClosureGuard done_guard(done);

  const auto& group_names = manager_->GetGroupNames();
  *response->mutable_group_names() = {group_names.begin(), group_names.end()};
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, LoadMembers) {
  (void)controller;
  ::brpc::ClosureGuard done_guard(done);

  std::vector<PBCacheGroupMember> members;
  auto status = manager_->GetMembers(request->group_name(), &members);

  response->set_status(Helper::PBErr(status));
  *response->mutable_members() = {members.begin(), members.end()};
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, ReweightMember) {
  (void)controller;
  ::brpc::ClosureGuard done_guard(done);

  auto status =
      manager_->ReweightMember(request->member_id(), request->weight());
  response->set_status(Helper::PBErr(status));
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
