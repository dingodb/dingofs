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

#include "mds/cachegroup/member_service.h"

#include <brpc/controller.h>

#include "common/status.h"
#include "mds/cachegroup/helper.h"
#include "mds/cachegroup/member.h"
#include "mds/cachegroup/member_manager.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

CacheGroupMemberServiceImpl::CacheGroupMemberServiceImpl(
    CacheGroupMemberManagerSPtr manager)
    : manager_(manager) {}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, RegisterMember) {
  (void)controller;
  brpc::ClosureGuard done_guard(done);

  std::string member_id;
  auto status = manager_->RegisterMember(
      EndPoint(request->ip(), request->port()), request->want_id(), &member_id);

  response->set_status(Helper::PBErr(status));
  if (status.ok()) {
    response->set_member_id(member_id);
  }
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, DeregisterMember) {
  (void)controller;
  brpc::ClosureGuard done_guard(done);

  auto status =
      manager_->DeregisterMember(EndPoint(request->ip(), request->port()));

  response->set_status(Helper::PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, Heartbeat) {
  (void)controller;
  brpc::ClosureGuard done_guard(done);

  auto status =
      manager_->MemberHeartbeat(EndPoint(request->ip(), request->port()));

  response->set_status(Helper::PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, ReweightMember) {
  (void)controller;
  brpc::ClosureGuard done_guard(done);

  auto status =
      manager_->ReweightMember(request->member_id(), request->weight());
  response->set_status(Helper::PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, ListMembers) {  // NOLINT
  (void)controller;
  brpc::ClosureGuard done_guard(done);

  Status status;
  std::vector<PBCacheGroupMember> members;
  if (request->has_group_name()) {
    status = manager_->ListGroupMembers(request->group_name(), &members);
  } else {
    members = manager_->ListAllMembers();
    status = Status::OK();
  }

  response->set_status(Helper::PBErr(status));
  if (status.ok()) {
    *response->mutable_members() = {members.begin(), members.end()};
  }
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, JoinCacheGroup) {
  (void)controller;
  brpc::ClosureGuard done_guard(done);

  std::string member_id;
  auto status = manager_->JoinCacheGroup(
      request->group_name(), EndPoint(request->ip(), request->port()),
      request->weight(), &member_id);

  response->set_status(Helper::PBErr(status));
  if (status.ok()) {
    response->set_member_id(member_id);
  }
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, LeaveCacheGroup) {
  (void)controller;
  brpc::ClosureGuard done_guard(done);

  Status status;
  const auto& group_name = request->group_name();
  if (request->has_ip() && request->has_port()) {
    status = manager_->LeaveCacheGroup(
        group_name, EndPoint(request->ip(), request->port()));
  } else if (request->has_member_id()) {
    status = manager_->LeaveCacheGroup(group_name, request->member_id());
  } else {
    status = Status::InvalidParam("either endpoint or id must be provided");
  }

  response->set_status(Helper::PBErr(status));
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, ListGroups) {  // NOLINT
  (void)controller;
  brpc::ClosureGuard done_guard(done);

  const auto& group_names = manager_->ListGroups();
  *response->mutable_group_names() = {group_names.begin(), group_names.end()};
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
