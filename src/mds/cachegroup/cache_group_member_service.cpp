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

#include "mds/cachegroup/errno.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

CacheGroupMemberServiceImpl::CacheGroupMemberServiceImpl(
    std::shared_ptr<CacheGroupMemberManager> member_manager)
    : member_manager_(member_manager) {}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, RegisterMember) {
  (void)controller;
  ::brpc::ClosureGuard done_guard(done);

  uint64_t member_id;
  uint64_t old_id = request->has_old_id() ? request->old_id() : 0;
  auto rc = member_manager_->RegisterMember(old_id, &member_id);
  response->set_status(PbErr(rc));
  response->set_member_id(member_id);
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, AddMember) {
  (void)controller;
  ::brpc::ClosureGuard done_guard(done);

  auto rc =
      member_manager_->AddMember(request->group_name(), request->member());
  response->set_status(PbErr(rc));
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, LoadMembers) {
  (void)controller;
  ::brpc::ClosureGuard done_guard(done);

  std::vector<CacheGroupMember> members;
  auto rc = member_manager_->LoadMembers(request->group_name(), &members);
  response->set_status(PbErr(rc));
  *response->mutable_members() = {members.begin(), members.end()};
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, ReweightMember) {
  (void)controller;
  ::brpc::ClosureGuard done_guard(done);

  auto rc = member_manager_->ReweightMember(
      request->group_name(), request->member_id(), request->weight());
  response->set_status(PbErr(rc));
}

DEFINE_RPC_METHOD(CacheGroupMemberServiceImpl, Heartbeat) {
  (void)controller;
  ::brpc::ClosureGuard done_guard(done);

  auto rc = member_manager_->HandleHeartbeat(
      request->group_name(), request->member_id(), request->stat());
  response->set_status(PbErr(rc));
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
