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

#ifndef DINGOFS_SRC_MDS_CACHEGROUP_SERVICE_H_
#define DINGOFS_SRC_MDS_CACHEGROUP_SERVICE_H_

#include "dingofs/cachegroup.pb.h"
#include "mds/cachegroup/member_manager.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

#define DECLARE_RPC_METHOD(method)                                 \
  void method(::google::protobuf::RpcController* controller,       \
              const pb::mds::cachegroup::method##Request* request, \
              pb::mds::cachegroup::method##Response* response,     \
              ::google::protobuf::Closure* done)

#define DEFINE_RPC_METHOD(classname, method)                                  \
  void classname::method(::google::protobuf::RpcController* controller,       \
                         const pb::mds::cachegroup::method##Request* request, \
                         pb::mds::cachegroup::method##Response* response,     \
                         ::google::protobuf::Closure* done)

class CacheGroupMemberServiceImpl
    : public pb::mds::cachegroup::CacheGroupMemberService {
 public:
  explicit CacheGroupMemberServiceImpl(CacheGroupMemberManagerSPtr manager);

  DECLARE_RPC_METHOD(RegisterMember);
  DECLARE_RPC_METHOD(DeregisterMember);
  DECLARE_RPC_METHOD(DeleteMemberId);
  DECLARE_RPC_METHOD(Heartbeat);
  DECLARE_RPC_METHOD(ReweightMember);
  DECLARE_RPC_METHOD(ListMembers);

  DECLARE_RPC_METHOD(JoinCacheGroup);
  DECLARE_RPC_METHOD(LeaveCacheGroup);
  DECLARE_RPC_METHOD(ListGroups);

 private:
  CacheGroupMemberManagerSPtr manager_;
};

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_CACHEGROUP_SERVICE_H_
