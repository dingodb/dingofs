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

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_MEMBER_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_MEMBER_H_

#include <memory>
#include <string>

#include "cache/common/common.h"
#include "common/status.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using dingofs::stub::rpcclient::MdsClient;

class CacheGroupNodeMember {
 public:
  virtual ~CacheGroupNodeMember() = default;

  virtual Status JoinGroup() = 0;

  virtual Status LeaveGroup() = 0;

  virtual std::string GetGroupName() = 0;

  virtual uint64_t GetMemberId() = 0;
};

class CacheGroupNodeMemberImpl : public CacheGroupNodeMember {
 public:
  CacheGroupNodeMemberImpl(CacheGroupNodeOption option,
                           std::shared_ptr<MdsClient> mds_client);

  ~CacheGroupNodeMemberImpl() override = default;

  Status JoinGroup() override;

  Status LeaveGroup() override;

  std::string GetGroupName() override;

  uint64_t GetMemberId() override;

 private:
  Status LoadMemberId(uint64_t* member_id);

  Status SaveMemberId(uint64_t member_id);

  Status RegisterMember(uint64_t old_id, uint64_t* member_id);

  Status AddMember2Group(const std::string& group_name, uint64_t member_id);

 private:
  uint64_t member_id_;
  CacheGroupNodeOption option_;
  std::shared_ptr<MdsClient> mds_client_;
};

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_MEMBER_H_
