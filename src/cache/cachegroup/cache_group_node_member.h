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

#include <string>

#include "cache/common/config.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using common::CacheGroupNodeOptions;
using stub::rpcclient::MdsClient;

class CacheGroupNodeMember {
 public:
  virtual ~CacheGroupNodeMember() = default;

  virtual bool JoinGroup() = 0;

  virtual bool LeaveGroup() = 0;

  virtual uint64_t GetMemberId() = 0;
};

class CacheGroupNodeMemberImpl : public CacheGroupNodeMember {
 public:
  explicit CacheGroupNodeMemberImpl(CacheGroupNodeOptions options);

  ~CacheGroupNodeMemberImpl() override = default;

  bool JoinGroup() override;

  bool LeaveGroup() override;

  uint64_t GetGroupId() override;

  uint64_t GetMemberId() override;

  uint64_t GetNodeId() override;

 private:
  bool LoadMemberId(uint64_t* member_id);

  bool SaveMemberId(uint64_t member_id);

  bool RegisterMember(uint64_t old_id, uint64_t* member_id);

  bool AddMember2Group(const std::string& group_name, uint64_t member_id);

 private:
  uint64_t member_id_;
  CacheGroupNodeOptions options_;
  std::shared_ptr<MdsClient> mds_client_;
};

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_MEMBER_H_
