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

#include "common/status.h"
#include "options/cache/cachegroup.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {

class CacheGroupNodeMember {
 public:
  virtual ~CacheGroupNodeMember() = default;

  virtual Status JoinGroup() = 0;
  virtual Status LeaveGroup() = 0;

  virtual std::string GetGroupName() const = 0;
  virtual std::string GetListenIP() const = 0;
  virtual uint32_t GetListenPort() const = 0;
  virtual uint64_t GetMemberId() const = 0;
  virtual std::string GetMemberUuid() const = 0;
};

using CacheGroupNodeMemberSPtr = std::shared_ptr<CacheGroupNodeMember>;

class CacheGroupNodeMemberImpl final : public CacheGroupNodeMember {
 public:
  using MdsClientSPtr = std::shared_ptr<stub::rpcclient::MdsClient>;

  CacheGroupNodeMemberImpl(CacheGroupNodeOption option,
                           MdsClientSPtr mds_client);

  ~CacheGroupNodeMemberImpl() override = default;

  Status JoinGroup() override;
  Status LeaveGroup() override;

  std::string GetGroupName() const override;
  std::string GetListenIP() const override;
  uint32_t GetListenPort() const override;
  uint64_t GetMemberId() const override;
  std::string GetMemberUuid() const override;

 private:
  uint64_t member_id_;       // allocated by mds
  std::string member_uuid_;  // allocated by local, used for directory name
  const CacheGroupNodeOption option_;
  MdsClientSPtr mds_client_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_MEMBER_H_
