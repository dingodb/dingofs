/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CACHE_V2_REMOTE_MEMBERS_H_
#define DINGOFS_CACHE_V2_REMOTE_MEMBERS_H_

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "cache/v2/common/mds_client.h"
#include "cache/v2/utils/hash.h"

namespace dingofs {
namespace cache {
namespace v2 {

class MemberGroup {
 public:
  explicit MemberGroup(Members members);

  const CacheGroupMember* GetMember(uint64_t key) const;

  const Members& members() const { return members_; }
  size_t size() const { return members_.size(); }
  bool empty() const { return members_.empty(); }

  bool operator==(const MemberGroup& other) const {
    return members_ == other.members_;
  }

 private:
  static constexpr uint32_t kMaxWeightRatio = 64;

  Members FilterMembers(Members members);
  std::vector<uint32_t> RecalcWeights(const Members& members);

  Members members_;
  ConsistentHash chash_;
};

using MemberGroupSPtr = std::shared_ptr<const MemberGroup>;

class CacheGroupMemberSyncer {
 public:
  explicit CacheGroupMemberSyncer(MDSClient* mds_client);
  ~CacheGroupMemberSyncer();

  CacheGroupMemberSyncer(const CacheGroupMemberSyncer&) = delete;
  CacheGroupMemberSyncer& operator=(const CacheGroupMemberSyncer&) = delete;

  void Start();
  void Shutdown();

 private:
  friend class CacheGroupMemberSyncerTest;

  void PeriodicSyncMembers();
  void SyncMembers();
  void PublishToAllShards(MemberGroupSPtr member_group);

  bool running_ = false;
  MDSClient* mds_client_;
  MemberGroupSPtr member_group_;
  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

using CacheGroupMemberSyncerUPtr = std::unique_ptr<CacheGroupMemberSyncer>;

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_REMOTE_MEMBERS_H_
