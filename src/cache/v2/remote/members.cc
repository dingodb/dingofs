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

#include "cache/v2/remote/members.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "cache/v2/common/flag_decls.h"
#include "cache/v2/core/runtime/shard_inbox.h"
#include "cache/v2/core/runtime/smp.h"
#include "cache/v2/remote/node_group.h"
#include "cache/v2/utils/thread.h"

namespace dingofs {
namespace cache {
namespace v2 {

DEFINE_uint32(periodic_sync_members_ms, 3000,
              "interval for sync members from mds in milliseconds");

MemberGroup::MemberGroup(Members members)
    : members_(FilterMembers(std::move(members))) {
  const std::vector<uint32_t> weights = RecalcWeights(members_);
  for (uint32_t i = 0; i < members_.size(); ++i) {
    chash_.Add(i, members_[i].id, weights[i]);
  }
  chash_.Finalize();
}

const CacheGroupMember* MemberGroup::GetMember(uint64_t key) const {
  if (members_.empty()) {
    return nullptr;
  }
  return &members_[chash_.MemberOf(key)];
}

Members MemberGroup::FilterMembers(Members members) {
  std::erase_if(members, [](const CacheGroupMember& member) {
    return member.state != CacheGroupMemberState::kOnline || member.weight == 0;
  });

  std::ranges::sort(members, {}, &CacheGroupMember::id);

  const auto duplicated =
      std::ranges::unique(members, {}, &CacheGroupMember::id);
  members.erase(duplicated.begin(), duplicated.end());

  return members;
}

std::vector<uint32_t> MemberGroup::RecalcWeights(const Members& members) {
  std::vector<uint64_t> raw;
  raw.reserve(members.size());
  for (const CacheGroupMember& member : members) {
    raw.push_back(member.weight);
  }

  std::vector<uint32_t> weights = NormalizeGcd(raw);
  const auto heaviest = std::ranges::max_element(weights);
  if (heaviest == weights.end() || *heaviest <= kMaxWeightRatio) {
    return weights;
  }

  const uint64_t top = *heaviest;
  for (uint32_t& weight : weights) {
    weight = static_cast<uint32_t>(
        std::max<uint64_t>(1, uint64_t{weight} * kMaxWeightRatio / top));
  }

  return weights;
}

CacheGroupMemberSyncer::CacheGroupMemberSyncer(MDSClient* mds_client)
    : mds_client_(mds_client),
      member_group_(std::make_shared<const MemberGroup>(Members{})) {
  CHECK(mds_client_ != nullptr) << "cache group syncer needs mds client";
  CHECK(!FLAGS_cache_group.empty()) << "cache group syncer needs group";
}

CacheGroupMemberSyncer::~CacheGroupMemberSyncer() { Shutdown(); }

void CacheGroupMemberSyncer::Start() {
  LOG(INFO) << "CacheGroupMemberSyncer is starting...";

  CHECK(!thread_.joinable()) << "CacheGroupMemberSyncer started twice";

  running_ = true;

  thread_ = std::thread([this] {
    SetThreadName("group-syncer");
    PeriodicSyncMembers();
  });

  LOG(INFO) << "Successfully start CacheGroupMemberSyncer{group="
            << FLAGS_cache_group
            << " interval=" << FLAGS_periodic_sync_members_ms << "ms}";
}

void CacheGroupMemberSyncer::Shutdown() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) {
      return;
    }
    running_ = false;
  }
  cv_.notify_one();

  LOG(INFO) << "CacheGroupMemberSyncer is shutting down...";

  thread_.join();

  LOG(INFO) << "Successfully shutdown CacheGroupMemberSyncer";
}

void CacheGroupMemberSyncer::PeriodicSyncMembers() {
  std::unique_lock<std::mutex> lock(mutex_);
  do {
    lock.unlock();
    SyncMembers();
    lock.lock();
  } while (!cv_.wait_for(
      lock, std::chrono::milliseconds(FLAGS_periodic_sync_members_ms),
      [this] { return !running_; }));
}

void CacheGroupMemberSyncer::SyncMembers() {
  Members members;
  const Status status = mds_client_->ListMembers(FLAGS_cache_group, &members);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to list members of cache group=" << FLAGS_cache_group
               << " from mds: " << status.ToString();
    return;
  }

  auto member_group = std::make_shared<const MemberGroup>(std::move(members));
  if (member_group->empty()) {
    LOG(ERROR) << "Thers is no members in cache group=" << FLAGS_cache_group;
  } else if (*member_group != *member_group_) {
    LOG(INFO) << "Cache group=" << FLAGS_cache_group
              << " changed: " << member_group_->size() << " -> "
              << member_group->size() << " member(s)";
    member_group_ = std::move(member_group);
  }

  if (!member_group_->empty()) {
    PublishToAllShards(member_group_);
  }
}

struct PublishTask : InboxWork {
  explicit PublishTask(MemberGroupSPtr member_group)
      : member_group(std::move(member_group)) {
    run = [](InboxWork* base) {
      auto* self = static_cast<PublishTask*>(base);
      RemoteNodeGroup* node_group = ThisNodeGroup();
      if (node_group != nullptr) {
        node_group->Rebuild(std::move(self->member_group));
      }
      delete self;
    };
  }

  MemberGroupSPtr member_group;
};

void CacheGroupMemberSyncer::PublishToAllShards(MemberGroupSPtr member_group) {
  const unsigned shards = ShardCount();
  for (unsigned shard = 0; shard < shards; ++shard) {
    auto* task = new PublishTask(member_group);
    if (!PostTo(shard, task)) {
      delete task;
    }
  }
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
