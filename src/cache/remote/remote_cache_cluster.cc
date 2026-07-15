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
 * Created Date: 2025-01-10
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remote/remote_cache_cluster.h"

#include <butil/logging.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <unordered_set>

#include "cache/common/block_handle_helper.h"
#include "cache/common/mds_client.h"
#include "cache/iutil/ketama_con_hash.h"
#include "cache/iutil/math_util.h"
#include "common/options/cache.h"
#include "dingofs/blockcache.pb.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(periodic_sync_members_ms, 3000,
              "interval for refreshing cache group members from mds in "
              "milliseconds");

RemoteCacheCluster::RemoteCacheCluster()
    : running_(false),
      mds_client_(std::make_unique<MDSClientImpl>()),
      executor_(std::make_unique<BthreadExecutor>()),
      group_(std::make_shared<RemoteNodeGroup>()),
      builder_(std::make_unique<RemoteNodeGroupBuilder>()),
      vars_(std::make_unique<RemoteCacheClusterMetrics>()) {}

void RemoteCacheCluster::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "RemoteCacheCluster already started";
    return;
  }

  LOG(INFO) << "RemoteCacheCluster is starting...";

  Status status = mds_client_->Start();
  if (!status.ok()) {
    LOG(FATAL) << "Fail to start MDS client, status=" << status.ToString();
    return;
  }

  if (!SyncMembers()) {
    LOG(FATAL)
        << "Fail to sync members from mds, is there any member in cache group="
        << FLAGS_cache_group << "?";
    return;
  }

  CHECK(executor_->Start());
  executor_->Schedule([this] { PeriodicSyncMembers(); },
                      FLAGS_periodic_sync_members_ms);

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "RemoteCacheCluster{mds_addr=" << FLAGS_mds_addrs << "} started";
}

void RemoteCacheCluster::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "RemoteCacheCluster already shutdown";
    return;
  }

  LOG(INFO) << "RemoteCacheCluster is shutting down...";

  CHECK(executor_->Stop());

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "RemoteCacheCluster shutdown";
}

Status RemoteCacheCluster::SendPutRequest(const BlockHandle& handle,
                                          const IOBuffer& block) {
  Status status;
  size_t length = block.Size();
  RemoteCacheClusterMetricsGuard guard("Put", length, status, vars_.get());

  pb::cache::PutRequest raw;
  *raw.mutable_handle() = ToHandlePB(handle);
  raw.set_block_size(length);
  auto request = MakeRequest("Put", raw, &block);

  auto response =
      SendRequest<pb::cache::PutRequest, pb::cache::PutResponse>(request);
  status = response.status;
  if (status.IsCacheUnhealthy()) {
    LOG_EVERY_SECOND(ERROR) << "Fail to send " << request;
  } else if (!status.ok()) {
    LOG(ERROR) << "Fail to send " << request;
  }
  return status;
}

Status RemoteCacheCluster::SendRangeRequest(const BlockHandle& handle,
                                            off_t offset, size_t length,
                                            IOBuffer* buffer,
                                            size_t block_whole_length,
                                            bool* cache_hit) {
  Status status;
  RemoteCacheClusterMetricsGuard guard("Range", length, status, vars_.get());

  pb::cache::RangeRequest raw;
  *raw.mutable_handle() = ToHandlePB(handle);
  raw.set_offset(offset);
  raw.set_length(length);
  raw.set_block_size(block_whole_length);
  auto request = MakeRequest("Range", raw, nullptr, buffer);

  auto response =
      SendRequest<pb::cache::RangeRequest, pb::cache::RangeResponse>(request);
  status = response.status;
  if (status.ok()) {
    if (cache_hit != nullptr) {
      *cache_hit = response.raw.cache_hit();
    }
  } else if (status.IsCacheUnhealthy()) {
    LOG_EVERY_SECOND(ERROR) << "Fail to send " << request;
  } else {
    LOG(ERROR) << "Fail to send " << request;
  }
  return status;
}

Status RemoteCacheCluster::SendCacheRequest(const BlockHandle& handle,
                                            const IOBuffer& block) {
  Status status;
  size_t length = block.Size();
  RemoteCacheClusterMetricsGuard guard("Cache", length, status, vars_.get());

  pb::cache::CacheRequest raw;
  *raw.mutable_handle() = ToHandlePB(handle);
  raw.set_block_size(length);
  auto request = MakeRequest("Cache", raw, &block);

  auto response =
      SendRequest<pb::cache::CacheRequest, pb::cache::CacheResponse>(request);
  status = response.status;
  if (status.IsCacheUnhealthy()) {
    LOG_EVERY_SECOND(ERROR) << "Fail to send " << request;
  } else if (!status.ok()) {
    LOG(ERROR) << "Fail to send " << request;
  }
  return status;
}

Status RemoteCacheCluster::SendPrefetchRequest(const BlockHandle& handle,
                                               size_t length) {
  Status status;
  RemoteCacheClusterMetricsGuard guard("Prefetch", length, status, vars_.get());

  pb::cache::PrefetchRequest raw;
  *raw.mutable_handle() = ToHandlePB(handle);
  raw.set_block_size(length);
  auto request = MakeRequest("Prefetch", raw);

  auto response =
      SendRequest<pb::cache::PrefetchRequest, pb::cache::PrefetchResponse>(
          request);
  status = response.status;
  if (status.IsCacheUnhealthy()) {
    LOG_EVERY_SECOND(ERROR) << "Fail to send " << request;
  } else if (!status.ok()) {
    LOG(ERROR) << "Fail to send " << request;
  }
  return status;
}

template <typename T, typename U>
Response<U> RemoteCacheCluster::SendRequest(const Request<T>& request) {
  auto node_group = CHECK_NOTNULL(GetRemoteNodeGroup());
  auto node =
      node_group->SelectNode(FromHandlePB(request.raw.handle()).Filename());
  if (nullptr == node) {
    LOG(ERROR) << "No node found for " << request;
    return Response<U>{Status::NotFound("no node found")};
  } else if (!node->IsHealthy()) {
    LOG_EVERY_SECOND(WARNING)
        << "Fail to send request to " << node << ", because "
        << "node is unhealthy";
    return Response<U>{Status::CacheUnhealthy("node is unhealthy")};
  }

  return node->template SendRequest<T, U>(request);
}

bool RemoteCacheCluster::SendListMembersRequest(Members* members) {
  auto group_name = FLAGS_cache_group;
  auto status = mds_client_->ListMembers(group_name, members);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to list cache group=" << group_name
               << " members from mds";
    return false;
  } else if (members->empty()) {
    LOG(WARNING) << "No member found in cache group=" << group_name;
    return false;
  }
  return true;
}

bool RemoteCacheCluster::SyncMembers() {
  Members members;
  if (!SendListMembersRequest(&members)) {
    return false;
  }

  auto new_group = builder_->Build(members);
  if (new_group != nullptr) {
    SetRemoteNodeGroup(new_group);
    LOG(INFO) << "Successfully rebuild remote node group";
  }
  return true;
}

void RemoteCacheCluster::PeriodicSyncMembers() {
  SyncMembers();
  executor_->Schedule([this] { PeriodicSyncMembers(); },
                      FLAGS_periodic_sync_members_ms);
}

bool RemoteCacheCluster::Dump(Json::Value& value) {
  auto group = GetRemoteNodeGroup();
  if (nullptr == group) {
    return true;
  }

  Json::Value items = Json::arrayValue;
  for (const auto& [id, node] : group->nodes) {
    Json::Value item = Json::objectValue;
    node->Dump(item);

    items.append(item);
  }

  value["members"] = items;
  return true;
}

RemoteNodeGroupBuilder::RemoteNodeGroupBuilder(bool start_nodes)
    : old_group_(std::make_shared<RemoteNodeGroup>()),
      start_nodes_(start_nodes) {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(
                  &queue_id_, &options, &RemoteNodeGroupBuilder::ShutdownNodes,
                  this));
}

RemoteNodeGroupBuilder::~RemoteNodeGroupBuilder() {
  CHECK_EQ(0, bthread::execution_queue_stop(queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(queue_id_));
}

RemoteNodeGroupSPtr RemoteNodeGroupBuilder::Build(const Members& members) {
  auto new_members = FilterMembers(members);
  auto diff = MakeDiff(new_members);
  if (diff.add.empty() && diff.remove.empty()) {
    return nullptr;
  } else if (new_members.empty()) {
    LOG(WARNING)
        << "No nodes alive, skip building RemoteNodeGroup and use old one";
    return nullptr;
  }

  // Create a new RemoteNodeGroup
  std::vector<RemoteNodeSPtr> to_start, to_shutdown;
  std::unordered_map<std::string, RemoteNodeSPtr> new_nodes;

  // kept nodes
  for (const auto& old_node : diff.keep) {
    new_nodes[old_node->Id()] = old_node;
  }

  // added nodes
  for (const auto& new_node : diff.add) {
    new_nodes[new_node->Id()] = new_node;
    to_start.emplace_back(new_node);
  }

  // removed nodes
  to_shutdown = std::move(diff.remove);

  if (start_nodes_) {
    StartNodes(to_start);
  }
  DeferShutdownNodes(to_shutdown);

  auto group = std::make_shared<RemoteNodeGroup>();
  group->chash = BuildHashRing(new_members);
  group->nodes = std::move(new_nodes);

  old_group_ = group;
  return group;
}

Members RemoteNodeGroupBuilder::FilterMembers(const Members& members) {
  Members members_out;
  for (const auto& member : members) {
    if (member.state != CacheGroupMemberState::kOnline) {
      LOG(INFO) << "Filter out non-online " << member;
      continue;
    } else if (member.weight == 0) {
      LOG(INFO) << "Filter out zero-weight " << member;
      continue;
    }
    members_out.emplace_back(member);
  }
  return members_out;
}

RemoteNodeGroupBuilder::Diff RemoteNodeGroupBuilder::MakeDiff(
    const Members& new_members) {
  CHECK_NOTNULL(old_group_);

  Diff diff;
  std::unordered_set<std::string> new_mset;
  const auto& old_nodes = old_group_->nodes;

  // case 1: exist in new members
  for (const auto& new_member : new_members) {
    auto iter = old_nodes.find(new_member.id);
    if (iter == old_nodes.end()) {  // no found in old group
      diff.add.emplace_back(std::make_shared<RemoteNode>(
          new_member.id, new_member.ip, new_member.port, new_member.weight));
    } else {  // found in old group
      auto old_node = iter->second;
      if (old_node->IP() == new_member.ip &&
          old_node->Port() == new_member.port) {
        diff.keep.emplace_back(old_node);
      } else {
        diff.remove.emplace_back(old_node);
        diff.add.emplace_back(std::make_shared<RemoteNode>(
            new_member.id, new_member.ip, new_member.port, new_member.weight));
      }
    }
    new_mset.emplace(new_member.id);
  }

  // case 2: not exist in new members, it should be removed
  for (const auto& item : old_nodes) {
    if (!new_mset.count(item.first)) {        // member_id
      diff.remove.emplace_back(item.second);  // node
    }
  }

  return diff;
}

std::vector<uint64_t> RemoteNodeGroupBuilder::RecalcWeights(
    const Members& members) {
  std::vector<uint64_t> weights(members.size());
  for (int i = 0; i < members.size(); i++) {
    weights[i] = members[i].weight;
  }
  return iutil::NormalizeByGcd(weights);  // FIXME: uint32_t
}

iutil::ConHashUPtr RemoteNodeGroupBuilder::BuildHashRing(
    const Members& members) {
  auto chash = std::make_unique<iutil::KetamaConHash>();
  auto weights = RecalcWeights(members);
  for (int i = 0; i < members.size(); i++) {
    const auto& member = members[i];
    chash->AddNode(member.id, weights[i]);

    LOG(INFO) << "Add " << member << " to consistent hash";
  }

  chash->Final();
  LOG(INFO) << "Hash ring builded";
  return chash;
}

void RemoteNodeGroupBuilder::StartNodes(std::vector<RemoteNodeSPtr> nodes) {
  for (const auto& node : nodes) {
    if (node->Start().ok()) {
      LOG(INFO) << "Successfully started " << *node;
    } else {
      LOG(ERROR) << "Fail to start " << *node;
    }
  }
}

void RemoteNodeGroupBuilder::DeferShutdownNodes(
    std::vector<RemoteNodeSPtr> nodes) {
  CHECK_EQ(0, bthread::execution_queue_execute(queue_id_, std::move(nodes)));
}

int RemoteNodeGroupBuilder::ShutdownNodes(
    void*, bthread::TaskIterator<std::vector<RemoteNodeSPtr>>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  for (; iter; iter++) {
    for (const auto& node : *iter) {
      node->Shutdown();
      LOG(INFO) << "Successfully shutdown " << *node;
    }
  }
  return 0;
}

}  // namespace cache
}  // namespace dingofs
