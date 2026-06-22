
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
 * Created Date: 2026-01-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTE_REMOTE_CACHE_CLUSTER_H_
#define DINGOFS_SRC_CACHE_REMOTE_REMOTE_CACHE_CLUSTER_H_

#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>
#include <bthread/rwlock.h>

#include <memory>
#include <unordered_map>
#include <vector>

#include "cache/common/mds_client.h"
#include "cache/common/vars.h"
#include "cache/iutil/con_hash.h"
#include "cache/local/cache_store.h"
#include "cache/remote/remote_node.h"
#include "cache/remote/request.h"
#include "common/block/block_handle.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {

struct RemoteNodeGroup {
  RemoteNodeSPtr SelectNode(const std::string& key) {
    if (chash == nullptr) {
      return nullptr;
    }

    iutil::ConNode node;
    if (chash->Lookup(key, node)) {
      auto iter = nodes.find(node.key);
      if (iter != nodes.end()) {
        return iter->second;
      }
    }
    return nullptr;
  }

  std::unique_ptr<iutil::ConHash> chash;  // member id => vnode
  std::unordered_map<std::string, RemoteNodeSPtr> nodes;  // member id => node
};

using RemoteNodeGroupSPtr = std::shared_ptr<RemoteNodeGroup>;

class RemoteNodeGroupBuilder {
 public:
  explicit RemoteNodeGroupBuilder(bool start_nodes = true);
  ~RemoteNodeGroupBuilder();

  RemoteNodeGroupSPtr Build(const Members& members);

 private:
  struct Diff {
    std::vector<RemoteNodeSPtr> keep;
    std::vector<RemoteNodeSPtr> add;
    std::vector<RemoteNodeSPtr> remove;
  };

  Members FilterMembers(const Members& members);
  Diff MakeDiff(const Members& new_members);
  std::vector<uint64_t> RecalcWeights(const Members& members);
  iutil::ConHashUPtr BuildHashRing(const Members& members);

  void StartNodes(std::vector<RemoteNodeSPtr> nodes);
  void DeferShutdownNodes(std::vector<RemoteNodeSPtr> nodes);
  static int ShutdownNodes(
      void* meta, bthread::TaskIterator<std::vector<RemoteNodeSPtr>>& iter);

  RemoteNodeGroupSPtr old_group_;
  bool start_nodes_;
  bthread::ExecutionQueueId<std::vector<RemoteNodeSPtr>> queue_id_;
};

using RemoteNodeGroupBuilderUPtr = std::unique_ptr<RemoteNodeGroupBuilder>;

struct RemoteCacheClusterMetrics {
  inline static const std::string prefix = "dingofs_remote_cache_cluster";

  OpVar op_put{absl::StrFormat("%s_%s", prefix, "put")};
  OpVar op_range{absl::StrFormat("%s_%s", prefix, "range")};
  OpVar op_cache{absl::StrFormat("%s_%s", prefix, "cache")};
  OpVar op_prefetch{absl::StrFormat("%s_%s", prefix, "prefetch")};
};

using RemoteCacheClusterMetricsUPtr = std::unique_ptr<RemoteCacheClusterMetrics>;

struct RemoteCacheClusterMetricsGuard {
  RemoteCacheClusterMetricsGuard(const std::string& opname, size_t bytes,
                          Status& status, RemoteCacheClusterMetrics* vars)
      : opname(opname), bytes(bytes), status(status), vars(vars) {
    CHECK(opname == "Put" || opname == "Range" || opname == "Cache" ||
          opname == "Prefetch")
        << "Invalid operation name=" << opname;
    timer.start();
  }

  ~RemoteCacheClusterMetricsGuard() {
    timer.stop();

    OpVar* op;
    if (opname == "Put") {
      op = &vars->op_put;
    } else if (opname == "Range") {
      op = &vars->op_range;
    } else if (opname == "Cache") {
      op = &vars->op_cache;
    } else if (opname == "Prefetch") {
      op = &vars->op_prefetch;
    }

    if (status.ok()) {
      op->op_per_second.total_count << 1;
      op->bandwidth_per_second.total_count << bytes;
      op->latency << timer.u_elapsed();
      op->total_latency << timer.u_elapsed();
    } else {
      op->error_per_second.total_count << 1;
    }
  }

  std::string opname;
  size_t bytes;
  Status& status;
  butil::Timer timer;
  RemoteCacheClusterMetrics* vars;
};

class RemoteCacheCluster {
 public:
  RemoteCacheCluster();
  void Start();
  void Shutdown();

  Status SendPutRequest(const BlockHandle& handle, const IOBuffer& block);
  // `cache_hit` (out, may be nullptr): set true iff the remote cache server
  // satisfied the request from its local cache (vs falling through to S3).
  Status SendRangeRequest(const BlockHandle& handle, off_t offset,
                          size_t length, IOBuffer* buffer,
                          size_t block_whole_length, bool* cache_hit);
  Status SendCacheRequest(const BlockHandle& handle, const IOBuffer& block);
  Status SendPrefetchRequest(const BlockHandle& handle, size_t length);

  bool Dump(Json::Value& value);

 private:
  RemoteNodeGroupSPtr GetRemoteNodeGroup() {
    bthread::RWLockRdGuard guard(rwlock_);
    return group_;
  }

  void SetRemoteNodeGroup(const RemoteNodeGroupSPtr& group) {
    bthread::RWLockWrGuard guard(rwlock_);
    group_ = group;
  }

  template <typename T, typename U>
  Response<U> SendRequest(const Request<T>& request);

  bool SendListMembersRequest(Members* members);
  bool SyncMembers();
  void PeriodicSyncMembers();

  std::atomic<bool> running_;
  MDSClientUPtr mds_client_;
  ExecutorUPtr executor_;
  bthread::RWLock rwlock_;  // for group_
  RemoteNodeGroupSPtr group_;
  RemoteNodeGroupBuilderUPtr builder_;
  RemoteCacheClusterMetricsUPtr vars_;
};

using RemoteCacheClusterUPtr = std::unique_ptr<RemoteCacheCluster>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTE_REMOTE_CACHE_CLUSTER_H_