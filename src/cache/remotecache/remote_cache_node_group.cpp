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
 * Created Date: 2025-06-05
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/remote_cache_node_group.h"

#include <glog/logging.h>

#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/macro.h"
#include "cache/common/mds_client.h"
#include "cache/remotecache/remote_cache_node.h"
#include "cache/remotecache/remote_cache_node_manager.h"
#include "cache/remotecache/upstream.h"
#include "cache/utils/context.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

RemoteCacheNodeGroup::RemoteCacheNodeGroup()
    : running_(false),
      upstream_(std::make_unique<UpstreamImpl>()),
      metric_(std::make_shared<RemoteCacheCacheNodeGroupMetric>()) {
  node_manager_ = std::make_unique<RemoteCacheNodeManager>(
      [this](const std::vector<CacheGroupMember>& members) {
        this->upstream_->Build(members);
      });
}

Status RemoteCacheNodeGroup::Start() {
  CHECK_NOTNULL(upstream_);
  CHECK_NOTNULL(node_manager_);
  CHECK_NOTNULL(metric_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Remote node group is starting...";

  auto status = node_manager_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start remote node manager failed: " << status.ToString();
    return status;
  }

  running_ = true;

  LOG(INFO) << "Remote node group is up.";

  CHECK_RUNNING("Remote node group");
  return Status::OK();
}

Status RemoteCacheNodeGroup::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Remote node group is shutting down...";

  node_manager_->Shutdown();

  LOG(INFO) << "Remote node group is down.";

  CHECK_DOWN("Remote node group");
  return Status::OK();
}

Status RemoteCacheNodeGroup::Put(ContextSPtr ctx, const BlockKey& key,
                                 const Block& block) {
  CHECK_RUNNING("Remote cache node group");

  Status status;
  RemoteCacheNodeSPtr node;
  RemoteCacheCacheNodeGroupMetricGuard metirc_guard(__func__, block.size,
                                                    status, metric_);
  status = upstream_->GetNode(key, node);
  if (status.ok()) {
    status = node->Put(ctx, key, block);
  }
  return status;
}

Status RemoteCacheNodeGroup::Range(ContextSPtr ctx, const BlockKey& key,
                                   off_t offset, size_t length,
                                   IOBuffer* buffer, RangeOption option) {
  CHECK_RUNNING("Remote cache node group");

  Status status;
  RemoteCacheNodeSPtr node;
  RemoteCacheCacheNodeGroupMetricGuard metric_guard(__func__, length, status,
                                                    metric_);
  status = upstream_->GetNode(key, node);
  if (status.ok()) {
    status = node->Range(ctx, key, offset, length, buffer, option);
  }
  return status;
}

Status RemoteCacheNodeGroup::Cache(ContextSPtr ctx, const BlockKey& key,
                                   const Block& block) {
  CHECK_RUNNING("Remote cache node group");

  Status status;
  RemoteCacheNodeSPtr node;
  RemoteCacheCacheNodeGroupMetricGuard metric_guard(__func__, block.size,
                                                    status, metric_);
  status = upstream_->GetNode(key, node);
  if (status.ok()) {
    status = node->Cache(ctx, key, block);
  }
  return status;
}

Status RemoteCacheNodeGroup::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                      size_t length) {
  CHECK_RUNNING("Remote cache node group");

  Status status;
  RemoteCacheNodeSPtr node;
  RemoteCacheCacheNodeGroupMetricGuard metric_guard(__func__, length, status,
                                                    metric_);
  status = upstream_->GetNode(key, node);
  if (status.ok()) {
    status = node->Prefetch(ctx, key, length);
  }
  return status;
}

}  // namespace cache
}  // namespace dingofs
