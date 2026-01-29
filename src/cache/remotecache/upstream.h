
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

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_UPSTREAM_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_UPSTREAM_H_

#include <bthread/rwlock.h>

#include <memory>

#include "cache/blockcache/cache_store.h"
#include "cache/common/mds_client.h"
#include "cache/common/vars.h"
#include "cache/remotecache/peer_group.h"
#include "cache/remotecache/request.h"
#include "common/trace/context.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {

struct UpstreamVarsCollector {
  // FIXME: dingofs_upstream
  inline static const std::string prefix = "dingofs_remote_node_group";

  OpVar op_put{absl::StrFormat("%s_%s", prefix, "put")};
  OpVar op_range{absl::StrFormat("%s_%s", prefix, "range")};
  OpVar op_cache{absl::StrFormat("%s_%s", prefix, "cache")};
  OpVar op_prefetch{absl::StrFormat("%s_%s", prefix, "prefetch")};
};

using UpstreamVarsCollectorUPtr = std::unique_ptr<UpstreamVarsCollector>;

struct UpstreamVarsRecordGuard {
  UpstreamVarsRecordGuard(const std::string& opname, size_t bytes,
                          Status& status, UpstreamVarsCollector* vars)
      : opname(opname), bytes(bytes), status(status), vars(vars) {
    CHECK(opname == "Put" || opname == "Range" || opname == "Cache" ||
          opname == "Prefetch")
        << "Invalid operation name=" << opname;
    timer.start();
  }

  ~UpstreamVarsRecordGuard() {
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
  UpstreamVarsCollector* vars;
};

class Upstream {
 public:
  Upstream();
  void Start();
  void Shutdown();

  Status SendPutRequest(ContextSPtr ctx, const BlockKey& key,
                        const Block& block);
  Status SendRangeRequest(ContextSPtr ctx, const BlockKey& key, off_t offset,
                          size_t length, IOBuffer* buffer,
                          size_t block_whole_length);
  Status SendCacheRequest(ContextSPtr ctx, const BlockKey& key,
                          const Block& block);
  Status SendPrefetchRequest(ContextSPtr ctx, const BlockKey& key,
                             size_t length);

  bool Dump(Json::Value& value);

 private:
  PeerGroupSPtr GetPeerGroup() {
    bthread::RWLockRdGuard guard(rwlock_);
    return group_;
  }

  void SetPeerGroup(const PeerGroupSPtr& group) {
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
  PeerGroupSPtr group_;
  PeerGroupBuilderUPtr builder_;
  UpstreamVarsCollectorUPtr vars_;
};

using UpstreamUPtr = std::unique_ptr<Upstream>;

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_UPSTREAM_H_