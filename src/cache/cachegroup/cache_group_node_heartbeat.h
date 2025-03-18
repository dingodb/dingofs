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
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_SERVER_BLOCK_CACHE_NODE_HEARTBEAT_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_SERVER_BLOCK_CACHE_NODE_HEARTBEAT_H_

#include <cstdlib>
#include <memory>

#include "base/timer/timer_impl.h"
#include "cache/cachegroup/block_cache_node_metric.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using base::timer::Timer;
using stub::rpcclient::MdsClient;

class CacheGroupNodeHeartbeat {
 public:
  virtual ~CacheGroupNodeHeartbeat() = default;

  virtual void Start() = 0;
};

class CacheGroupNodeHeartbeatImpl : public CacheGroupNodeHeartbeat {
 public:
  CacheGroupNodeHeartbeatImpl(const std::string& group_name, uint64_t member_id,
                              std::shared_ptr<BlockCacheNodeMetric> metric,
                              std::shared_ptr<MdsClient> mds_client);

  void Start() override;

 private:
  void SendHeartbeat();

 private:
  std::string group_name_;
  uint64_t member_id_;
  std::shared_ptr<BlockCacheNodeMetric> metric_;
  std::shared_ptr<MdsClient> mds_client_;
  std::unique_ptr<Timer> timer_;
};

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_SERVER_BLOCK_CACHE_NODE_HEARTBEAT_H_
