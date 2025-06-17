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

#include "cache/cachegroup/cache_group_node_heartbeat.h"

#include "cache/common/macro.h"
#include "cache/common/proto.h"
#include "cache/config/cachegroup.h"
#include "metrics/cache/cache_group_node_metric.h"
#include "utils/executor/timer_impl.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(send_heartbeat_interval_ms, 1000,
              "Interval to send heartbeat to MDS in milliseconds");

CacheGroupNodeHeartbeatImpl::CacheGroupNodeHeartbeatImpl(
    CacheGroupNodeMemberSPtr member, MdsClientSPtr mds_client)
    : running_(false),
      member_(member),
      mds_client_(mds_client),
      timer_(std::make_unique<TimerImpl>()) {}

void CacheGroupNodeHeartbeatImpl::Start() {
  CHECK_NOTNULL(member_);
  CHECK_NOTNULL(mds_client_);
  CHECK_NOTNULL(timer_);

  if (running_) {
    return;
  }

  LOG(INFO) << "Cache group node heartbeat is starting...";

  CHECK(timer_->Start());
  timer_->Add([this] { SendHeartbeat(); }, FLAGS_send_heartbeat_interval_ms);

  running_ = true;

  LOG(INFO) << "Cache group node heartbeat is up.";

  CHECK_RUNNING("Cache group node heartbeat");
}

void CacheGroupNodeHeartbeatImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  LOG(INFO) << "Cache group node heartbeat is shutting down...";

  CHECK(timer_->Stop());

  LOG(INFO) << "Cache group node heartbeat is down.";

  CHECK_DOWN("Cache group node heartbeat");
}

void CacheGroupNodeHeartbeatImpl::SendHeartbeat() {
  PBStatistic stat;
  stat.set_hits(GetCacheHitCount());
  stat.set_misses(GetCacheMissCount());

  std::string group_name = member_->GetGroupName();
  uint64_t member_id = member_->GetMemberId();
  auto rc = mds_client_->SendCacheGroupHeartbeat(group_name, member_id, stat);
  if (rc != PBCacheGroupErrCode::CacheGroupOk) {
    LOG(ERROR) << "Send heartbeat for (" << group_name << "," << member_id
               << ") failed: rc = " << CacheGroupErrCode_Name(rc);
  }

  timer_->Add([this] { SendHeartbeat(); }, FLAGS_send_heartbeat_interval_ms);
}

int64_t CacheGroupNodeHeartbeatImpl::GetCacheHitCount() {
  return metrics::CacheGroupNodeMetric::GetInstance()
      .cache_hit_count.get_value();
}

int64_t CacheGroupNodeHeartbeatImpl::GetCacheMissCount() {
  return metrics::CacheGroupNodeMetric::GetInstance()
      .cache_miss_count.get_value();
}

}  // namespace cache
}  // namespace dingofs
