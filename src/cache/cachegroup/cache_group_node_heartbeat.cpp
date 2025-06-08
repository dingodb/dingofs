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

#include "cache/common/common.h"
#include "cache/metrics/cache_group_node_metric.h"

namespace dingofs {
namespace cache {

CacheGroupNodeHeartbeatImpl::CacheGroupNodeHeartbeatImpl(
    CacheGroupNodeMemberSPtr member,
    std::shared_ptr<stub::rpcclient::MdsClient> mds_client)
    : running_(false),
      member_(member),
      mds_client_(mds_client),
      timer_(std::make_unique<TimerImpl>()) {}

void CacheGroupNodeHeartbeatImpl::Start() {
  if (!running_.exchange(true)) {
    LOG(INFO) << "Cache group node heartbeat starting...";

    CHECK(timer_->Start());
    timer_->Add([this] { SendHeartbeat(); }, FLAGS_send_heartbeat_interval_ms);

    LOG(INFO) << "Cache group node heartbeat started.";
  }
}

void CacheGroupNodeHeartbeatImpl::Stop() {
  if (running_.exchange(false)) {
    LOG(INFO) << "Cache group node heartbeat stoping...";

    CHECK(timer_->Stop());

    LOG(INFO) << "Cache group node heartbeat stopped.";
  }
}

void CacheGroupNodeHeartbeatImpl::SendHeartbeat() {
  PBStatistic stat;
  stat.set_hits(CacheGroupNodeMetric::GetInstance().GetCacheHit());
  stat.set_misses(CacheGroupNodeMetric::GetInstance().GetCacheMiss());

  std::string group_name = member_->GetGroupName();
  uint64_t member_id = member_->GetMemberId();
  auto rc = mds_client_->SendCacheGroupHeartbeat(group_name, member_id, stat);
  if (rc != PBCacheGroupErrCode::CacheGroupOk) {
    LOG(ERROR) << "Send heartbeat for (" << group_name << "," << member_id
               << ") failed: rc = " << CacheGroupErrCode_Name(rc);
  }

  timer_->Add([this] { SendHeartbeat(); }, FLAGS_send_heartbeat_interval_ms);
}

}  // namespace cache
}  // namespace dingofs
