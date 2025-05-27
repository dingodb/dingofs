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

#include <glog/logging.h>

#include "dingofs/cachegroup.pb.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using dingofs::pb::mds::cachegroup::CacheGroupOk;

CacheGroupNodeHeartbeatImpl::CacheGroupNodeHeartbeatImpl(
    CacheGroupNodeOption option, std::shared_ptr<CacheGroupNodeMember> member,
    std::shared_ptr<CacheGroupNodeMetric> metric,
    std::shared_ptr<MdsClient> mds_client)
    : option_(option),
      member_(member),
      metric_(metric),
      mds_client_(mds_client),
      timer_(std::make_unique<TimerImpl>()) {}

void CacheGroupNodeHeartbeatImpl::Start() {
  if (!running_.exchange(true)) {
    CHECK(timer_->Start());
    timer_->Add([this] { SendHeartbeat(); }, 1000);
  }
}

void CacheGroupNodeHeartbeatImpl::Stop() {
  if (running_.exchange(false)) {
    CHECK(timer_->Stop());
  }
}

void CacheGroupNodeHeartbeatImpl::SendHeartbeat() {
  pb::mds::cachegroup::HeartbeatRequest::Statistic stat;
  stat.set_hits(metric_->GetCacheHit());
  stat.set_misses(metric_->GetCacheMiss());

  std::string group_name = member_->GetGroupName();
  uint64_t member_id = member_->GetMemberId();
  auto rc = mds_client_->SendCacheGroupHeartbeat(group_name, member_id, stat);
  if (rc != CacheGroupOk) {
    LOG(ERROR) << "Send heartbeat for (" << group_name << "," << member_id
               << ") failed, rc = " << CacheGroupErrCode_Name(rc);
  }
  timer_->Add([this] { SendHeartbeat(); }, 1000);
}

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs
