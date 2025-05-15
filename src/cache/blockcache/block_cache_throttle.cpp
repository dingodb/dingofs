/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-26
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/block_cache_throttle.h"

#include <bthread/mutex.h>

#include <memory>

#include "base/math/math.h"
#include "cache/common/common.h"
#include "utils/leaky_bucket.h"

namespace dingofs {
namespace cache {
namespace blockcache {

BlockCacheThrottle::BlockCacheThrottle()
    : current_bandwidth_throttle_mb_(
          FLAGS_block_cache_stage_bandwidth_throttle_mb),
      waiting_(false),
      throttle_(std::make_unique<dingofs::utils::LeakyBucket>()),
      timer_(std::make_unique<base::timer::TimerImpl>()) {
  throttle_->SetLimit(current_bandwidth_throttle_mb_ * kMiB, 0, 0);
}

void BlockCacheThrottle::Start() {
  CHECK(timer_->Start());
  timer_->Add([this] { UpdateThrottleParam(); }, 100);
}

void BlockCacheThrottle::Stop() { timer_->Stop(); }

bool BlockCacheThrottle::Add(uint64_t stage_bytes) {
  if (!FLAGS_block_cache_stage_bandwidth_throttle_enable) {
    return false;
  }

  std::lock_guard<bthread::Mutex> lk(mutex_);
  if (waiting_) {
    return true;
  }

  // BlockCacheThrottleClosure will reset overflow flag
  auto* done = new BlockCacheThrottleClosure(this);
  waiting_ = throttle_->Add(stage_bytes, done);
  return waiting_;
}

void BlockCacheThrottle::Reset() {
  std::lock_guard<bthread::Mutex> lk(mutex_);
  waiting_ = false;
}

void BlockCacheThrottle::UpdateThrottleParam() {
  if (FLAGS_block_cache_stage_bandwidth_throttle_mb !=
      current_bandwidth_throttle_mb_) {
    current_bandwidth_throttle_mb_ =
        FLAGS_block_cache_stage_bandwidth_throttle_mb;

    std::lock_guard<bthread::Mutex> lk(mutex_);
    throttle_->SetLimit(current_bandwidth_throttle_mb_ * kMiB, 0, 0);
  }
  timer_->Add([this] { UpdateThrottleParam(); }, 100);
}

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs
