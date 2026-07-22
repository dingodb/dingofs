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

/*
 * Project: DingoFS
 * Created Date: 2026-07-22
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/eviction_guard.h"

#include <brpc/reloadable_flags.h>

#include <algorithm>

namespace dingofs {
namespace cache {

DEFINE_uint32(warmup_protect_s, 0,
              "protection lease for warmed-up blocks in seconds; a protected "
              "block is skipped by eviction until the lease expires "
              "(0 = no protection)");
DEFINE_validator(warmup_protect_s, brpc::PassValidate);

DEFINE_double(warmup_protect_max_ratio, 0.5,
              "cap of protected bytes as a fraction of cache capacity; above "
              "it protected blocks are evicted oldest-first as usual");
DEFINE_validator(warmup_protect_max_ratio, brpc::PassValidate);

void EvictionGuard::OnAdd(const Config& config, uint64_t now_s,
                          CacheValue* value) {
  if (value->source == static_cast<uint8_t>(BlockSource::kWarmup) &&
      config.protect_s > 0) {
    value->protect_until = now_s + config.protect_s;
    protected_bytes_ += value->size;
  }
}

void EvictionGuard::OnRemove(const CacheValue& value) {
  // every block accounted by OnAdd carries a non-zero lease; expired leases
  // are only released here, so protected_bytes_ overstates briefly but can
  // never leak
  if (value.protect_until != 0) {
    protected_bytes_ -= std::min<uint64_t>(protected_bytes_, value.size);
  }
}

EvictionPolicy::FilterFunc EvictionGuard::Wrap(
    const Config& config, uint64_t now_s, EvictionPolicy::FilterFunc base) {
  bool force = force_next_round_;
  force_next_round_ = false;
  if (force) {
    force_rounds_++;
  }

  uint64_t quota = capacity_bytes_ * config.max_ratio;
  return [this, base = std::move(base), now_s, quota,
          force](const CacheValue& value) {
    if (!force && value.protect_until > now_s && protected_bytes_ <= quota) {
      protect_skips_++;
      return FilterStatus::kSkip;
    }
    return base(value);
  };
}

void EvictionGuard::OnRoundEnd(bool under_pressure, uint64_t freed_bytes) {
  force_next_round_ =
      under_pressure && freed_bytes == 0 && protected_bytes_ > 0;
}

void EvictionGuard::Reset() {
  protected_bytes_ = 0;
  protect_skips_ = 0;
  force_rounds_ = 0;
  force_next_round_ = false;
}

}  // namespace cache
}  // namespace dingofs
