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

#include "cache/blockcache/admission.h"

#include <absl/hash/hash.h>
#include <brpc/reloadable_flags.h>

#include <algorithm>

#include "common/const.h"

namespace dingofs {
namespace cache {

DEFINE_bool(cache_admit_second_hit, false,
            "admit a block to the cache disk only on its second access "
            "within a rotating one-hour window (halves flash writes on "
            "one-hit-wonder heavy workloads)");
DEFINE_validator(cache_admit_second_hit, brpc::PassValidate);

DEFINE_uint32(cache_admit_write_budget_mbps, 0,
              "target cache-write rate in MiB/s enforced by probabilistic "
              "admission (0 = unlimited)");
DEFINE_validator(cache_admit_write_budget_mbps, brpc::PassValidate);

bool AdmissionController::Admit(const Config& config, uint64_t now_s,
                                const CacheKey& key, BlockSource source,
                                size_t size) {
  if (!config.second_hit && config.write_budget_mbps == 0) {
    return true;  // both mechanisms off: zero overhead
  }

  bool ok = source == BlockSource::kWarmup;  // explicit intent always admits
  if (!ok) {
    if (config.second_hit && !PassSecondHit(now_s, key)) {
      rejects_second_hit_++;
      return false;
    }
    if (!PassWriteBudget(config, now_s, size)) {
      rejects_write_budget_++;
      return false;
    }
  }

  accepts_++;
  budget_window_bytes_ += size;
  return true;
}

bool AdmissionController::PassSecondHit(uint64_t now_s, const CacheKey& key) {
  // two sets rotate per window: the current one collects first touches, the
  // previous one keeps lookups alive, so "seen recently" covers 1-2 windows
  uint64_t epoch = now_s / kDoorkeeperWindowS;
  if (epoch != seen_epoch_) {
    if (epoch - seen_epoch_ >= 2) {
      seen_[(epoch + 1) % 2].clear();  // both stale after a long quiet gap
    }
    seen_[epoch % 2].clear();  // held two-window-old entries until now
    seen_epoch_ = epoch;
  }

  uint64_t fingerprint = absl::Hash<std::string>{}(key.Filename());
  if (seen_[0].count(fingerprint) != 0 || seen_[1].count(fingerprint) != 0) {
    return true;  // second touch within the window
  }
  seen_[epoch % 2].insert(fingerprint);
  return false;
}

bool AdmissionController::PassWriteBudget(const Config& config, uint64_t now_s,
                                          size_t /*size*/) {
  if (config.write_budget_mbps == 0) {
    return true;
  }

  if (now_s - budget_window_start_ >= kBudgetWindowS) {
    double observed = budget_window_bytes_ * 1.0 / kBudgetWindowS;  // bytes/s
    double budget = config.write_budget_mbps * 1.0 * kMiB;
    if (observed > 0) {
      // converge towards the budget, clamped to +-25% per step (CacheLib)
      double factor = std::clamp(budget / observed, 0.75, 1.25);
      probability_ = std::clamp(probability_ * factor, 0.05, 1.0);
    } else {
      probability_ = std::min(probability_ * 1.25, 1.0);
    }
    budget_window_start_ = now_s;
    budget_window_bytes_ = 0;
  }

  return std::uniform_real_distribution<double>(0.0, 1.0)(rng_) <
         probability_;
}

void AdmissionController::Reset() {
  seen_[0].clear();
  seen_[1].clear();
  seen_epoch_ = 0;
  probability_ = 1.0;
  budget_window_start_ = 0;
  budget_window_bytes_ = 0;
  accepts_ = 0;
  rejects_second_hit_ = 0;
  rejects_write_budget_ = 0;
}

}  // namespace cache
}  // namespace dingofs
