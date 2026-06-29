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

#include "cache/tools/bench/common/stats.h"

namespace dingofs {
namespace cache {
namespace bench {

const uint64_t kDistThresholdsUs[kDistBuckets] = {
    100, 250, 500, 1000, 2500, 5000, 10000, UINT64_MAX};
const char* const kDistLabels[kDistBuckets] = {
    "<100us", "<250us", "<500us", "<1ms", "<2.5ms", "<5ms", "<10ms", ">=10ms"};

namespace {
size_t DistBucket(uint64_t latency_us) {
  for (size_t i = 0; i < kDistBuckets; ++i) {
    if (latency_us < kDistThresholdsUs[i]) {
      return i;
    }
  }
  return kDistBuckets - 1;
}
}  // namespace

Stats::Stats(uint32_t workers) {
  slots_.reserve(workers);
  for (uint32_t i = 0; i < workers; ++i) {
    slots_.push_back(std::make_unique<Slot>());
  }
}

void Stats::Record(uint32_t worker, uint64_t op_bytes, uint64_t latency_us,
                   bool ok) {
  auto& s = *slots_[worker];
  std::lock_guard<bthread::Mutex> lock(s.mutex);
  ++s.ops;
  if (ok) {
    ++s.succeeded;
    s.bytes += op_bytes;
    s.hist.Add(latency_us);
    ++s.dist[DistBucket(latency_us)];
  } else {
    ++s.failed;
  }
}

Aggregated Stats::Aggregate() const {
  Aggregated out;
  for (const auto& s : slots_) {
    std::lock_guard<bthread::Mutex> lock(s->mutex);
    out.ops += s->ops;
    out.succeeded += s->succeeded;
    out.failed += s->failed;
    out.bytes += s->bytes;
    out.hist.Merge(s->hist);
    for (size_t i = 0; i < kDistBuckets; ++i) {
      out.dist[i] += s->dist[i];
    }
  }
  return out;
}

void Stats::Reset() {
  for (const auto& s : slots_) {
    std::lock_guard<bthread::Mutex> lock(s->mutex);
    s->ops = 0;
    s->succeeded = 0;
    s->failed = 0;
    s->bytes = 0;
    s->hist.Clear();
    s->dist.fill(0);
  }
}

}  // namespace bench
}  // namespace cache
}  // namespace dingofs
