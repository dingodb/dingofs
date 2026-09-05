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

#ifndef DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_COLLECTOR_H_
#define DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_COLLECTOR_H_

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace dingofs {
namespace blockcache {

class Stat {
 public:
  Stat() = default;

  void Add(uint64_t bytes, uint64_t latency_ns);
  void Merge(const Stat& other);

  uint64_t IOPS(uint64_t interval_us) const;
  uint64_t Bandwidth(uint64_t interval_us) const;

  // latencies in nanoseconds
  uint64_t AvgLat() const;
  uint64_t MaxLat() const;
  uint64_t MinLat() const;

  uint64_t Count() const;

 private:
  uint64_t count_{0};
  uint64_t total_bytes_{0};
  uint64_t max_latency_ns_{0};
  uint64_t min_latency_ns_{0};
  uint64_t total_latency_ns_{0};
};

// Every worker owns one slot and updates it under the slot mutex on each
// completion; the reporter drains all slots on its tick. This keeps the hot
// path free of any shared queue, which used to pin a core above ~2.5M op/s.
class Collector {
 public:
  class Slot {
   public:
    void Add(uint64_t bytes, uint64_t latency_ns) {
      std::lock_guard<std::mutex> lock(mutex_);
      stat_.Add(bytes, latency_ns);
    }

    void Drain(Stat* into) {
      std::lock_guard<std::mutex> lock(mutex_);
      into->Merge(stat_);
      stat_ = Stat();
    }

   private:
    std::mutex mutex_;
    Stat stat_;
  };

  explicit Collector(uint32_t slots);

  Slot* SlotAt(uint32_t index) { return slots_[index].get(); }
  void Drain(Stat* into);

 private:
  std::vector<std::unique_ptr<Slot>> slots_;
};

using CollectorSPtr = std::shared_ptr<Collector>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_TOOLS_BENCHMARK_COLLECTOR_H_
