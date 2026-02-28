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

#ifndef DINGOFS_COMMON_BLOCK_ACCESS_BLOCK_ACCESS_BENCH_H_
#define DINGOFS_COMMON_BLOCK_ACCESS_BLOCK_ACCESS_BENCH_H_

#include <atomic>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "common/blockaccess/block_accesser.h"
#include "common/status.h"

// Performance optimization macros from elbencho
// Branch prediction hints to help compiler optimize hot paths
#ifdef __GNUC__
#define IF_UNLIKELY(condition) if (__builtin_expect((condition), 0))
#define IF_LIKELY(condition) if (__builtin_expect(!!(condition), 1))
#else
#define IF_UNLIKELY(condition) if (condition)
#define IF_LIKELY(condition) if (condition)
#endif

#define LATHISTO_BUCKETFRACTION 4
#define LATHISTO_MAXLOG2MICROSEC 28
#define LATHISTO_NUMBUCKETS (LATHISTO_MAXLOG2MICROSEC * LATHISTO_BUCKETFRACTION)

namespace dingofs {
namespace blockaccess {

class LatencyHistogram {
 public:
  LatencyHistogram() : buckets(LATHISTO_NUMBUCKETS, 0) {}

  // Inline hot path for better performance
  __attribute__((always_inline)) void addLatency(uint64_t latencyMicroSec) {
    // Update live stats with relaxed memory ordering for performance
    numStoredValuesLive.fetch_add(1, std::memory_order_relaxed);
    numMicroSecsTotalLive.fetch_add(latencyMicroSec, std::memory_order_relaxed);

    numStoredValues++;
    numMicroSecTotal += latencyMicroSec;

    // Use branch prediction hints for min/max (typically not the extreme
    // values)
    IF_UNLIKELY(latencyMicroSec < minMicroSecLat) {
      minMicroSecLat = latencyMicroSec;
    }
    IF_UNLIKELY(latencyMicroSec > maxMicroSecLat) {
      maxMicroSecLat = latencyMicroSec;
    }

    size_t bucketIndex;
    // log2(0) does not exist, so special case
    IF_UNLIKELY(!latencyMicroSec) { bucketIndex = 0; }
    else {
      bucketIndex = static_cast<size_t>(std::log2(latencyMicroSec)) *
                    LATHISTO_BUCKETFRACTION;
    }

    IF_UNLIKELY(bucketIndex >= LATHISTO_NUMBUCKETS) {
      bucketIndex = LATHISTO_NUMBUCKETS - 1;
    }

    buckets[bucketIndex]++;
  }

  void addAndResetAverageLiveMicroSec(uint64_t& outNumStoredValues,
                                      uint64_t& outNumMicroSecsTotal) {
    outNumStoredValues += numStoredValuesLive.load(std::memory_order_relaxed);
    outNumMicroSecsTotal +=
        numMicroSecsTotalLive.load(std::memory_order_relaxed);
    numStoredValuesLive = 0;
    numMicroSecsTotalLive = 0;
  }

  size_t getNumStoredValues() const { return numStoredValues; }

  size_t getMinMicroSecLat() const { return minMicroSecLat; }

  size_t getMaxMicroSecLat() const { return maxMicroSecLat; }

  size_t getAverageMicroSec() const {
    return numStoredValues ? (numMicroSecTotal / numStoredValues) : 0;
  }

  void reset() {
    // Use memset for faster bucket clearing
    std::memset(buckets.data(), 0, buckets.size() * sizeof(uint64_t));
    numStoredValues = 0;
    numMicroSecTotal = 0;
    minMicroSecLat = std::numeric_limits<uint64_t>::max();
    maxMicroSecLat = 0;
    numStoredValuesLive.store(0, std::memory_order_relaxed);
    numMicroSecsTotalLive.store(0, std::memory_order_relaxed);
  }

  double getPercentile(double percentage) const {
    size_t numValuesSoFar = 0;
    double log2BucketSize = 1.0 / LATHISTO_BUCKETFRACTION;

    for (size_t bucketIndex = 0; bucketIndex < LATHISTO_NUMBUCKETS;
         bucketIndex++) {
      numValuesSoFar += buckets[bucketIndex];
      double percentileSoFar =
          static_cast<double>(numValuesSoFar) / numStoredValues;
      if (percentileSoFar >= (percentage / 100)) {
        double bucketMicroSec = std::pow(2, (bucketIndex + 1) * log2BucketSize);
        return bucketMicroSec;
      }
    }
    return 0;
  }

  bool getHistogramExceeded() const {
    return buckets[LATHISTO_NUMBUCKETS - 1] != 0;
  }

  LatencyHistogram& operator+=(const LatencyHistogram& rhs) {
    // Use pointer arithmetic for faster iteration
    const uint64_t* src = rhs.buckets.data();
    uint64_t* dst = buckets.data();
    for (size_t bucketIndex = 0; bucketIndex < LATHISTO_NUMBUCKETS;
         ++bucketIndex) {
      dst[bucketIndex] += src[bucketIndex];
    }
    numStoredValues += rhs.numStoredValues;
    numMicroSecTotal += rhs.numMicroSecTotal;
    // Use branch prediction hints
    IF_UNLIKELY(rhs.minMicroSecLat < minMicroSecLat) {
      minMicroSecLat = rhs.minMicroSecLat;
    }
    IF_UNLIKELY(rhs.maxMicroSecLat > maxMicroSecLat) {
      maxMicroSecLat = rhs.maxMicroSecLat;
    }
    return *this;
  }

 private:
  uint64_t numStoredValues{0};
  uint64_t numMicroSecTotal{0};
  uint64_t minMicroSecLat{std::numeric_limits<uint64_t>::max()};
  uint64_t maxMicroSecLat{0};
  std::vector<uint64_t> buckets;
  std::atomic_uint64_t numStoredValuesLive{0};
  std::atomic_uint64_t numMicroSecsTotalLive{0};
};

struct BenchOptions {
  AccesserType accesser_type{kLocalFile};
  std::string data_path;
  std::string s3_bucket;
  std::string s3_endpoint;
  std::string s3_ak;
  std::string s3_sk;

  bool s3_use_crt_client{true};
  bool s3_use_thread_pool{false};
  int s3_async_thread_num{16};

  std::string rados_pool;
  std::string rados_mon_host;
  std::string rados_cluster_name;
  std::string rados_user_name;
  std::string rados_key;

  uint32_t num_ops{10000};
  uint32_t num_ops_per_thread{10000};
  uint32_t threads{1};
  uint32_t block_size{4194304};

  bool run_put{false};
  bool run_async_put{false};
  bool run_get{false};
  bool run_async_get{false};
  bool run_range{false};

  // Performance options
  bool bind_to_cpu{false};   // Enable CPU affinity binding
  bool skip_prefill{false};  // Skip data prefill (use when data already exists)
  uint32_t prefill_delay_sec{0};  // Delay seconds after prefill before
                                  // benchmark (for storage backend to settle)
};

struct BenchResult {
  std::string op_name;
  uint32_t total_ops;
  uint64_t total_bytes;
  double total_time_ms;
  double avg_latency_us;
  double p50_latency_us;
  double p95_latency_us;
  double p99_latency_us;
  double throughput_ops;
  double throughput_mb;

  void Print() const;
};

class BlockAccessBench {
 public:
  explicit BlockAccessBench(const BenchOptions& options);

  Status Init();

  void Run();

  const std::vector<BenchResult>& GetResults() const { return results_; }

 private:
  void RunPutBench();
  void RunAsyncPutBench();
  void RunGetBench();
  void RunAsyncGetBench();
  void RunRangeBench();

  void GenerateTestData();
  void PreGenerateKeys();
  Status PrefillDataForGet();  // 为 GET 测试预写入数据

  double GetPercentile(const LatencyHistogram& histogram, double p);

  void WaitAsync();

  BenchOptions options_;
  BlockAccesserSPtr accesser_;
  // Per-thread buffers to avoid false sharing in multi-threaded scenarios
  std::vector<std::vector<char>> test_data_pool_;
  std::vector<std::string> keys_;
  std::vector<BenchResult> results_;

  std::atomic<uint32_t> pending_ops_{0};
  std::mutex mtx_;
  std::condition_variable cv_;
};

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_ACCESS_BLOCK_ACCESS_BENCH_H_
