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

#include "common/blockaccess/bench/block_access_bench.h"

#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <thread>
#include <vector>

// Platform-specific includes for CPU affinity
#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

// Helper function to set CPU affinity for a thread
static void SetThreadAffinity(uint32_t thread_id) {
#ifdef __linux__
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(thread_id % std::thread::hardware_concurrency(), &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
#endif
}

namespace dingofs {
namespace blockaccess {

using Timer = std::chrono::steady_clock;
using TimePoint = std::chrono::time_point<Timer>;
using Duration = std::chrono::duration<double, std::milli>;

// Branch prediction hints (duplicated here for compilation unit)
#ifdef __GNUC__
#define IF_UNLIKELY(condition) if (__builtin_expect((condition), 0))
#define IF_LIKELY(condition) if (__builtin_expect(!!(condition), 1))
#else
#define IF_UNLIKELY(condition) if (condition)
#define IF_LIKELY(condition) if (condition)
#endif

class FastSemaphore {
 public:
  explicit FastSemaphore(int permits) : count_(permits) {}

  __attribute__((always_inline)) void acquire() {
    // Fast path: try to decrement without blocking
    int old = count_.fetch_sub(1, std::memory_order_acquire);
    IF_LIKELY(old > 0) {
      return;  // Success, no need to block
    }

    // Slow path: need to wait
    slowAcquire();
  }

  __attribute__((always_inline)) void release() {
    // Fast path: try to increment and notify
    int old = count_.fetch_add(1, std::memory_order_release);
    IF_UNLIKELY(old < 0) {
      // Someone was waiting, wake them up
      std::lock_guard<std::mutex> lock(mtx_);
      cv_.notify_one();
    }
  }

 private:
  void slowAcquire() {
    // Spin briefly before blocking (reduces context switch overhead)
    for (int spin = 0; spin < 1000; ++spin) {
      if (count_.load(std::memory_order_relaxed) >= 0) {
        return;
      }
// Pause instruction reduces power consumption and improves SMT
#if defined(__x86_64__) || defined(__i386__)
      __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__)
      __asm__ volatile("yield" ::: "memory");
#endif
    }

    // Block on condition variable
    std::unique_lock<std::mutex> lock(mtx_);
    cv_.wait(lock,
             [this] { return count_.load(std::memory_order_relaxed) >= 0; });
  }

  alignas(64) std::atomic<int> count_;  // Cache line aligned
  std::mutex mtx_;
  std::condition_variable cv_;
};

class ProgressTracker {
 public:
  ProgressTracker(uint32_t total_ops, const std::string& op_name,
                  uint64_t block_size)
      : total_ops_(total_ops),
        op_name_(op_name),
        block_size_(block_size),
        completed_(0),
        running_(true),
        finished_(false),
        start_time_(Timer::now()) {
    print_thread_ = std::thread([this]() {
      while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        if (!finished_.load(std::memory_order_relaxed)) {
          PrintProgress();
        }
      }
    });
  }

  ~ProgressTracker() {
    running_ = false;
    if (print_thread_.joinable()) {
      print_thread_.join();
    }
  }

  void Increment() { completed_.fetch_add(1, std::memory_order_relaxed); }

  void PrintProgress() {
    auto now = Timer::now();
    auto elapsed =
        std::chrono::duration_cast<Duration>(now - start_time_).count();
    uint32_t completed = completed_.load(std::memory_order_relaxed);
    if (elapsed <= 0) elapsed = 0.001;
    double ops_per_sec = completed / (elapsed / 1000.0);
    double mb_per_sec =
        (completed * block_size_) / 1024.0 / 1024.0 / (elapsed / 1000.0);
    double progress = static_cast<double>(completed) / total_ops_ * 100;
    std::cout << "\r  " << op_name_ << ": " << completed << "/" << total_ops_
              << " (" << std::fixed << std::setprecision(1) << progress << "%) "
              << std::fixed << std::setprecision(1) << ops_per_sec << " ops/s "
              << std::fixed << std::setprecision(2) << mb_per_sec << " MB/s  ";
    std::cout.flush();
  }

  void Finish() {
    finished_.store(true, std::memory_order_relaxed);
    running_ = false;
    if (print_thread_.joinable()) {
      print_thread_.join();
    }
    auto now = Timer::now();
    auto elapsed =
        std::chrono::duration_cast<Duration>(now - start_time_).count();
    uint32_t completed = completed_.load(std::memory_order_relaxed);
    double ops_per_sec = completed / (elapsed / 1000.0);
    double mb_per_sec =
        (completed * block_size_) / 1024.0 / 1024.0 / (elapsed / 1000.0);
    std::cout << "\r  " << op_name_ << ": " << completed << "/" << total_ops_
              << " (100%) " << ops_per_sec << " ops/s " << mb_per_sec << " MB/s"
              << '\n';
  }

  uint32_t GetCompleted() const {
    return completed_.load(std::memory_order_relaxed);
  }

 private:
  uint32_t total_ops_;
  std::string op_name_;
  uint64_t block_size_;
  std::atomic<uint32_t> completed_;
  std::atomic<bool> running_;
  std::atomic<bool> finished_;
  TimePoint start_time_;
  std::thread print_thread_;
};

void BenchResult::Print() const {
  std::cout << "=== " << op_name << " ===" << '\n';
  std::cout << "  Total ops: " << total_ops << '\n';
  std::cout << "  Total bytes: " << total_bytes << " ("
            << (total_bytes / 1024.0 / 1024.0) << " MB)" << '\n';
  std::cout << "  Total time: " << total_time_ms << " ms" << '\n';
  std::cout << "  Avg latency: " << avg_latency_us << " ms" << '\n';
  std::cout << "  P50 latency: " << p50_latency_us << " ms" << '\n';
  std::cout << "  P95 latency: " << p95_latency_us << " ms" << '\n';
  std::cout << "  P99 latency: " << p99_latency_us << " ms" << '\n';
  std::cout << "  Throughput: " << throughput_ops << " ops/s" << '\n';
  std::cout << "  Throughput: " << throughput_mb << " MB/s" << '\n';
}

BlockAccessBench::BlockAccessBench(const BenchOptions& options)
    : options_(options), pending_ops_(0) {}

Status BlockAccessBench::Init() {
  BlockAccessOptions access_options;
  access_options.type = options_.accesser_type;

  switch (options_.accesser_type) {
    case AccesserType::kS3:
      access_options.s3_options.s3_info.bucket_name = options_.s3_bucket;
      access_options.s3_options.s3_info.endpoint = options_.s3_endpoint;
      access_options.s3_options.s3_info.ak = options_.s3_ak;
      access_options.s3_options.s3_info.sk = options_.s3_sk;
      InitBlockAccessOption(&access_options);
      access_options.s3_options.aws_sdk_config.use_crt_client =
          options_.s3_use_crt_client;
      access_options.s3_options.aws_sdk_config.use_thread_pool =
          options_.s3_use_thread_pool;
      access_options.s3_options.aws_sdk_config.async_thread_num =
          options_.s3_async_thread_num;
      break;
    case AccesserType::kRados:
      access_options.rados_options.pool_name = options_.rados_pool;
      access_options.rados_options.mon_host = options_.rados_mon_host;
      access_options.rados_options.cluster_name = options_.rados_cluster_name;
      access_options.rados_options.user_name = options_.rados_user_name;
      access_options.rados_options.key = options_.rados_key;
      InitBlockAccessOption(&access_options);
      break;
    case AccesserType::kLocalFile:
      access_options.file_options.path = options_.data_path;
      InitBlockAccessOption(&access_options);
      break;
    default:
      InitBlockAccessOption(&access_options);
      break;
  }

  accesser_ = NewShareBlockAccesser(access_options);
  if (accesser_ == nullptr) {
    return Status::Internal("failed to create accesser");
  }

  auto s = accesser_->Init();
  if (!s.ok()) {
    return s;
  }

  // Warm up connection: check if container exists (triggers DNS/TCP/TLS
  // handshake)
  if (!accesser_->ContainerExist()) {
    LOG(WARNING)
        << "[bench] Container/bucket does not exist or connection failed";
  } else {
    VLOG(1) << "[bench] Connection warmed up successfully";
  }

  GenerateTestData();
  PreGenerateKeys();

  return Status::OK();
}

void BlockAccessBench::GenerateTestData() {
  // Pre-allocate per-thread buffers to avoid false sharing
  // Each buffer is cache-line aligned for better performance
  const size_t cache_line_size = 64;
  const size_t aligned_block_size =
      (options_.block_size + cache_line_size - 1) & ~(cache_line_size - 1);

  test_data_pool_.resize(options_.threads);
  for (uint32_t t = 0; t < options_.threads; ++t) {
    // Allocate aligned memory for DMA-friendly access
    test_data_pool_[t].resize(aligned_block_size);
    // Fill with pattern that includes thread ID for debugging
    uint8_t pattern = static_cast<uint8_t>(t);
    std::generate(test_data_pool_[t].begin(),
                  test_data_pool_[t].begin() + options_.block_size,
                  [&pattern]() { return pattern++; });
  }
}

void BlockAccessBench::PreGenerateKeys() {
  keys_.reserve(options_.num_ops);
  for (uint32_t t = 0; t < options_.threads; ++t) {
    for (uint32_t i = 0; i < options_.num_ops_per_thread; ++i) {
      keys_.emplace_back("block_" + std::to_string(t) + "_" +
                         std::to_string(i));
    }
  }
}

Status BlockAccessBench::PrefillDataForGet() {
  std::cout << "Pre-filling data for GET benchmark..." << '\n';

  std::atomic<uint32_t> completed{0};
  std::vector<std::thread> threads;
  uint32_t num_threads = options_.threads;
  uint32_t num_ops_per_thread = options_.num_ops_per_thread;

  auto start = Timer::now();

  // 并行预写入，提高预填充速度
  threads.reserve(num_threads);
  for (uint32_t t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, t, num_ops_per_thread, &completed]() {
      for (uint32_t i = 0; i < num_ops_per_thread; ++i) {
        auto key = keys_[(t * num_ops_per_thread) + i];
        const auto& buffer = test_data_pool_[t % options_.threads];
        auto s = accesser_->Put(key, buffer.data(), options_.block_size);
        if (!s.ok()) {
          LOG(ERROR) << "Failed to prefill key " << key << ": " << s.ToString();
          return;
        }
        uint32_t current = ++completed;
        if (current % 100 == 0 || current == options_.num_ops) {
          std::cout << "  Prefilled " << current << "/" << options_.num_ops
                    << " blocks\r";
          std::cout.flush();
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  auto end = Timer::now();
  double prefill_time_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();
  double throughput_mb =
      (static_cast<double>(options_.num_ops) * options_.block_size) / 1024.0 /
      1024.0 / (prefill_time_ms / 1000.0);

  std::cout << "\nPre-fill complete in " << prefill_time_ms << " ms ("
            << throughput_mb << " MB/s)" << '\n';
  return Status::OK();
}

double BlockAccessBench::GetPercentile(const LatencyHistogram& histogram,
                                       double p) {
  if (histogram.getNumStoredValues() == 0) return 0;
  return histogram.getPercentile(p);
}

void BlockAccessBench::WaitAsync() {
  std::unique_lock<std::mutex> lock(mtx_);
  cv_.wait(lock, [this] { return pending_ops_ == 0; });
}

void BlockAccessBench::RunPutBench() {
  std::vector<std::thread> threads;
  uint32_t num_threads = options_.threads;
  uint32_t num_ops_per_thread = options_.num_ops_per_thread;
  uint32_t total_ops = options_.num_ops;

  std::vector<LatencyHistogram> thread_histograms(num_threads);
  ProgressTracker progress(total_ops, "PUT", options_.block_size);

  auto start = Timer::now();

  threads.reserve(num_threads);
  for (uint32_t t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, t, num_ops_per_thread, &thread_histograms,
                          &progress]() {
      // Set CPU affinity if enabled
      if (options_.bind_to_cpu) {
        SetThreadAffinity(t);
      }
      auto& histogram = thread_histograms[t];
      // Use thread-local buffer to avoid false sharing
      const auto& thread_buffer = test_data_pool_[t];
      for (uint32_t i = 0; i < num_ops_per_thread; ++i) {
        auto key = keys_[(t * num_ops_per_thread) + i];
        auto op_start = Timer::now();
        auto s = accesser_->Put(key, thread_buffer.data(), options_.block_size);
        auto op_end = Timer::now();
        double latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(op_end -
                                                                  op_start)
                .count();
        histogram.addLatency(static_cast<uint64_t>(latency_us));
        progress.Increment();
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  progress.Finish();

  auto end = Timer::now();
  double total_time_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();

  LatencyHistogram combined_histogram;
  for (auto& histo : thread_histograms) {
    combined_histogram += histo;
  }

  double avg_latency = combined_histogram.getAverageMicroSec() / 1000.0;

  BenchResult result;
  result.op_name = "PUT";
  result.total_ops = total_ops;
  result.total_bytes = static_cast<uint64_t>(total_ops) * options_.block_size;
  result.total_time_ms = total_time_ms;
  result.avg_latency_us = avg_latency;
  result.p50_latency_us = GetPercentile(combined_histogram, 50) / 1000.0;
  result.p95_latency_us = GetPercentile(combined_histogram, 95) / 1000.0;
  result.p99_latency_us = GetPercentile(combined_histogram, 99) / 1000.0;
  result.throughput_ops = total_ops / (result.total_time_ms / 1000.0);
  result.throughput_mb =
      result.total_bytes / 1024.0 / 1024.0 / (result.total_time_ms / 1000.0);

  results_.push_back(result);
}

void BlockAccessBench::RunAsyncPutBench() {
  std::vector<LatencyHistogram> thread_histograms(options_.threads);
  uint32_t num_threads = options_.threads;
  uint32_t num_ops_per_thread = options_.num_ops_per_thread;
  uint32_t total_ops = options_.num_ops;

  FastSemaphore semaphore(num_threads);
  pending_ops_ = total_ops;

  ProgressTracker progress(total_ops, "ASYNC_PUT", options_.block_size);

  auto start = Timer::now();

  for (uint32_t i = 0; i < total_ops; ++i) {
    semaphore.acquire();

    uint32_t thread_id = i / num_ops_per_thread;
    auto key = keys_[i];

    // Use thread-local buffer to avoid false sharing
    const auto& thread_buffer = test_data_pool_[thread_id];

    auto context = std::make_shared<PutObjectAsyncContext>(key);
    context->buffer = thread_buffer.data();
    context->buffer_size = options_.block_size;
    context->start_time = std::chrono::duration_cast<std::chrono::microseconds>(
                              Timer::now().time_since_epoch())
                              .count();

    context->cb = [this, thread_id, &thread_histograms, &semaphore, &progress](
                      const std::shared_ptr<PutObjectAsyncContext>& ctx) {
      auto end = std::chrono::duration_cast<std::chrono::microseconds>(
                     Timer::now().time_since_epoch())
                     .count();
      thread_histograms[thread_id].addLatency(
          static_cast<uint64_t>(end - ctx->start_time));
      pending_ops_--;
      semaphore.release();
      progress.Increment();
      if (pending_ops_ == 0) {
        cv_.notify_one();
      }
    };

    accesser_->AsyncPut(key, context);
  }

  WaitAsync();
  progress.Finish();

  auto end = Timer::now();
  double total_time_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();

  LatencyHistogram combined_histogram;
  for (auto& histo : thread_histograms) {
    combined_histogram += histo;
  }

  double avg_latency = combined_histogram.getAverageMicroSec() / 1000.0;

  BenchResult result;
  result.op_name = "ASYNC_PUT";
  result.total_ops = total_ops;
  result.total_bytes = static_cast<uint64_t>(total_ops) * options_.block_size;
  result.total_time_ms = total_time_ms;
  result.avg_latency_us = avg_latency;
  result.p50_latency_us = GetPercentile(combined_histogram, 50) / 1000.0;
  result.p95_latency_us = GetPercentile(combined_histogram, 95) / 1000.0;
  result.p99_latency_us = GetPercentile(combined_histogram, 99) / 1000.0;
  result.throughput_ops = total_ops / (result.total_time_ms / 1000.0);
  result.throughput_mb =
      result.total_bytes / 1024.0 / 1024.0 / (result.total_time_ms / 1000.0);

  results_.push_back(result);
}

void BlockAccessBench::RunGetBench() {
  std::vector<std::thread> threads;
  uint32_t num_threads = options_.threads;
  uint32_t num_ops_per_thread = options_.num_ops_per_thread;
  uint32_t total_ops = options_.num_ops;

  std::vector<LatencyHistogram> thread_histograms(num_threads);

  // 使用缓存行对齐的预分配内存池，避免 false sharing 和动态分配
  const size_t cache_line_size = 64;
  const size_t aligned_block_size =
      (options_.block_size + cache_line_size - 1) & ~(cache_line_size - 1);
  std::vector<char> read_buffer_pool(num_threads * aligned_block_size);

  ProgressTracker progress(total_ops, "GET", options_.block_size);

  auto start = Timer::now();

  threads.reserve(num_threads);
  for (uint32_t t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, t, num_ops_per_thread, aligned_block_size,
                          &thread_histograms, &read_buffer_pool, &progress]() {
      // Set CPU affinity if enabled
      if (options_.bind_to_cpu) {
        SetThreadAffinity(t);
      }

      auto& histogram = thread_histograms[t];
      // 每个线程使用独立的缓存行对齐 buffer
      char* buffer = &read_buffer_pool[t * aligned_block_size];

      for (uint32_t i = 0; i < num_ops_per_thread; ++i) {
        auto key = keys_[(t * num_ops_per_thread) + i];
        auto op_start = Timer::now();
        // 使用 Range 接口读取完整 block，避免 std::string 内存分配
        auto s = accesser_->Range(key, 0, options_.block_size, buffer);
        auto op_end = Timer::now();
        double latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(op_end -
                                                                  op_start)
                .count();
        histogram.addLatency(static_cast<uint64_t>(latency_us));
        progress.Increment();
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  progress.Finish();

  auto end = Timer::now();
  double total_time_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();

  LatencyHistogram combined_histogram;
  for (auto& histo : thread_histograms) {
    combined_histogram += histo;
  }

  double avg_latency = combined_histogram.getAverageMicroSec() / 1000.0;

  BenchResult result;
  result.op_name = "GET";
  result.total_ops = total_ops;
  result.total_bytes = static_cast<uint64_t>(total_ops) * options_.block_size;
  result.total_time_ms = total_time_ms;
  result.avg_latency_us = avg_latency;
  result.p50_latency_us = GetPercentile(combined_histogram, 50) / 1000.0;
  result.p95_latency_us = GetPercentile(combined_histogram, 95) / 1000.0;
  result.p99_latency_us = GetPercentile(combined_histogram, 99) / 1000.0;
  result.throughput_ops = total_ops / (result.total_time_ms / 1000.0);
  result.throughput_mb =
      result.total_bytes / 1024.0 / 1024.0 / (result.total_time_ms / 1000.0);

  results_.push_back(result);
}

void BlockAccessBench::RunAsyncGetBench() {
  std::vector<LatencyHistogram> thread_histograms(options_.threads);
  uint32_t num_threads = options_.threads;
  uint32_t num_ops_per_thread = options_.num_ops_per_thread;
  uint32_t total_ops = options_.num_ops;

  FastSemaphore semaphore(num_threads);
  pending_ops_ = total_ops;

  std::vector<char*> read_buffers(num_threads);
  for (auto& buf : read_buffers) {
    buf = new char[options_.block_size];
  }

  ProgressTracker progress(total_ops, "ASYNC_GET", options_.block_size);

  auto start = Timer::now();

  for (uint32_t i = 0; i < total_ops; ++i) {
    semaphore.acquire();

    uint32_t thread_id = i / num_ops_per_thread;
    auto key = keys_[i];

    auto context = std::make_shared<GetObjectAsyncContext>(key);
    context->offset = 0;
    context->len = options_.block_size;
    context->buf = read_buffers[thread_id];
    context->start_time = std::chrono::duration_cast<std::chrono::microseconds>(
                              Timer::now().time_since_epoch())
                              .count();

    context->cb = [this, thread_id, &thread_histograms, &semaphore, &progress](
                      const std::shared_ptr<GetObjectAsyncContext>& ctx) {
      auto end = std::chrono::duration_cast<std::chrono::microseconds>(
                     Timer::now().time_since_epoch())
                     .count();
      thread_histograms[thread_id].addLatency(
          static_cast<uint64_t>(end - ctx->start_time));
      pending_ops_--;
      semaphore.release();
      progress.Increment();
      if (pending_ops_ == 0) {
        cv_.notify_one();
      }
    };

    accesser_->AsyncGet(key, context);
  }

  WaitAsync();
  progress.Finish();

  for (auto* buf : read_buffers) {
    delete[] buf;
  }

  auto end = Timer::now();
  double total_time_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();

  LatencyHistogram combined_histogram;
  for (auto& histo : thread_histograms) {
    combined_histogram += histo;
  }

  double avg_latency = combined_histogram.getAverageMicroSec() / 1000.0;

  BenchResult result;
  result.op_name = "ASYNC_GET";
  result.total_ops = total_ops;
  result.total_bytes = static_cast<uint64_t>(total_ops) * options_.block_size;
  result.total_time_ms = total_time_ms;
  result.avg_latency_us = avg_latency;
  result.p50_latency_us = GetPercentile(combined_histogram, 50) / 1000.0;
  result.p95_latency_us = GetPercentile(combined_histogram, 95) / 1000.0;
  result.p99_latency_us = GetPercentile(combined_histogram, 99) / 1000.0;
  result.throughput_ops = total_ops / (result.total_time_ms / 1000.0);
  result.throughput_mb =
      result.total_bytes / 1024.0 / 1024.0 / (result.total_time_ms / 1000.0);

  results_.push_back(result);
}

void BlockAccessBench::RunRangeBench() {
  std::vector<std::thread> threads;
  uint32_t num_threads = options_.threads;
  uint32_t num_ops_per_thread = options_.num_ops_per_thread;
  uint32_t total_ops = options_.num_ops;

  const size_t range_size = options_.block_size;

  const size_t cache_line_size = 64;
  const size_t aligned_range_size =
      (range_size + cache_line_size - 1) & ~(cache_line_size - 1);

  std::vector<LatencyHistogram> thread_histograms(num_threads);

  std::vector<char> range_buffer_pool(num_threads * aligned_range_size);

  ProgressTracker progress(total_ops, "RANGE", range_size);

  auto start = Timer::now();

  threads.reserve(num_threads);
  for (uint32_t t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, t, num_ops_per_thread, range_size,
                          aligned_range_size, &thread_histograms,
                          &range_buffer_pool, &progress]() {
      // Set CPU affinity if enabled
      if (options_.bind_to_cpu) {
        SetThreadAffinity(t);
      }

      auto& histogram = thread_histograms[t];
      // 每个线程使用独立的 buffer，对齐到缓存行
      char* buffer = &range_buffer_pool[t * aligned_range_size];

      for (uint32_t i = 0; i < num_ops_per_thread; ++i) {
        auto key = keys_[(t * num_ops_per_thread) + i];
        auto op_start = Timer::now();
        auto s = accesser_->Range(key, 0, range_size, buffer);
        auto op_end = Timer::now();
        double latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(op_end -
                                                                  op_start)
                .count();
        histogram.addLatency(static_cast<uint64_t>(latency_us));
        progress.Increment();
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  progress.Finish();

  auto end = Timer::now();
  double total_time_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();

  LatencyHistogram combined_histogram;
  for (auto& histo : thread_histograms) {
    combined_histogram += histo;
  }

  double avg_latency = combined_histogram.getAverageMicroSec() / 1000.0;

  BenchResult result;
  result.op_name = "RANGE";
  result.total_ops = total_ops;
  result.total_bytes = static_cast<uint64_t>(total_ops) * range_size;
  result.total_time_ms = total_time_ms;
  result.avg_latency_us = avg_latency;
  result.p50_latency_us = GetPercentile(combined_histogram, 50) / 1000.0;
  result.p95_latency_us = GetPercentile(combined_histogram, 95) / 1000.0;
  result.p99_latency_us = GetPercentile(combined_histogram, 99) / 1000.0;
  result.throughput_ops = total_ops / (result.total_time_ms / 1000.0);
  result.throughput_mb =
      result.total_bytes / 1024.0 / 1024.0 / (result.total_time_ms / 1000.0);

  results_.push_back(result);
}

void BlockAccessBench::Run() {
  std::cout << "Running benchmark..." << '\n';
  std::cout << "  Accesser type: " << AccesserType2Str(options_.accesser_type)
            << '\n';
  std::cout << "  Threads: " << options_.threads << '\n';
  std::cout << "  Num ops per thread: " << options_.num_ops_per_thread << '\n';
  std::cout << "  Total ops: " << options_.num_ops << '\n';
  std::cout << "  Block size: " << options_.block_size << '\n';

  if (options_.run_put) {
    std::cout << "\n--- PUT benchmark ---" << '\n';
    RunPutBench();
  }

  if (options_.run_async_put) {
    std::cout << "\n--- ASYNC PUT benchmark ---" << '\n';
    RunAsyncPutBench();
  }

  // 如果需要 prefill 数据，在测试开始前完成（不计入测试时间）
  if ((options_.run_get || options_.run_async_get || options_.run_range) &&
      !options_.skip_prefill) {
    auto s = PrefillDataForGet();
    if (!s.ok()) {
      std::cerr << "Prefill failed: " << s.ToString() << '\n';
      return;
    }
    // prefill 后可选延迟，让存储后端完成后台处理（如 MinIO flush 到磁盘）
    if (options_.prefill_delay_sec > 0) {
      std::cout << "Waiting " << options_.prefill_delay_sec
                << " seconds for storage backend to settle..." << '\n';
      std::this_thread::sleep_for(
          std::chrono::seconds(options_.prefill_delay_sec));
    }
  }

  if (options_.run_get) {
    std::cout << "\n--- GET benchmark ---" << '\n';
    RunGetBench();
  }

  if (options_.run_async_get) {
    std::cout << "\n--- ASYNC GET benchmark ---" << '\n';
    RunAsyncGetBench();
  }

  if (options_.run_range) {
    std::cout << "\n--- RANGE benchmark ---" << '\n';
    RunRangeBench();
  }

  std::cout << "\n=== RESULTS ===" << '\n';
  for (const auto& result : results_) {
    result.Print();
    std::cout << '\n';
  }
}

}  // namespace blockaccess
}  // namespace dingofs
