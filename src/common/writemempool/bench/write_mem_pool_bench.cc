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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pthread.h>
#include <sched.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "cache/common/memory_pool.h"
#include "common/writemempool/write_mem_pool.h"
#include "common/writemempool/write_page_pool.h"

DEFINE_uint32(threads, 1, "Number of allocation threads.");
DEFINE_uint32(iterations, 1000, "Iterations per thread.");
DEFINE_uint32(batch_pages, 16, "Pages acquired and released per iteration.");
DEFINE_uint32(alloc_batches_per_release, 1,
              "Allocation batches accumulated before one ReleaseBatch; use 4 "
              "to model four 1MiB writes filling one 4MiB block.");
DEFINE_uint64(page_size, 64 * 1024, "Page size in bytes.");
DEFINE_bool(touch_pages, true, "Memset every acquired page.");
DEFINE_string(implementation, "write_mem_pool",
              "write_mem_pool, write_page_pool, serialized_write_page_pool, "
              "or legacy_single.");
DEFINE_bool(pin_threads, false, "Pin worker N to logical CPU N.");
DEFINE_uint32(inflight_batches, 1,
              "Batches held before release; use > LLC/page batch for "
              "streaming-memory measurements.");
DEFINE_bool(cross_thread_release, false,
            "Release batches on separate callback threads.");
DEFINE_int64(pressure_free_pages, -1,
             "Pre-acquire pages so only this many remain free; -1 disables "
             "the exhaustion-pressure mode.");
DEFINE_uint32(latency_sample_stride, 100,
              "Record every Nth allocation latency; 0 disables sampling.");

namespace {

void PinToCpu(uint32_t cpu) {
  cpu_set_t cpus;
  CPU_ZERO(&cpus);
  CPU_SET(cpu, &cpus);
  const int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpus), &cpus);
  CHECK_EQ(rc, 0) << std::strerror(rc);
}

struct alignas(64) TransferSlot {
  // 0: producer may fill pages; 1: callback may release pages.
  std::atomic<uint32_t> state{0};
  size_t count{0};
  std::vector<char*> pages;
};

}  // namespace

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  CHECK_GT(FLAGS_threads, 0);
  CHECK_GT(FLAGS_iterations, 0);
  CHECK_GT(FLAGS_batch_pages, 0);
  CHECK_GT(FLAGS_alloc_batches_per_release, 0);
  CHECK_GT(FLAGS_inflight_batches, 0);
  CHECK_GE(FLAGS_pressure_free_pages, -1);
  CHECK_GE(FLAGS_page_size, sizeof(uint32_t));
  CHECK(FLAGS_implementation == "write_mem_pool" ||
        FLAGS_implementation == "write_page_pool" ||
        FLAGS_implementation == "serialized_write_page_pool" ||
        FLAGS_implementation == "legacy_single");

  CHECK_LE(
      static_cast<uint64_t>(FLAGS_batch_pages),
      std::numeric_limits<uint64_t>::max() / FLAGS_alloc_batches_per_release);
  const uint64_t pages_per_release = static_cast<uint64_t>(FLAGS_batch_pages) *
                                     FLAGS_alloc_batches_per_release;
  const uint64_t working_pages =
      static_cast<uint64_t>(FLAGS_threads) * pages_per_release;
  const uint64_t pool_pages = std::max<uint64_t>(
      4096, working_pages * std::max<uint32_t>(4, FLAGS_inflight_batches) * 2);
  CHECK_LE(pool_pages, INT64_MAX / FLAGS_page_size);

  dingofs::WritePagePoolUPtr page_pool;
  std::unique_ptr<dingofs::WriteMemPool> write_mem_pool;
  std::unique_ptr<dingofs::MemoryPool> legacy_pool;
  if (FLAGS_implementation == "write_mem_pool") {
    write_mem_pool = std::make_unique<dingofs::WriteMemPool>(
        pool_pages * FLAGS_page_size, FLAGS_page_size);
  } else if (FLAGS_implementation == "write_page_pool" ||
             FLAGS_implementation == "serialized_write_page_pool") {
    page_pool = dingofs::WritePagePool::Create(FLAGS_page_size, pool_pages);
  } else {
    legacy_pool = dingofs::MemoryPool::Create(FLAGS_page_size, pool_pages);
  }
  CHECK(write_mem_pool != nullptr || page_pool != nullptr ||
        legacy_pool != nullptr);
  const bool serialize_page_pool =
      FLAGS_implementation == "serialized_write_page_pool";
  std::mutex page_pool_materialize_mutex;

  std::vector<char*> pressure_hold;
  if (FLAGS_pressure_free_pages >= 0) {
    CHECK_LE(static_cast<uint64_t>(FLAGS_pressure_free_pages), pool_pages);
    if (write_mem_pool != nullptr) {
      CHECK_GE(static_cast<uint64_t>(FLAGS_pressure_free_pages),
               working_pages * FLAGS_inflight_batches)
          << "blocking WriteMemPool benchmark must leave enough pages for "
             "every in-flight lease";
    }
    const size_t hold_count =
        static_cast<size_t>(pool_pages - FLAGS_pressure_free_pages);
    pressure_hold.resize(hold_count);
    size_t held = 0;
    if (write_mem_pool != nullptr) {
      dingofs::WritePageLease lease;
      const dingofs::Status status =
          write_mem_pool->Acquire(hold_count, &lease);
      CHECK(status.ok()) << status.ToString();
      lease.Take(hold_count, pressure_hold.data());
      held = hold_count;
    } else if (page_pool != nullptr) {
      held = page_pool->RequireBatch(pressure_hold.data(), hold_count);
    } else {
      while (held < hold_count) {
        char* page = legacy_pool->Require();
        CHECK_NOTNULL(page);
        pressure_hold[held++] = page;
      }
    }
    CHECK_EQ(held, hold_count);
  }

  std::atomic<bool> start{false};
  std::atomic<uint64_t> failures{0};
  std::atomic<uint64_t> successful_pages{0};
  std::vector<std::vector<uint64_t>> acquire_latencies_ns(FLAGS_threads);
  std::vector<std::thread> workers;
  workers.reserve(FLAGS_threads * (FLAGS_cross_thread_release ? 2 : 1));

  std::vector<std::unique_ptr<TransferSlot[]>> transfer_slots;
  if (FLAGS_cross_thread_release) {
    transfer_slots.reserve(FLAGS_threads);
    for (uint32_t thread = 0; thread < FLAGS_threads; ++thread) {
      auto slots = std::make_unique<TransferSlot[]>(FLAGS_inflight_batches);
      for (uint32_t i = 0; i < FLAGS_inflight_batches; ++i) {
        slots[i].pages.resize(pages_per_release);
      }
      transfer_slots.push_back(std::move(slots));
    }
  }

  for (uint32_t thread = 0; thread < FLAGS_threads; ++thread) {
    if (FLAGS_cross_thread_release) {
      workers.emplace_back([&, thread]() {
        if (FLAGS_pin_threads) {
          PinToCpu(thread + FLAGS_threads);
        }
        while (!start.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        TransferSlot* slots = transfer_slots[thread].get();
        for (uint32_t completed = 0; completed < FLAGS_iterations;
             ++completed) {
          TransferSlot& slot = slots[completed % FLAGS_inflight_batches];
          while (slot.state.load(std::memory_order_acquire) != 1) {
            std::this_thread::yield();
          }
          if (write_mem_pool != nullptr) {
            write_mem_pool->Release(slot.pages.data(), slot.count);
          } else if (page_pool != nullptr) {
            page_pool->ReleaseBatch(slot.pages.data(), slot.count);
          } else {
            for (size_t i = 0; i < slot.count; ++i) {
              legacy_pool->Release(slot.pages[i]);
            }
          }
          slot.state.store(0, std::memory_order_release);
        }
      });
    }

    workers.emplace_back([&, thread]() {
      if (FLAGS_pin_threads) {
        PinToCpu(thread);
      }
      uint64_t allocation_index = 0;
      auto acquire_pages = [&](char** pages) {
        const bool sample =
            FLAGS_latency_sample_stride != 0 &&
            allocation_index++ % FLAGS_latency_sample_stride == 0;
        const auto begin = sample ? std::chrono::steady_clock::now()
                                  : std::chrono::steady_clock::time_point{};
        size_t acquired = 0;
        if (write_mem_pool != nullptr) {
          dingofs::WritePageLease lease;
          const dingofs::Status status =
              write_mem_pool->Acquire(FLAGS_batch_pages, &lease);
          if (status.ok()) {
            lease.Take(FLAGS_batch_pages, pages);
            acquired = FLAGS_batch_pages;
          }
        } else if (page_pool != nullptr) {
          if (serialize_page_pool) {
            std::lock_guard<std::mutex> lock(page_pool_materialize_mutex);
            acquired = page_pool->RequireBatch(pages, FLAGS_batch_pages);
          } else {
            acquired = page_pool->RequireBatch(pages, FLAGS_batch_pages);
          }
        } else {
          while (acquired < FLAGS_batch_pages) {
            char* page = legacy_pool->Require();
            if (page == nullptr) break;
            pages[acquired++] = page;
          }
        }
        if (sample) {
          acquire_latencies_ns[thread].push_back(
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - begin)
                  .count());
        }
        if (acquired != FLAGS_batch_pages) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
        return acquired;
      };
      if (FLAGS_cross_thread_release) {
        while (!start.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        TransferSlot* slots = transfer_slots[thread].get();
        for (uint32_t completed = 0; completed < FLAGS_iterations;
             ++completed) {
          TransferSlot& slot = slots[completed % FLAGS_inflight_batches];
          while (slot.state.load(std::memory_order_acquire) != 0) {
            std::this_thread::yield();
          }
          size_t count = 0;
          for (uint32_t batch = 0; batch < FLAGS_alloc_batches_per_release;
               ++batch) {
            count += acquire_pages(slot.pages.data() + count);
          }
          successful_pages.fetch_add(count, std::memory_order_relaxed);
          slot.count = count;
          if (FLAGS_touch_pages) {
            for (size_t i = 0; i < count; ++i) {
              std::memset(slot.pages[i], static_cast<int>(thread),
                          FLAGS_page_size);
            }
          }
          slot.state.store(1, std::memory_order_release);
        }
        return;
      }
      std::vector<char*> pages(static_cast<size_t>(pages_per_release) *
                               FLAGS_inflight_batches);
      std::vector<size_t> counts(FLAGS_inflight_batches);
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      uint32_t completed = 0;
      while (completed < FLAGS_iterations) {
        const uint32_t window =
            std::min(FLAGS_inflight_batches, FLAGS_iterations - completed);
        for (uint32_t batch = 0; batch < window; ++batch) {
          char** batch_pages =
              pages.data() + (static_cast<size_t>(batch) * pages_per_release);
          size_t count = 0;
          for (uint32_t allocation = 0;
               allocation < FLAGS_alloc_batches_per_release; ++allocation) {
            count += acquire_pages(batch_pages + count);
          }
          successful_pages.fetch_add(count, std::memory_order_relaxed);
          counts[batch] = count;
          if (FLAGS_touch_pages) {
            for (size_t i = 0; i < count; ++i) {
              std::memset(batch_pages[i], static_cast<int>(thread),
                          FLAGS_page_size);
            }
          }
        }

        for (uint32_t batch = 0; batch < window; ++batch) {
          char** batch_pages =
              pages.data() + (static_cast<size_t>(batch) * pages_per_release);
          if (write_mem_pool != nullptr) {
            write_mem_pool->Release(batch_pages, counts[batch]);
          } else if (page_pool != nullptr) {
            page_pool->ReleaseBatch(batch_pages, counts[batch]);
          } else {
            for (size_t i = 0; i < counts[batch]; ++i) {
              legacy_pool->Release(batch_pages[i]);
            }
          }
        }
        completed += window;
      }
    });
  }

  const auto begin = std::chrono::steady_clock::now();
  start.store(true, std::memory_order_release);
  for (auto& worker : workers) worker.join();
  const double seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - begin)
          .count();

  const uint64_t release_groups =
      static_cast<uint64_t>(FLAGS_threads) * FLAGS_iterations;
  const uint64_t allocation_requests =
      release_groups * FLAGS_alloc_batches_per_release;
  const uint64_t pages = release_groups * pages_per_release;
  const double gib =
      static_cast<double>(pages) * FLAGS_page_size / (1024.0 * 1024 * 1024);
  const uint64_t completed_pages =
      successful_pages.load(std::memory_order_relaxed);

  if (write_mem_pool != nullptr) {
    write_mem_pool->Release(pressure_hold.data(), pressure_hold.size());
    CHECK_EQ(write_mem_pool->GetUsedBytes(), 0);
  } else if (page_pool != nullptr) {
    page_pool->ReleaseBatch(pressure_hold.data(), pressure_hold.size());
  } else {
    for (char* page : pressure_hold) {
      legacy_pool->Release(page);
    }
  }

  std::vector<uint64_t> latencies;
  for (auto& thread_latencies : acquire_latencies_ns) {
    latencies.insert(latencies.end(), thread_latencies.begin(),
                     thread_latencies.end());
  }
  std::sort(latencies.begin(), latencies.end());
  const auto percentile_ns = [&latencies](uint32_t percentile) {
    if (latencies.empty()) return uint64_t{0};
    const size_t index =
        (latencies.size() - 1) * static_cast<size_t>(percentile) / 100;
    return latencies[index];
  };

  LOG(INFO) << "implementation=" << FLAGS_implementation
            << " threads=" << FLAGS_threads
            << " batch_pages=" << FLAGS_batch_pages
            << " alloc_batches_per_release=" << FLAGS_alloc_batches_per_release
            << " pages_per_release=" << pages_per_release << " refill_pages=32"
            << " iterations=" << FLAGS_iterations
            << " touch_pages=" << FLAGS_touch_pages
            << " pin_threads=" << FLAGS_pin_threads
            << " inflight_batches=" << FLAGS_inflight_batches
            << " cross_thread_release=" << FLAGS_cross_thread_release
            << " pressure_free_pages=" << FLAGS_pressure_free_pages
            << " latency_sample_stride=" << FLAGS_latency_sample_stride
            << " acquire_latency_samples=" << latencies.size()
            << " acquire_p50_ns=" << percentile_ns(50)
            << " acquire_p95_ns=" << percentile_ns(95)
            << " acquire_p99_ns=" << percentile_ns(99)
            << " acquire_max_ns=" << (latencies.empty() ? 0 : latencies.back())
            << " allocation_requests_per_sec=" << allocation_requests / seconds
            << " release_groups_per_sec=" << release_groups / seconds
            << " ns_per_requested_page=" << seconds * 1e9 / pages
            << " ns_per_successful_page="
            << (completed_pages == 0 ? 0 : seconds * 1e9 / completed_pages)
            << " touched_gib_per_sec="
            << (FLAGS_touch_pages ? gib / seconds : 0)
            << " allocation_failures=" << failures.load();
  return FLAGS_pressure_free_pages >= 0 || failures.load() == 0 ? 0 : 1;
}
