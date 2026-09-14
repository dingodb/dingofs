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

#include "blockcache/core/fs/io_ring.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "blockcache/core/fs/filesystem.h"
#include "blockcache/core/runtime/bootstrap.h"
#include "blockcache/core/runtime/smp.h"
#include "common/options/cache.h"

namespace dingofs {
namespace blockcache {
namespace {

constexpr size_t kAlign = 4096;
constexpr uint32_t kBlock = 4096;

struct Cleanup {
  explicit Cleanup(std::function<void()> f) : fn(std::move(f)) {}
  ~Cleanup() { fn(); }
  std::function<void()> fn;
};

// ---- ring-level admission: in-flight requests never exceed the CQ ----

class IoRingTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    saved_iodepth_ = FLAGS_iodepth;
    FLAGS_shards = 1;
    FLAGS_iodepth = 4;  // SQ 4, CQ 16: trivial to overrun
    StartProcessRuntime();
  }
  static void TearDownTestSuite() {
    StopProcessRuntime();
    FLAGS_iodepth = saved_iodepth_;
  }

  static uint32_t saved_iodepth_;
};

uint32_t IoRingTest::saved_iodepth_ = 0;

struct RingStats {
  unsigned capacity;
  unsigned peak;
  unsigned inflight;
  unsigned parked;
  uint64_t deferred;
};

RingStats Stats() {
  return RunOnAndWait(0, []() -> Future<RingStats> {
    IoRing& ring = ThisIoRing();
    co_return RingStats{.capacity = ring.cq_capacity(),
                        .peak = ring.peak_inflight(),
                        .inflight = ring.inflight(),
                        .parked = ring.parked(),
                        .deferred = ring.deferred()};
  });
}

// Touched only on the shard thread.
struct Batch {
  std::vector<unsigned> issued;  // prep order == admission order
  int failures = 0;
};

Future<> NopTask(unsigned i, Batch* batch) {
  const int rc = co_await UringOp([i, batch](io_uring_sqe* sqe) {
    batch->issued.push_back(i);
    io_uring_prep_nop(sqe);
  });
  if (rc != 0) {
    ++batch->failures;
  }
}

// Three NOPs admitted as one unit: the multi-slot path OpenReadCloseAwaiter
// takes.
class TripleNop final : public IoAwaiter<TripleNop>, public RingOp {
 public:
  TripleNop(Batch* batch, unsigned i) : batch_(batch), i_(i) {
    for (Op& op : ops_) {
      op.owner = this;
    }
  }

  int await_resume() const noexcept { return result_; }

  void Arm() { ThisIoRing().Admit(this, 3); }
  void OnResult() noexcept {}

  void Issue() override {
    IoRing& ring = ThisIoRing();
    ring.ReserveSqes(3);
    batch_->issued.push_back(i_);
    for (Op& op : ops_) {
      io_uring_prep_nop(ring.GetSqe(&op));
    }
  }

 private:
  struct Op final : IoCompletion {
    void Complete(int32_t res) noexcept override {
      if (res != 0) {
        owner->result_ = res;
      }
      if (--owner->pending_ == 0) {
        owner->ResumeLater(owner->result_);
      }
    }
    TripleNop* owner = nullptr;
  };

  Batch* batch_;
  unsigned i_;
  unsigned pending_ = 3;
  Op ops_[3];
};

Future<> TripleTask(unsigned i, Batch* batch) {
  const int rc = co_await TripleNop(batch, i);
  if (rc != 0) {
    ++batch->failures;
  }
}

// Coroutines start eagerly, so building the tasks arms every op back to back
// with no Reap() in between: the cap is hit by construction, not by timing.
template <typename MakeTask>
Batch RunBatch(unsigned ops, MakeTask make_task) {
  Batch batch;
  batch.issued.reserve(ops);
  RunOnAndWait(0, [&batch, ops, &make_task]() -> Future<> {
    std::vector<Future<>> tasks;
    tasks.reserve(ops);
    for (unsigned i = 0; i < ops; ++i) {
      tasks.push_back(make_task(i, &batch));
    }
    co_await WhenAll(std::move(tasks));
  });
  return batch;
}

TEST_F(IoRingTest, InflightNeverExceedsCq) {
  constexpr unsigned kOps = 4000;
  const RingStats before = Stats();
  ASSERT_EQ(before.capacity, 16u);

  const Batch batch = RunBatch(kOps, NopTask);

  const RingStats s = Stats();
  EXPECT_LE(s.peak, s.capacity);
  EXPECT_GT(s.deferred, before.deferred);
  EXPECT_EQ(s.inflight, 0u);
  EXPECT_EQ(s.parked, 0u);
  EXPECT_EQ(batch.failures, 0);
  ASSERT_EQ(batch.issued.size(), kOps);
  EXPECT_TRUE(std::ranges::is_sorted(batch.issued))
      << "parked ops must be issued in admission order";
}

TEST_F(IoRingTest, MultiSlotOpsShareTheCap) {
  constexpr unsigned kOps = 3000;
  const RingStats before = Stats();

  const Batch batch = RunBatch(kOps, [](unsigned i, Batch* b) {
    return i % 3 == 0 ? TripleTask(i, b) : NopTask(i, b);
  });

  const RingStats s = Stats();
  EXPECT_LE(s.peak, s.capacity);
  EXPECT_GT(s.deferred, before.deferred);
  EXPECT_EQ(s.inflight, 0u);
  EXPECT_EQ(s.parked, 0u);
  EXPECT_EQ(batch.failures, 0);
  ASSERT_EQ(batch.issued.size(), kOps);
  EXPECT_TRUE(std::ranges::is_sorted(batch.issued));
}

Future<> ReadLoop(File* file, char* buf, unsigned reads, int* failed) {
  for (unsigned r = 0; r < reads; ++r) {
    StatusOr<size_t> got = co_await file->Read(0, buf, kBlock);
    if (!got.ok() || *got != kBlock || buf[0] != 'r') {
      ++*failed;
    }
  }
}

// The production path: RwAwaiter goes through the per-file admission queue
// first, then the ring; completions re-enter Admit() from inside Reap().
TEST_F(IoRingTest, FileReadsAreCappedToo) {
  constexpr unsigned kWorkers = 512;
  constexpr unsigned kReadsEach = 8;
  const std::string path = (std::filesystem::current_path() /
                            ("io_ring_test_" + std::to_string(::getpid())))
                               .string();
  Cleanup cleanup([&path] { std::filesystem::remove(path); });

  std::vector<char*> bufs(kWorkers);
  for (char*& b : bufs) {
    b = static_cast<char*>(std::aligned_alloc(kAlign, kBlock));
  }
  Cleanup free_bufs([&bufs] {
    for (char* b : bufs) std::free(b);
  });
  std::memset(bufs[0], 'r', kBlock);

  const RingStats before = Stats();
  int failures = 0;
  Status st = RunOnAndWait(0, [&]() -> Future<Status> {
    OpenOption option{.io_inflight = 256, .register_fd = false};
    StatusOr<File> open = co_await FileSystem::Open(
        path, OpenFlags::kWrite | OpenFlags::kCreate, option);
    if (!open.ok()) co_return open.status();
    File file = std::move(open).value();
    StatusOr<size_t> n = co_await file.Write(0, bufs[0], kBlock);
    if (!n.ok()) co_return n.status();
    Status close = co_await file.Close();
    if (!close.ok()) co_return close;

    open = co_await FileSystem::Open(path, OpenFlags::kRead, option);
    if (!open.ok()) co_return open.status();
    file = std::move(open).value();
    std::vector<Future<>> tasks;
    tasks.reserve(kWorkers);
    for (unsigned w = 0; w < kWorkers; ++w) {
      tasks.push_back(ReadLoop(&file, bufs[w], kReadsEach, &failures));
    }
    co_await WhenAll(std::move(tasks));
    co_return co_await file.Close();
  });
  ASSERT_TRUE(st.ok()) << st.ToString();

  const RingStats s = Stats();
  EXPECT_EQ(failures, 0);
  EXPECT_LE(s.peak, s.capacity);
  EXPECT_GT(s.deferred, before.deferred);
  EXPECT_EQ(s.inflight, 0u);
  EXPECT_EQ(s.parked, 0u);
}

// ---- local-disk read microbenchmark through File::Read ----
// Off by default; run it explicitly on a real NVMe:
//   IO_RING_BENCH_DIR=/mnt/nvme/x IO_RING_BENCH_SECS=5 \
//   test_blockcache_fs --gtest_also_run_disabled_tests --gtest_filter='*Bench*'

class IoRingBench : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    FLAGS_shards = 1;
    StartProcessRuntime();
  }
  static void TearDownTestSuite() { StopProcessRuntime(); }
};

struct BenchStats {
  uint64_t ops = 0;
  double secs = 0;
  std::vector<uint32_t> lat_us;
};

Future<> ReadWorker(File* file, char* buffer, uint64_t blocks, uint32_t seed,
                    std::chrono::steady_clock::time_point deadline,
                    BenchStats* stats) {
  std::minstd_rand rng(seed);
  while (std::chrono::steady_clock::now() < deadline) {
    const uint64_t pos = (rng() % blocks) * kBlock;
    const auto t0 = std::chrono::steady_clock::now();
    StatusOr<size_t> n = co_await file->Read(pos, buffer, kBlock);
    const auto t1 = std::chrono::steady_clock::now();
    if (!n.ok() || *n != kBlock) {
      ADD_FAILURE() << "read " << pos << ": " << n.status().ToString();
      co_return;
    }
    stats->lat_us.push_back(static_cast<uint32_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
            .count()));
    ++stats->ops;
  }
}

void Report(const char* tag, unsigned inflight, std::vector<BenchStats> all) {
  std::vector<uint32_t> lat;
  uint64_t ops = 0;
  double secs = 0;
  for (BenchStats& s : all) {
    ops += s.ops;
    secs = std::max(secs, s.secs);
    lat.insert(lat.end(), s.lat_us.begin(), s.lat_us.end());
  }
  std::ranges::sort(lat);
  double sum = 0;
  for (uint32_t v : lat) sum += v;
  auto pct = [&](double p) {
    return lat.empty() ? 0u
                       : lat[std::min(lat.size() - 1,
                                      static_cast<size_t>(p * lat.size()))];
  };
  (void)std::printf(
      "BENCH %s inflight=%u ops=%lu iops=%.0f avg=%.1fus p50=%u p99=%u "
      "p999=%u max=%u\n",
      tag, inflight, static_cast<unsigned long>(ops), ops / secs,
      lat.empty() ? 0.0 : sum / lat.size(), pct(0.50), pct(0.99), pct(0.999),
      lat.empty() ? 0u : lat.back());
  (void)std::fflush(stdout);
}

TEST_F(IoRingBench, DISABLED_RandomRead4K) {
  const char* dir = std::getenv("IO_RING_BENCH_DIR");
  if (dir == nullptr) {
    GTEST_SKIP() << "set IO_RING_BENCH_DIR to a directory on a real disk";
  }
  const char* secs_env = std::getenv("IO_RING_BENCH_SECS");
  const unsigned secs =
      secs_env != nullptr ? std::strtoul(secs_env, nullptr, 10) : 5;
  const char* mb_env = std::getenv("IO_RING_BENCH_MB");
  const uint64_t file_mb =
      mb_env != nullptr ? std::strtoull(mb_env, nullptr, 10) : 1024;
  const uint64_t blocks = file_mb * 1024 * 1024 / kBlock;

  const std::string path = (std::filesystem::path(dir) /
                            ("io_ring_bench_" + std::to_string(::getpid())))
                               .string();
  Cleanup cleanup([&path] { std::filesystem::remove(path); });

  // Populate with 4 MiB direct writes.
  constexpr uint32_t kChunk = 4u << 20;
  char* wbuf = static_cast<char*>(std::aligned_alloc(kAlign, kChunk));
  std::memset(wbuf, 'x', kChunk);
  Status st = RunOnAndWait(0, [path, wbuf, file_mb]() -> Future<Status> {
    OpenOption option{.register_fd = false};
    StatusOr<File> open = co_await FileSystem::Open(
        path, OpenFlags::kWrite | OpenFlags::kCreate | OpenFlags::kTruncate,
        option);
    if (!open.ok()) co_return open.status();
    File file = std::move(open).value();
    for (uint64_t off = 0; off < file_mb * 1024 * 1024; off += kChunk) {
      StatusOr<size_t> n = co_await file.Write(off, wbuf, kChunk);
      if (!n.ok()) co_return n.status();
    }
    Status sync = co_await file.Sync();
    Status close = co_await file.Close();
    co_return sync.ok() ? close : sync;
  });
  std::free(wbuf);
  ASSERT_TRUE(st.ok()) << st.ToString();

  for (unsigned inflight : {1u, 8u, 32u, 128u}) {
    std::vector<BenchStats> stats(inflight);
    std::vector<char*> bufs(inflight);
    for (char*& b : bufs) {
      b = static_cast<char*>(std::aligned_alloc(kAlign, kBlock));
    }
    Cleanup free_bufs([&bufs] {
      for (char* b : bufs) std::free(b);
    });

    st = RunOnAndWait(0, [&]() -> Future<Status> {
      OpenOption option{.io_inflight = 256};
      StatusOr<File> open =
          co_await FileSystem::Open(path, OpenFlags::kRead, option);
      if (!open.ok()) co_return open.status();
      File file = std::move(open).value();
      const auto start = std::chrono::steady_clock::now();
      const auto deadline = start + std::chrono::seconds(secs);
      std::vector<Future<>> workers;
      for (unsigned i = 0; i < inflight; ++i) {
        stats[i].lat_us.reserve((secs * 400000 / inflight) + 1024);
        workers.push_back(
            ReadWorker(&file, bufs[i], blocks, 1000 + i, deadline, &stats[i]));
      }
      co_await WhenAll(std::move(workers));
      const double elapsed = std::chrono::duration<double>(
                                 std::chrono::steady_clock::now() - start)
                                 .count();
      for (BenchStats& s : stats) s.secs = elapsed;
      co_return co_await file.Close();
    });
    ASSERT_TRUE(st.ok()) << st.ToString();
    Report("rand_read_4k", inflight, std::move(stats));
  }
}

}  // namespace
}  // namespace blockcache
}  // namespace dingofs
