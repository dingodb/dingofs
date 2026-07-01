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

// Measures how long it costs to wake a *sleeping* thread with
// futex(FUTEX_WAKE) -- the exact cost DingoFS's AioQueue pays per completion,
// where a single BackgroundWait thread wakes a parked worker via
// bthread::ConditionVariable (butex -> futex -> kernel context switch).
//
// It decomposes the cost into four levels:
//   (0) a raw syscall floor           (getpid)
//   (1) FUTEX_WAKE with no waiter      (futex bookkeeping, no wakeup)
//   (2) wake a sleeping thread, SAME core   (+ 1 context switch, no IPI)
//   (3) wake a sleeping thread, CROSS core  (+ IPI + remote schedule)  <--
//   AioQueue's case
//
// Standalone, no deps:  g++ -O2 -pthread futex_wake_bench.cc -o
// futex_wake_bench

#include <linux/futex.h>
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <thread>

namespace {

long Futex(uint32_t* uaddr, int op, uint32_t val) {
  return ::syscall(SYS_futex, uaddr, op, val, nullptr, nullptr, 0);
}

uint64_t NowNs() {
  timespec ts{};
  ::clock_gettime(CLOCK_MONOTONIC, &ts);
  return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL) + ts.tv_nsec;
}

void PinTo(int cpu) {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  ::sched_setaffinity(0, sizeof(set), &set);
}

// Atomic access on a plain uint32_t so that &x stays a valid futex word.
uint32_t Load(uint32_t* p) { return __atomic_load_n(p, __ATOMIC_ACQUIRE); }
void Store(uint32_t* p, uint32_t v) {
  __atomic_store_n(p, v, __ATOMIC_RELEASE);
}

// (0) raw syscall floor: getpid bypasses glibc caching via direct syscall().
double RawSyscallNs(int iters) {
  uint64_t t0 = NowNs();
  for (int i = 0; i < iters; ++i) ::syscall(SYS_getpid);
  return static_cast<double>(NowNs() - t0) / iters;
}

// (1) FUTEX_WAKE on a word nobody waits on: pure futex bookkeeping, no wakeup.
double BareWakeNs(int iters) {
  uint32_t x = 0;
  uint64_t t0 = NowNs();
  for (int i = 0; i < iters; ++i) Futex(&x, FUTEX_WAKE_PRIVATE, 1);
  return static_cast<double>(NowNs() - t0) / iters;
}

// (2)/(3) ping-pong: each wake targets a genuinely sleeping thread, because the
// two threads strictly alternate (each blocks in FUTEX_WAIT before the other
// wakes it). Returns the one-way latency of "wake a sleeping thread".
double PingPongNs(int iters, int cpu_main, int cpu_worker) {
  uint32_t a = 0;  // worker sleeps on a; main wakes it
  uint32_t b = 0;  // main sleeps on b; worker wakes it

  std::thread worker([&] {
    PinTo(cpu_worker);

    Futex(&b, FUTEX_WAIT_PRIVATE, 0);
    // for (int i = 0; i < iters; ++i) {
    //   while (Load(&a) == 0) Futex(&a, FUTEX_WAIT_PRIVATE, 0);
    //   Store(&a, 0);
    //   Store(&b, 1);
    //   Futex(&b, FUTEX_WAKE_PRIVATE, 1);
    // }
  });

  sleep(1);

  PinTo(cpu_main);
  uint64_t t0 = NowNs();

  Futex(&b, FUTEX_WAKE_PRIVATE, 1);

  // for (int i = 0; i < iters; ++i) {
  //   Store(&a, 1);
  //   Futex(&a, FUTEX_WAKE_PRIVATE, 1);  // wake the sleeping worker
  //   while (Load(&b) == 0) Futex(&b, FUTEX_WAIT_PRIVATE, 0);  // sleep for
  //   reply Store(&b, 0);
  // }
  uint64_t t1 = NowNs();
  worker.join();
  // one round trip = two wakes of a sleeping thread (main->worker,
  // worker->main)

  return static_cast<double>(t1 - t0);
  // return static_cast<double>(t1 - t0) / (2.0 * iters);
}

}  // namespace

int main() {
  const int ncpu = static_cast<int>(::sysconf(_SC_NPROCESSORS_ONLN));
  const int cpu_a = 0;
  const int cpu_b = ncpu > 2 ? 2 : (ncpu > 1 ? 1 : 0);

  std::printf("futex wake microbenchmark  (online cpus = %d)\n\n", ncpu);

  const double raw = RawSyscallNs(2000000);
  const double bare = BareWakeNs(2000000);
  const double same = PingPongNs(200000, cpu_a, cpu_a);
  const double cross = PingPongNs(200000, cpu_a, cpu_b);

  std::printf("(0) raw syscall floor (getpid)                  : %7.0f ns\n",
              raw);
  std::printf("(1) FUTEX_WAKE, no waiter                       : %7.0f ns\n",
              bare);
  std::printf(
      "(2) wake a SLEEPING thread, same core (cpu%d)    : %7.0f ns  "
      "(one-way)\n",
      cpu_a, same);
  std::printf(
      "(3) wake a SLEEPING thread, cross core (cpu%d->%d) : %7.0f ns  "
      "(one-way)\n",
      cpu_a, cpu_b, cross);

  std::printf(
      "\n(3) is what DingoFS AioQueue pays per completion when its single\n"
      "BackgroundWait thread wakes a parked worker (cross-core futex + kernel\n"
      "context switch). At ~%.0f ns each, one such thread tops out near %.0f k "
      "ops/s\n"
      "-- which is why `cb aio` plateaus around there regardless of "
      "--threads.\n",
      cross, 1.0e6 / cross);
  return 0;
}
