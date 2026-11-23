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
 * Created Date: 2026-10-04
 * Author: AI
 */

#include <gtest/gtest.h>
#include <pthread.h>
#include <sched.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "blockcache/utils/thread.h"

namespace dingofs {
namespace blockcache {

static std::string GetThreadName(pthread_t thread) {
  char name[16] = {};
  EXPECT_EQ(::pthread_getname_np(thread, name, sizeof(name)), 0);
  return name;
}

static cpu_set_t GetThreadAffinity(pthread_t thread) {
  cpu_set_t mask;
  CPU_ZERO(&mask);
  EXPECT_EQ(::pthread_getaffinity_np(thread, sizeof(mask), &mask), 0);
  return mask;
}

TEST(ThreadTest, SetThreadName) {
  std::thread([] {
    SetThreadName("heartbeat");
    EXPECT_EQ(GetThreadName(::pthread_self()), "heartbeat");

    {  // the kernel keeps 15 characters, the rest is cut off
      SetThreadName("blockcache-shard-12");
      EXPECT_EQ(GetThreadName(::pthread_self()), "blockcache-shar");
    }

    {  // a view needs no terminating '\0'
      const std::string_view names = "offload-3,offload-4";
      SetThreadName(names.substr(0, 9));
      EXPECT_EQ(GetThreadName(::pthread_self()), "offload-3");
    }
  }).join();
}

TEST(ThreadTest, BindThreadToCpu) {
  const std::vector<int> cpus = GetThreadCpus(::pthread_self());
  ASSERT_FALSE(cpus.empty());

  {  // a thread binds itself, as a shard does, and moves onto that cpu at once
    std::thread([cpu = cpus.back()] {
      EXPECT_TRUE(BindThreadToCpu(::pthread_self(), cpu));
      const cpu_set_t mask = GetThreadAffinity(::pthread_self());
      EXPECT_EQ(CPU_COUNT(&mask), 1);
      EXPECT_TRUE(CPU_ISSET(cpu, &mask));
      EXPECT_EQ(::sched_getcpu(), cpu);
    }).join();
  }

  {  // or is bound by another thread
    std::atomic<bool> stop{false};
    std::thread worker([&stop] {
      while (!stop.load(std::memory_order_relaxed)) {
        std::this_thread::yield();
      }
    });
    EXPECT_TRUE(BindThreadToCpu(worker.native_handle(), cpus.front()));
    const cpu_set_t mask = GetThreadAffinity(worker.native_handle());
    EXPECT_EQ(CPU_COUNT(&mask), 1);
    EXPECT_TRUE(CPU_ISSET(cpus.front(), &mask));
    stop.store(true, std::memory_order_relaxed);
    worker.join();
  }

  {  // a cpu the machine does not have is refused and the mask stays as it was
    std::thread([] {
      const cpu_set_t before = GetThreadAffinity(::pthread_self());
      EXPECT_FALSE(BindThreadToCpu(::pthread_self(), CPU_SETSIZE - 1));
      const cpu_set_t after = GetThreadAffinity(::pthread_self());
      EXPECT_TRUE(CPU_EQUAL(&before, &after));
    }).join();
  }
}

TEST(ThreadTest, GetThreadCpus) {
  const std::vector<int> cpus = GetThreadCpus(::pthread_self());
  ASSERT_FALSE(cpus.empty());

  {  // every cpu of the thread's affinity mask, ascending: not 0..n-1 in
     // general, as cpus can be offline or outside its cpuset
    const cpu_set_t mask = GetThreadAffinity(::pthread_self());
    EXPECT_EQ(cpus.size(), static_cast<size_t>(CPU_COUNT(&mask)));
    EXPECT_TRUE(std::ranges::is_sorted(cpus));
    for (const int cpu : cpus) {
      EXPECT_TRUE(CPU_ISSET(cpu, &mask)) << cpu;
    }
  }

  {  // those of the thread asked about, not of the caller
    std::atomic<bool> stop{false};
    std::thread worker([&stop] {
      while (!stop.load(std::memory_order_relaxed)) {
        std::this_thread::yield();
      }
    });
    EXPECT_TRUE(BindThreadToCpu(worker.native_handle(), cpus.back()));
    EXPECT_EQ(GetThreadCpus(worker.native_handle()),
              std::vector<int>{cpus.back()});
    EXPECT_EQ(GetThreadCpus(::pthread_self()), cpus);
    stop.store(true, std::memory_order_relaxed);
    worker.join();
  }
}

}  // namespace blockcache
}  // namespace dingofs
