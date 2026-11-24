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
#include <sched.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "blockcache/utils/cpu.h"

namespace dingofs {
namespace blockcache {

// A cpu number no machine here has, so the kernel knows nothing about it.
constexpr int kUnknownCpu = 4096;

static std::string ReadLine(const std::string& path) {
  std::ifstream file(path);
  std::string line;
  std::getline(file, line);
  return line;
}

// The cpus the kernel has a directory for, online or not.
static std::vector<int> GetPresentCpus() {
  std::vector<int> cpus;
  for (int cpu = 0; cpu < CPU_SETSIZE; cpu++) {
    if (std::filesystem::exists("/sys/devices/system/cpu/cpu" +
                                std::to_string(cpu))) {
      cpus.push_back(cpu);
    }
  }
  return cpus;
}

// The cpus the kernel keeps a topology for, which an offline cpu loses.
// Asked cpu by cpu, apart from /sys/devices/system/cpu/online.
static std::vector<int> GetCpusWithTopology() {
  std::vector<int> cpus;
  for (int cpu = 0; cpu < CPU_SETSIZE; cpu++) {
    if (std::filesystem::exists("/sys/devices/system/cpu/cpu" +
                                std::to_string(cpu) + "/topology")) {
      cpus.push_back(cpu);
    }
  }
  return cpus;
}

// The node that /sys/devices/system/node lists `cpu` under; kNoNumaNode when
// none does, as on a kernel without NUMA.
static int GetListedNumaNode(int cpu) {
  std::error_code ec;
  for (const auto& entry :
       std::filesystem::directory_iterator("/sys/devices/system/node", ec)) {
    const std::string name = entry.path().filename().string();
    if (name.starts_with("node") &&
        std::filesystem::exists(entry.path() / ("cpu" + std::to_string(cpu)))) {
      return std::stoi(name.substr(4));
    }
  }
  return kNoNumaNode;
}

// The cpus of `cpu`'s core, itself included, as the kernel lists them.
static std::vector<int> GetSiblingCpuSet(int cpu) {
  auto cpus =
      ParseCpuSet(ReadLine("/sys/devices/system/cpu/cpu" + std::to_string(cpu) +
                           "/topology/thread_siblings_list"));
  EXPECT_TRUE(cpus.ok()) << cpus.status().ToString();
  return cpus.ok() ? std::move(cpus).value() : std::vector<int>{};
}

TEST(CpuTest, CpuRelax) {
  // what an idle loop spins on until another thread has something for it
  std::atomic<uint64_t> spins{0};
  std::atomic<bool> ready{false};
  std::thread notifier([&spins, &ready] {
    while (spins.load(std::memory_order_relaxed) < 1000) {
      std::this_thread::yield();
    }
    ready.store(true, std::memory_order_release);
  });
  while (!ready.load(std::memory_order_acquire)) {
    CpuRelax();
    spins.fetch_add(1, std::memory_order_relaxed);
  }
  notifier.join();
  EXPECT_GE(spins.load(), 1000);
}

TEST(CpuTest, ParseCpuSet) {
  {  // cpus and ranges of them, as --cpuset and the kernel's cpu lists hold,
     // in the order given
    struct Case {
      std::string spec;
      std::vector<int> cpus;
    };
    const Case cases[] = {
        {.spec = "5", .cpus = {5}},
        {.spec = "0-3", .cpus = {0, 1, 2, 3}},
        {.spec = "9,73", .cpus = {9, 73}},
        {.spec = "73,9", .cpus = {73, 9}},
        {.spec = "0-3,5,9-11", .cpus = {0, 1, 2, 3, 5, 9, 10, 11}},
        {.spec = "8,0-3", .cpus = {8, 0, 1, 2, 3}},
        {.spec = "7-7", .cpus = {7}},
        {.spec = "1023", .cpus = {1023}},
        {.spec = "", .cpus = {}},
    };
    for (const Case& c : cases) {
      auto cpus = ParseCpuSet(c.spec);
      ASSERT_TRUE(cpus.ok()) << c.spec << ": " << cpus.status().ToString();
      EXPECT_EQ(cpus.value(), c.cpus) << c.spec;
    }
  }

  {  // a malformed item, a reversed range or a cpu of CPU_SETSIZE or more is
     // refused, naming where it went wrong
    struct Case {
      std::string spec;
      std::string at;
    };
    const Case cases[] = {
        {.spec = "abc", .at = "abc"},  {.spec = "0-3,,8", .at = ",8"},
        {.spec = "0-3x", .at = "x"},   {.spec = "-1", .at = "-1"},
        {.spec = "0-", .at = "0-"},    {.spec = "3-1", .at = "3-1"},
        {.spec = "1-2-3", .at = "-3"}, {.spec = "0-1024", .at = "0-1024"},
    };
    for (const Case& c : cases) {
      auto cpus = ParseCpuSet(c.spec);
      ASSERT_FALSE(cpus.ok()) << c.spec;
      EXPECT_TRUE(cpus.status().IsInvalidParam()) << c.spec;
      EXPECT_EQ(cpus.status().ToString(),
                "InvalidParam: bad cpuset: spec=" + c.spec + ", at=" + c.at);
    }
  }

  {  // so is a cpu listed twice
    struct Case {
      std::string spec;
      int cpu;
    };
    const Case cases[] = {
        {.spec = "3,1,1", .cpu = 1},
        {.spec = "0-3,2", .cpu = 2},
        {.spec = "1-2,2-3", .cpu = 2},
    };
    for (const Case& c : cases) {
      auto cpus = ParseCpuSet(c.spec);
      ASSERT_FALSE(cpus.ok()) << c.spec;
      EXPECT_TRUE(cpus.status().IsInvalidParam()) << c.spec;
      EXPECT_EQ(cpus.status().ToString(),
                "InvalidParam: cpu listed twice: spec=" + c.spec +
                    ", cpu=" + std::to_string(c.cpu));
    }
  }
}

TEST(CpuTest, GetOnlineCpus) {
  // ascending, and just the cpus the kernel keeps a topology for: not 0..n-1
  // in general, since cpus can be offline
  EXPECT_EQ(GetOnlineCpus(), GetCpusWithTopology());
}

TEST(CpuTest, GetSiblingCpu) {
  const std::vector<int> online = GetCpusWithTopology();
  for (const int cpu : online) {
    const std::vector<int> siblings = GetSiblingCpuSet(cpu);
    ASSERT_FALSE(siblings.empty()) << cpu;
    const int sibling = GetSiblingCpu(cpu);
    if (siblings.size() == 1) {  // a core of one online thread
      EXPECT_EQ(sibling, kNoCpu) << cpu;
      continue;
    }

    // the lowest-numbered other thread of the core, whichever asks
    std::vector<int> others = siblings;
    std::erase(others, cpu);
    EXPECT_EQ(sibling, std::ranges::min(others)) << cpu;
    if (siblings.size() == 2) {
      EXPECT_EQ(GetSiblingCpu(sibling), cpu);
    }
  }

  {  // an offline cpu has no topology, so no sibling either
    for (const int cpu : GetPresentCpus()) {
      if (std::ranges::find(online, cpu) == online.end()) {
        EXPECT_EQ(GetSiblingCpu(cpu), kNoCpu) << cpu;
      }
    }
  }

  {  // no cpu, or one the kernel knows nothing about, has none
    EXPECT_EQ(GetSiblingCpu(kNoCpu), kNoCpu);
    EXPECT_EQ(GetSiblingCpu(kUnknownCpu), kNoCpu);
  }
}

TEST(CpuTest, RemoveSiblingCpus) {
  const std::vector<int> cpus = GetCpusWithTopology();
  ASSERT_FALSE(cpus.empty());

  {  // one cpu per physical core; for cpus in order, the core's first thread
    std::set<std::vector<int>> cores;
    for (const int core : RemoveSiblingCpus(cpus)) {
      const std::vector<int> siblings = GetSiblingCpuSet(core);
      ASSERT_FALSE(siblings.empty()) << core;
      EXPECT_EQ(siblings.front(), core);
      EXPECT_TRUE(cores.insert(siblings).second) << core;
    }

    std::set<std::vector<int>> all;
    for (const int cpu : cpus) {
      all.insert(GetSiblingCpuSet(cpu));
    }
    EXPECT_EQ(cores, all);
  }

  {  // whichever thread of a core comes first stands for it, also without
     // the core's first thread and next to other cores
    for (const int cpu : cpus) {
      const std::vector<int> siblings = GetSiblingCpuSet(cpu);
      if (siblings.size() > 1 && siblings.front() == cpu) {
        const int sibling = siblings[1];
        EXPECT_EQ(RemoveSiblingCpus(siblings), std::vector<int>{cpu});
        EXPECT_EQ(RemoveSiblingCpus({sibling, cpu}), std::vector<int>{sibling});
        EXPECT_EQ(RemoveSiblingCpus({sibling}), std::vector<int>{sibling});
        const auto other = std::ranges::find_if(cpus, [&](int c) {
          return std::ranges::find(siblings, c) == siblings.end();
        });
        ASSERT_NE(other, cpus.end());
        EXPECT_EQ(RemoveSiblingCpus({*other, sibling}),
                  (std::vector<int>{*other, sibling}));
        break;
      }
    }
  }

  {  // a cpu without topology counts as a core of its own
    EXPECT_EQ(RemoveSiblingCpus({kUnknownCpu}), std::vector<int>{kUnknownCpu});
  }

  {  // no cpus, no cores
    EXPECT_TRUE(RemoveSiblingCpus({}).empty());
  }
}

TEST(CpuTest, GetNumaNode) {
  {  // the node that lists the cpu as one of its own, also for an offline
     // cpu, which keeps its node
    for (const int cpu : GetPresentCpus()) {
      EXPECT_EQ(GetNumaNode(cpu), GetListedNumaNode(cpu)) << cpu;
    }
  }

  {  // no cpu, or one the kernel knows nothing about, is on no node
    EXPECT_EQ(GetNumaNode(kNoCpu), kNoNumaNode);
    EXPECT_EQ(GetNumaNode(kUnknownCpu), kNoNumaNode);
  }
}

}  // namespace blockcache
}  // namespace dingofs
