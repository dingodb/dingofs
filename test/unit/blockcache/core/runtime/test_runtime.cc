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
 * Created Date: 2026-10-10
 * Author: AI
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <pthread.h>
#include <sched.h>

#include <set>
#include <string>
#include <thread>
#include <vector>

#include "blockcache/core/runtime/runtime.h"
#include "blockcache/utils/cpu.h"
#include "blockcache/utils/thread.h"
#include "common/options/cache.h"

namespace dingofs {
namespace blockcache {

constexpr int kNoSuchCpu = CPU_SETSIZE - 1;

static std::string BuildCpuSet(const std::vector<int>& cpus) {
  std::string spec;
  for (const int cpu : cpus) {
    if (!spec.empty()) {
      spec += ",";
    }
    spec += std::to_string(cpu);
  }
  return spec;
}

static std::vector<int> GetPlannedCpus(const ShardLayout& layout) {
  std::vector<int> cpus;
  cpus.reserve(layout.shard_count());
  for (unsigned shard = 0; shard < layout.shard_count(); shard++) {
    cpus.push_back(layout.CpuOf(shard));
  }
  return cpus;
}

static std::vector<int> GetSmtTrio() {
  const std::vector<int> cpus = GetThreadCpus(::pthread_self());
  const std::set<int> allowed(cpus.begin(), cpus.end());
  for (const int cpu : cpus) {
    const int sibling = GetSiblingCpu(cpu);
    if (sibling == kNoCpu || !allowed.contains(sibling)) {
      continue;
    }
    for (const int other : cpus) {
      const int other_sibling = GetSiblingCpu(other);
      if (other != cpu && other != sibling && other_sibling != cpu &&
          other_sibling != sibling) {
        return {cpu, sibling, other};
      }
    }
  }
  return {};
}

TEST(ShardLayoutTest, Plan) {
  const gflags::FlagSaver flag_saver;
  const std::vector<int> cpus = GetThreadCpus(::pthread_self());
  ASSERT_GE(cpus.size(), 2) << "the pinning cases need two cpus";

  {
    FLAGS_shards = 3;
    FLAGS_cpuset = BuildCpuSet({cpus[0]});
    FLAGS_pin_cpu = false;
    auto layout = ShardLayout::Plan();
    ASSERT_TRUE(layout.ok()) << layout.status().ToString();
    EXPECT_EQ(GetPlannedCpus(*layout),
              (std::vector<int>{kNoCpu, kNoCpu, kNoCpu}));
  }

  {
    FLAGS_shards = 2;
    FLAGS_cpuset = BuildCpuSet({cpus[1], cpus[0]});
    FLAGS_pin_cpu = true;
    auto layout = ShardLayout::Plan();
    ASSERT_TRUE(layout.ok()) << layout.status().ToString();
    EXPECT_EQ(GetPlannedCpus(*layout), (std::vector<int>{cpus[1], cpus[0]}));
  }

  {
    FLAGS_shards = 4;
    FLAGS_cpuset = BuildCpuSet({cpus[0], cpus[1]});
    FLAGS_pin_cpu = true;
    auto layout = ShardLayout::Plan();
    ASSERT_TRUE(layout.ok()) << layout.status().ToString();
    EXPECT_EQ(GetPlannedCpus(*layout),
              (std::vector<int>{cpus[0], cpus[1], kNoCpu, kNoCpu}));
  }

  {
    FLAGS_shards = 0;
    FLAGS_cpuset = BuildCpuSet({cpus[1]});
    FLAGS_pin_cpu = true;
    auto layout = ShardLayout::Plan();
    ASSERT_TRUE(layout.ok()) << layout.status().ToString();
    EXPECT_EQ(GetPlannedCpus(*layout), std::vector<int>{cpus[1]});
  }

  {
    FLAGS_shards = 0;
    FLAGS_cpuset = "";
    FLAGS_pin_cpu = false;
    auto layout = ShardLayout::Plan();
    ASSERT_TRUE(layout.ok()) << layout.status().ToString();
    EXPECT_EQ(layout->shard_count(), RemoveSiblingCpus(GetOnlineCpus()).size());
  }

  {
    FLAGS_shards = 2;
    FLAGS_cpuset = BuildCpuSet({cpus[0], cpus[0]});
    FLAGS_pin_cpu = true;
    auto layout = ShardLayout::Plan();
    ASSERT_FALSE(layout.ok());
    EXPECT_TRUE(layout.status().IsInvalidParam()) << layout.status().ToString();
  }

  {
    for (const bool pin_cpu : {true, false}) {
      FLAGS_shards = 2;
      FLAGS_cpuset = BuildCpuSet({cpus[0], kNoSuchCpu});
      FLAGS_pin_cpu = pin_cpu;
      auto layout = ShardLayout::Plan();
      ASSERT_FALSE(layout.ok()) << pin_cpu;
      EXPECT_EQ(
          layout.status().ToString(),
          "InvalidParam: --cpuset=" + FLAGS_cpuset +
              " has cpus that are not online: " + std::to_string(kNoSuchCpu));
    }
  }

  {
    FLAGS_shards = 2;
    FLAGS_cpuset = BuildCpuSet({cpus[0], cpus[1]});
    FLAGS_pin_cpu = true;
    std::thread([&cpus] {
      ASSERT_TRUE(BindThreadToCpu(::pthread_self(), cpus[0]));
      auto layout = ShardLayout::Plan();
      ASSERT_TRUE(layout.ok()) << layout.status().ToString();
      EXPECT_EQ(GetPlannedCpus(*layout), (std::vector<int>{cpus[0], cpus[1]}));
    }).join();
  }

  const std::vector<int> trio = GetSmtTrio();
  if (trio.empty()) {
    GTEST_SKIP() << "no two cpus of one core to plan around";
  }
  const int first = trio[0];
  const int sibling = trio[1];
  const int other = trio[2];
  FLAGS_cpuset = BuildCpuSet(trio);

  {
    FLAGS_shards = 0;
    FLAGS_pin_cpu = false;
    auto layout = ShardLayout::Plan();
    ASSERT_TRUE(layout.ok()) << layout.status().ToString();
    EXPECT_EQ(GetPlannedCpus(*layout), (std::vector<int>{kNoCpu, kNoCpu}));
  }

  {
    FLAGS_shards = 0;
    FLAGS_pin_cpu = true;
    auto layout = ShardLayout::Plan();
    ASSERT_TRUE(layout.ok()) << layout.status().ToString();
    EXPECT_EQ(GetPlannedCpus(*layout), (std::vector<int>{first, other}));
  }

  {
    FLAGS_shards = 2;
    FLAGS_pin_cpu = true;
    auto layout = ShardLayout::Plan();
    ASSERT_TRUE(layout.ok()) << layout.status().ToString();
    EXPECT_EQ(GetPlannedCpus(*layout), (std::vector<int>{first, other}));
  }

  {
    FLAGS_shards = 3;
    FLAGS_pin_cpu = true;
    auto layout = ShardLayout::Plan();
    ASSERT_TRUE(layout.ok()) << layout.status().ToString();
    EXPECT_EQ(GetPlannedCpus(*layout),
              (std::vector<int>{first, sibling, other}));
  }
}

}  // namespace blockcache
}  // namespace dingofs
