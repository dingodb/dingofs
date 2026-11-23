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
 * Created Date: 2026-10-12
 * Author: AI
 */

#include <gtest/gtest.h>
#include <linux/mempolicy.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <utility>

#include "blockcache/utils/align.h"
#include "blockcache/utils/cpu.h"
#include "blockcache/utils/memory.h"

namespace dingofs {
namespace blockcache {

// The last node a one-word node mask can name; no machine here has that many.
constexpr int kUnknownNumaNode = 63;

using MemPolicy = std::pair<int, unsigned long>;  // mode, node mask

// The nodes the calling thread may allocate memory on; none when the kernel
// has no NUMA or does not let it ask.
static unsigned long GetAllowedNumaNodes() {
  int mode = 0;
  unsigned long nodes = 0;
  const long rc =
      ::syscall(SYS_get_mempolicy, &mode, &nodes, (sizeof(nodes) * 8) + 1,
                nullptr, MPOL_F_MEMS_ALLOWED);
  return rc == 0 ? nodes : 0;
}

// The policy of the mapping at `addr` itself: MPOL_DEFAULT when it has none.
static MemPolicy GetMemPolicy(void* addr) {
  int mode = -1;
  unsigned long nodes = 0;
  EXPECT_EQ(::syscall(SYS_get_mempolicy, &mode, &nodes, (sizeof(nodes) * 8) + 1,
                      addr, MPOL_F_ADDR),
            0)
      << std::strerror(errno);
  return {mode, nodes};
}

// The node the page at `addr` was allocated on.
static int GetPageNumaNode(void* addr) {
  int node = kNoNumaNode;
  EXPECT_EQ(::syscall(SYS_get_mempolicy, &node, nullptr, 0, addr,
                      MPOL_F_NODE | MPOL_F_ADDR),
            0)
      << std::strerror(errno);
  return node;
}

// Whether the mapping at `addr` was advised MADV_HUGEPAGE: "hg" in its
// VmFlags.
static bool IsHugePageAdvised(void* addr) {
  const auto target = reinterpret_cast<uintptr_t>(addr);
  std::ifstream smaps("/proc/self/smaps");
  bool inside = false;
  for (std::string line; std::getline(smaps, line);) {
    char* dash = nullptr;
    const uintptr_t start = std::strtoul(line.c_str(), &dash, 16);
    if (dash != line.c_str() && *dash == '-') {  // a mapping: <start>-<end> ...
      inside = start <= target && target < std::strtoul(dash + 1, nullptr, 16);
    } else if (inside && line.starts_with("VmFlags:")) {
      return line.find(" hg") != std::string::npos;
    }
  }
  return false;
}

static void* MapAnonymous(size_t length) {
  void* addr = ::mmap(nullptr, length, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  EXPECT_NE(addr, MAP_FAILED) << std::strerror(errno);
  return addr;
}

TEST(MemoryUtilTest, BindMemoryToNumaNode) {
  constexpr size_t kLength = 4 * kMiB;
  const unsigned long nodes = GetAllowedNumaNodes();
  if (nodes == 0) {
    GTEST_SKIP() << "no NUMA memory policy to check against";
  }
  const int node = std::bit_width(nodes) - 1;  // the last node allowed

  {  // bound to a node: the pages come from that node, and no huge page
     // advice comes with it
    void* addr = MapAnonymous(kLength);
    BindMemoryToNumaNode(addr, kLength, node);
    EXPECT_EQ(GetMemPolicy(addr), MemPolicy(MPOL_BIND, 1UL << node));
    EXPECT_FALSE(IsHugePageAdvised(addr));
    static_cast<char*>(addr)[0] = 1;
    EXPECT_EQ(GetPageNumaNode(addr), node);
    ::munmap(addr, kLength);
  }

  {  // just [addr, addr + length) is bound, not the memory around it
    char* region = static_cast<char*>(MapAnonymous(3 * kLength));
    char* addr = region + kLength;
    BindMemoryToNumaNode(addr, kLength, node);
    EXPECT_EQ(GetMemPolicy(addr), MemPolicy(MPOL_BIND, 1UL << node));
    EXPECT_EQ(GetMemPolicy(addr + kLength - 1),
              MemPolicy(MPOL_BIND, 1UL << node));
    EXPECT_EQ(GetMemPolicy(addr - 1), MemPolicy(MPOL_DEFAULT, 0));
    EXPECT_EQ(GetMemPolicy(addr + kLength), MemPolicy(MPOL_DEFAULT, 0));
    ::munmap(region, 3 * kLength);
  }

  {  // no node, one the machine does not have, and one past what a one-word
     // node mask can name leave the mapping without a policy
    for (const int unknown :
         {kNoNumaNode, kUnknownNumaNode, kUnknownNumaNode + 1}) {
      void* addr = MapAnonymous(kLength);
      BindMemoryToNumaNode(addr, kLength, unknown);
      EXPECT_EQ(GetMemPolicy(addr), MemPolicy(MPOL_DEFAULT, 0)) << unknown;
      ::munmap(addr, kLength);
    }
  }
}

TEST(MemoryUtilTest, AdviseHugePages) {
  constexpr size_t kLength = 4 * kMiB;
  char* region = static_cast<char*>(MapAnonymous(3 * kLength));
  char* addr = region + kLength;
  EXPECT_FALSE(IsHugePageAdvised(addr));

  // just [addr, addr + length) is advised, not the memory around it
  AdviseHugePages(addr, kLength);
  EXPECT_TRUE(IsHugePageAdvised(addr));
  EXPECT_TRUE(IsHugePageAdvised(addr + kLength - 1));
  EXPECT_FALSE(IsHugePageAdvised(addr - 1));
  EXPECT_FALSE(IsHugePageAdvised(addr + kLength));
  ::munmap(region, 3 * kLength);
}

}  // namespace blockcache
}  // namespace dingofs
