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

#include "blockcache/utils/cpu.h"

#include <glog/logging.h>
#include <numaif.h>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <unistd.h>

#include <charconv>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <system_error>

namespace dingofs {
namespace blockcache {

static std::string CpuDir(int cpu) {
  return "/sys/devices/system/cpu/cpu" + std::to_string(cpu);
}

static int ReadSysfsInt(const std::string& path, int fallback) {
  std::ifstream file(path);
  int value = 0;
  return (file && (file >> value)) ? value : fallback;
}

std::vector<int> GetAllCpus() {
  int ncpu = static_cast<int>(::sysconf(_SC_NPROCESSORS_ONLN));
  std::vector<int> cpus;
  cpus.reserve(ncpu > 0 ? ncpu : 0);
  for (int cpu = 0; cpu < ncpu; ++cpu) {
    cpus.push_back(cpu);
  }
  return cpus;
}

StatusOr<std::vector<int>> ParseCpuSet(const std::string& spec) {
  std::vector<int> cpus;
  const char* p = spec.c_str();
  while (*p != '\0') {
    char* end = nullptr;
    long lo = std::strtol(p, &end, 10);
    if (end == p) {
      return Status::InvalidParam("bad cpuset: spec=" + spec + ", at=" + p);
    }

    long hi = (*end == '-') ? std::strtol(end + 1, &end, 10) : lo;
    for (long cpu = lo; cpu <= hi; ++cpu) {
      cpus.push_back(static_cast<int>(cpu));
    }
    p = (*end == ',') ? end + 1 : end;
  }
  return cpus;
}

std::vector<int> GetPhyCores(const std::vector<int>& cpus) {
  std::vector<int> leaders;
  for (int cpu : cpus) {
    std::string path = CpuDir(cpu) + "/topology/thread_siblings_list";
    if (ReadSysfsInt(path, cpu) == cpu) {
      leaders.push_back(cpu);
    }
  }
  return leaders.empty() ? cpus : leaders;
}

int GetNumaNode(int cpu) {
  std::error_code ec;
  for (const auto& entry :
       std::filesystem::directory_iterator(CpuDir(cpu), ec)) {
    std::string name = entry.path().filename();
    int node = 0;
    if (name.starts_with("node") &&
        std::from_chars(name.data() + 4, name.data() + name.size(), node).ec ==
            std::errc()) {
      return node;
    }
  }
  return -1;
}

int GetSmtSibling(int cpu) {
  std::ifstream file(CpuDir(cpu) + "/topology/thread_siblings_list");
  std::string spec;
  if (!file || !std::getline(file, spec)) {
    return -1;
  }
  auto siblings = ParseCpuSet(spec);
  if (!siblings.ok()) {
    return -1;
  }
  for (int sibling : siblings.value()) {
    if (sibling != cpu) {
      return sibling;
    }
  }
  return -1;
}

void BindPages(void* addr, size_t length, int numa_node) {
  if (numa_node >= 0) {
    unsigned long nodemask = 1ul << numa_node;
    (void)::mbind(addr, length, MPOL_BIND, &nodemask,
                  (sizeof(nodemask) * 8) + 1, 0);
  }
  (void)::madvise(addr, length, MADV_HUGEPAGE);
}

}  // namespace blockcache
}  // namespace dingofs
