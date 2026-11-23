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

#include <sched.h>

#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "blockcache/common/status.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

static std::string GetCpuDir(int cpu) {
  return "/sys/devices/system/cpu/cpu" + std::to_string(cpu);
}

static std::string GetSiblingCpuSet(int cpu) {
  std::ifstream file(GetCpuDir(cpu) + "/topology/thread_siblings_list");
  std::string siblings;
  std::getline(file, siblings);
  return siblings;
}

StatusOr<std::vector<int>> ParseCpuSet(const std::string& spec) {
  std::vector<int> cpus;
  std::set<int> listed;
  const char* p = spec.c_str();
  while (*p != '\0') {
    char* end = nullptr;
    const long lo = std::strtol(p, &end, 10);
    long hi = lo;
    bool ok = end != p;
    if (ok && *end == '-') {
      const char* from = end + 1;
      hi = std::strtol(from, &end, 10);
      ok = end != from;
    }
    if (!ok || lo < 0 || hi < lo || hi >= CPU_SETSIZE) {
      return Status::InvalidParam("bad cpuset: spec=" + spec + ", at=" + p);
    }

    for (long cpu = lo; cpu <= hi; ++cpu) {
      if (!listed.insert(static_cast<int>(cpu)).second) {
        return Status::InvalidParam("cpu listed twice: spec=" + spec +
                                    ", cpu=" + std::to_string(cpu));
      }
      cpus.push_back(static_cast<int>(cpu));
    }
    p = (*end == ',') ? end + 1 : end;
  }
  return cpus;
}

std::vector<int> GetOnlineCpus() {
  std::ifstream file("/sys/devices/system/cpu/online");
  std::string list;
  std::getline(file, list);
  auto cpus = ParseCpuSet(list);
  return cpus.ok() ? std::move(cpus).value() : std::vector<int>{};
}

int GetSiblingCpu(int cpu) {
  auto siblings = ParseCpuSet(GetSiblingCpuSet(cpu));
  if (!siblings.ok()) {
    return kNoCpu;
  }
  for (const int sibling : siblings.value()) {
    if (sibling != cpu) {
      return sibling;
    }
  }
  return kNoCpu;
}

std::vector<int> RemoveSiblingCpus(const std::vector<int>& cpus) {
  std::vector<int> cores;
  std::set<std::string> seen;
  for (const int cpu : cpus) {
    std::string siblings = GetSiblingCpuSet(cpu);
    if (siblings.empty()) {
      siblings = "cpu" + std::to_string(cpu);
    }
    if (seen.insert(siblings).second) {
      cores.push_back(cpu);
    }
  }
  return cores;
}

int GetNumaNode(int cpu) {
  if (cpu < 0) {
    return kNoNumaNode;
  }

  std::error_code ec;
  for (const auto& entry :
       std::filesystem::directory_iterator(GetCpuDir(cpu), ec)) {
    const std::string name = entry.path().filename();
    if (!name.starts_with("node")) {
      continue;
    }

    int node = 0;
    const auto result =
        std::from_chars(name.data() + 4, name.data() + name.size(), node);
    if (result.ec == std::errc()) {
      return node;
    }
  }
  return kNoNumaNode;
}

}  // namespace blockcache
}  // namespace dingofs
