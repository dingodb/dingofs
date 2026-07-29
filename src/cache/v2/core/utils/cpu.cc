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

#include "cache/v1/core/utils/cpu.h"

#include <glog/logging.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <system_error>

namespace dingofs {
namespace cache {

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
    // The siblings list starts with the lowest sibling.
    std::string path = CpuDir(cpu) + "/topology/thread_siblings_list";
    if (ReadSysfsInt(path, cpu) == cpu) {
      leaders.push_back(cpu);
    }
  }
  return leaders.empty() ? cpus : leaders;
}

int NumaNode(int cpu) {
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

bool PinToCpu(pthread_t thread, int cpu) {
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(cpu, &mask);
  int rc = ::pthread_setaffinity_np(thread, sizeof(mask), &mask);
  if (rc != 0) {
    PLOG(WARNING) << "Fail to pin thread to cpu=" << cpu;
    return false;
  }
  return true;
}

}  // namespace cache
}  // namespace dingofs
