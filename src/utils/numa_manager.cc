// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "utils/numa_manager.h"

#include <numa.h>
#include <numaif.h>
#include <sched.h>
#include <unistd.h>

#include <sstream>
#include <thread>

#include "glog/logging.h"

namespace dingofs {
namespace utils {

NumaManager& NumaManager::GetInstance() {
  static NumaManager instance;
  return instance;
}

NumaManager::NumaManager() {
  available_ = numa_available() >= 0;

  if (!available_) {
    nodes_ = 1;
    cpus_ = std::thread::hardware_concurrency();
    return;
  }

  nodes_ = numa_max_node() + 1;
  cpus_ = numa_num_configured_cpus();
}

void NumaManager::CheckNode(int node) const {
  CHECK(node >= 0 && node < nodes_) << "invalid numa node: " << node;
}

std::vector<int> NumaManager::GetNodeCpus(const int node) const {
  CheckNode(node);

  std::vector<int> cpulist;

  struct bitmask* mask = numa_allocate_cpumask();
  numa_node_to_cpus(node, mask);

  for (int i = 0; i < cpus_; ++i) {
    if (numa_bitmask_isbitset(mask, i)) {
      cpulist.push_back(i);
    }
  }

  numa_free_cpumask(mask);
  return cpulist;
}

void NumaManager::BindCpuNode(const int node) { BindCpuNodes({node}); }

void NumaManager::BindCpuNodes(const std::vector<int>& nodes) {
  CHECK(!nodes.empty());

  struct bitmask* cpumask = numa_allocate_cpumask();
  struct bitmask* tmpmask = numa_allocate_cpumask();

  numa_bitmask_clearall(cpumask);

  for (int node : nodes) {
    CheckNode(node);

    numa_bitmask_clearall(tmpmask);
    numa_node_to_cpus(node, tmpmask);

    for (int cpu = 0; cpu < cpus_; ++cpu) {
      if (numa_bitmask_isbitset(tmpmask, cpu)) {
        numa_bitmask_setbit(cpumask, cpu);
      }
    }
  }

  CHECK(numa_sched_setaffinity(0, cpumask) == 0)
      << "numa_sched_setaffinity failed";

  numa_free_cpumask(tmpmask);
  numa_free_cpumask(cpumask);
}

void NumaManager::BindMemPreferredNode(int node) {
  CheckNode(node);
  numa_set_preferred(node);
}

void NumaManager::BindMemNode(const int node) { BindMemNodes({node}); }

void NumaManager::BindMemNodes(const std::vector<int>& nodes) {
  CHECK(!nodes.empty()) << "nodes cannot be empty";

  struct bitmask* mask = numa_allocate_nodemask();
  numa_bitmask_clearall(mask);

  for (int node : nodes) {
    CheckNode(node);
    numa_bitmask_setbit(mask, node);
  }

  numa_set_membind(mask);

  numa_free_nodemask(mask);
}

void NumaManager::BindNode(int node) {
  BindCpuNode(node);
  BindMemNode(node);
}

void NumaManager::BindNodes(const std::vector<int>& nodes) {
  CHECK(!nodes.empty()) << "nodes cannot be empty";

  BindCpuNodes(nodes);
  BindMemNodes(nodes);
}

int NumaManager::CpuToNode(int cpu) const { return numa_node_of_cpu(cpu); }

void NumaManager::InterleaveAll() const {
  struct bitmask* mask = numa_allocate_nodemask();

  for (int i = 0; i < nodes_; ++i) {
    numa_bitmask_setbit(mask, i);
  }

  numa_set_interleave_mask(mask);
  numa_free_nodemask(mask);
}

void NumaManager::BindThreadToCpu(const int id, const int cpu) const {
  CHECK(cpu >= 0 && cpu < cpus_);

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);

  CHECK(sched_setaffinity(id, sizeof(cpuset), &cpuset) == 0)
      << "sched_setaffinity failed";
}

// 0 means current thread
void NumaManager::BindThreadToCpu(const int cpu) const {
  BindThreadToCpu(0, cpu);
}

void NumaManager::PrintTopology() const {
  std::stringstream ss;

  ss << "NUMA nodes: " << nodes_ << "\n";
  ss << "CPU count : " << cpus_ << "\n";

  for (int i = 0; i < nodes_; ++i) {
    auto cpus = GetNodeCpus(i);

    ss << "node " << i << " cpus: ";
    for (auto c : cpus) {
      ss << c << " ";
    }
    ss << "\n";
  }

  LOG(INFO) << ss.str();
}

}  // namespace utils
}  // namespace dingofs
