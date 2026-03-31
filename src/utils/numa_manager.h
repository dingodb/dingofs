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

#ifndef SRC_UTILS_NUMA_MANAGER_H_
#define SRC_UTILS_NUMA_MANAGER_H_

#include <vector>

namespace dingofs {
namespace utils {

class NumaManager {
 public:
  static NumaManager& GetInstance();

  bool Available() const { return available_; }

  int NodeCount() const { return nodes_; }

  int CpuCount() const { return cpus_; }

  std::vector<int> GetNodeCpus(int node) const;

  void BindCpuNode(int node);

  void BindCpuNodes(const std::vector<int>& nodes);

  void BindMemPreferredNode(int node);

  void BindMemNode(int node);

  void BindMemNodes(const std::vector<int>& nodes);

  void BindNode(int node);

  void BindNodes(const std::vector<int>& nodes);

  int CpuToNode(int cpu) const;

  void InterleaveAll() const;

  void BindThreadToCpu(int id, int cpu) const;

  void BindCurrentThreadToCpu(int cpu) const;

  void PrintTopology() const;

 public:
  NumaManager(const NumaManager&) = delete;
  NumaManager& operator=(const NumaManager&) = delete;

 private:
  NumaManager();

  void CheckNode(int node) const;

 private:
  bool available_;
  int nodes_;
  int cpus_;
};

}  // namespace utils
}  // namespace dingofs

#endif  // SRC_UTILS_NUMA_MANAGER_H_
