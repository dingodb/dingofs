// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "utils/numa_binder.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <numa.h>
#include <unistd.h>

#include <string>

DEFINE_int32(numa_node, -1,
             "NUMA node to bind the process to (CPU + memory). "
             "-1 disables binding; >= 0 binds to that node.");

DEFINE_string(numa_mem_policy, "preferred",
              "NUMA memory allocation policy when --numa_node is set. "
              "One of: preferred (soft, may spill), bind (strict, OOM on "
              "exhaustion), interleave (round-robin within the node).");

static bool ValidateNumaMemPolicy(const char*, const std::string& value) {
  return value == "preferred" || value == "bind" || value == "interleave";
}
DEFINE_validator(numa_mem_policy, &ValidateNumaMemPolicy);

namespace dingofs {
namespace utils {

void BindNumaOrDie() {
  const int node = FLAGS_numa_node;
  if (node < 0) {
    return;
  }

  CHECK_GE(numa_available(), 0)
      << "--numa_node=" << node
      << " specified but libnuma reports no NUMA support on this host";

  const int max_node = numa_max_node();
  CHECK_LE(node, max_node)
      << "--numa_node=" << node
      << " is out of range (max valid node: " << max_node << ")";

  PCHECK(numa_run_on_node(node) == 0)
      << "numa_run_on_node(" << node << ") failed";

  const std::string& policy = FLAGS_numa_mem_policy;
  if (policy == "preferred") {
    numa_set_preferred(node);
  } else {
    struct bitmask* mask = numa_allocate_nodemask();
    CHECK(mask != nullptr) << "numa_allocate_nodemask failed";
    numa_bitmask_clearall(mask);
    numa_bitmask_setbit(mask, node);
    if (policy == "bind") {
      numa_set_membind(mask);
    } else {  // interleave
      numa_set_interleave_mask(mask);
    }
    numa_free_nodemask(mask);
  }

  LOG(INFO) << "NUMA binding active: node=" << node
            << " mem_policy=" << policy
            << " (verify with /proc/" << getpid() << "/status)";
}

}  // namespace utils
}  // namespace dingofs
