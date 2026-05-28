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

#ifndef SRC_UTILS_NUMA_BINDER_H_
#define SRC_UTILS_NUMA_BINDER_H_

namespace dingofs {
namespace utils {

// Pins the current process to a NUMA node according to --numa_node and
// --numa_mem_policy gflags. No-op when --numa_node is negative; aborts on
// any failure when binding is requested.
//
// Must be called once after gflags parsing and before any worker threads
// or large allocations are created, so the binding propagates to all
// descendant threads.
void BindNumaOrDie();

}  // namespace utils
}  // namespace dingofs

#endif  // SRC_UTILS_NUMA_BINDER_H_
