/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_FSOP_OPTIONS_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_FSOP_OPTIONS_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cache/tools/bench/common/flags.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace fsop {

struct Options {
  std::string dir;
  std::string sizes_str{"4KiB,4MiB"};
  std::string threads_str{"1,8"};
  std::vector<uint64_t> sizes;    // parsed from sizes_str
  std::vector<uint32_t> threads;  // parsed from threads_str
  uint32_t iters{500};
  bool direct{true};
  bool fallocate{true};
  bool fsync{false};
  bool shared_dir{false};
  bool keep{false};
};

void RegisterFlags(FlagSet* fs, Options* o);
std::string Validate(Options* o);  // parses sizes_str/threads_str; "" = ok

}  // namespace fsop
}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_FSOP_OPTIONS_H_
