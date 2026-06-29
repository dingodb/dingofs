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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_COMMAND_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_COMMAND_H_

#include <string>

namespace dingofs {
namespace cache {
namespace bench {

// One `cb` subcommand. `run` is invoked with the program label (e.g. "cb aio",
// used in --help) and the args after the subcommand name (argv[0] = subcommand
// name, argv[1..] = flags). Returns the process exit code.
struct SubCommand {
  const char* name;
  const char* desc;
  int (*run)(const std::string& program, int argc, char** argv);
};

}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_COMMAND_H_
