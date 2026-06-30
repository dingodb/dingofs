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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_FILE_SERVER_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_FILE_SERVER_H_

#include <string>

namespace dingofs {
namespace cache {
namespace bench {

// Serve the files under `dir` over HTTP on 0.0.0.0:`port` (GET/HEAD, static
// content, "/" -> index.html). Blocks until the process is terminated (Ctrl-C);
// returns early only on a fatal setup error (socket/bind/listen). A tiny,
// dependency-free replacement for `python3 -m http.server`.
void ServeDirectory(const std::string& dir, int port);

}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_FILE_SERVER_H_
