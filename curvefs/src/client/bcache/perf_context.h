/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BCACHE_PERF_CONTEXT_H_
#define CURVEFS_SRC_CLIENT_BCACHE_PERF_CONTEXT_H_

#include <string>

namespace curvefs {
namespace client {
namespace bcache {

// e.g.
// get (10485763,1_10485763_1_1_0,4194304): OK (HIT,[0.00009,0.000000]) <0.000009>
class PerfContext {
 public:
    enum class ContextPhase {
        BLOCK_STAGE,
        BLOCK_CACHE,
        BLOCK_LOAD,
        S3_GET,
        S3_PUT,
        S3_RANGE,
    };

 public:
    PerfContext();

    ~PerfContext();

    void NextPhase(ContextPhase phase);

    std::string ToString();
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_PERF_CONTEXT_H_
