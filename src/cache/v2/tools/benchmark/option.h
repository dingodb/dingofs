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

#ifndef DINGOFS_CACHE_V2_TOOLS_BENCHMARK_OPTION_H_
#define DINGOFS_CACHE_V2_TOOLS_BENCHMARK_OPTION_H_

#include <gflags/gflags_declare.h>

namespace dingofs {
namespace cache {
namespace v2 {

DECLARE_uint32(threads);
DECLARE_uint32(iodepth);
DECLARE_string(op);
DECLARE_uint64(fsid);
DECLARE_uint64(blksize);
DECLARE_uint64(blocks);
DECLARE_uint64(offset);
DECLARE_uint64(length);
DECLARE_bool(stage);
DECLARE_bool(retrieve_storage);
DECLARE_uint32(runtime);
DECLARE_bool(time_based);

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_TOOLS_BENCHMARK_OPTION_H_
