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

#include "blockcache/tools/benchmark/option.h"

#include <gflags/gflags.h>

namespace dingofs {
namespace blockcache {

DEFINE_uint32(threads, 1, "submitting threads");
DEFINE_uint32(iodepth, 1, "in-flight requests per thread");
DEFINE_string(op, "put", "put | get | delete");
DEFINE_uint64(fsid, 1, "fs the blocks belong to");
DEFINE_uint64(blksize, 4194304, "block size in bytes");
DEFINE_uint64(blocks, 1, "blocks per thread");
DEFINE_uint64(offset, 0, "get: offset within the block");
DEFINE_uint64(length, 4194304, "get: bytes per operation");
DEFINE_bool(stage, false, "put: stage to the local disk first");
DEFINE_bool(retrieve_storage, true, "get: fall back to the object storage");
DEFINE_uint32(runtime, 300, "seconds a time-based run lasts");
DEFINE_bool(time_based, false, "loop over the blocks until --runtime elapses");

}  // namespace blockcache
}  // namespace dingofs
