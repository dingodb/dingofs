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

/*
 * Project: DingoFS
 * Created Date: 2025-08-20
 * Author: Jingli Chen (Wine93)
 */

#include "cache/benchmark/option.h"

#include <brpc/reloadable_flags.h>

namespace dingofs {
namespace cache {

DEFINE_uint32(threads, 1, "number of worker threads");
DEFINE_string(op, "put", "operation: put, cache, or range");
DEFINE_uint64(fsid, 1, "filesystem id used in block handles");
DEFINE_uint64(ino, 0, "legacy reserved block id offset");
DEFINE_uint64(blksize, 4194304, "block size in bytes");
DEFINE_uint64(blocks, 1, "number of blocks per worker");
DEFINE_uint64(warmup, 0,
              "warmup blocks per worker, run before timing and excluded from "
              "stats; also moves RDMA pool registration out of the timed "
              "window so qps/bandwidth reflect steady state");
DEFINE_uint64(start_block_id, 1, "first block slice id for this run");
DEFINE_uint64(offset, 0, "range offset inside each block");
DEFINE_uint64(length, 4194304, "range length per request");
DEFINE_bool(writeback, false, "put option writeback flag");
DEFINE_bool(retrive, true, "whether range may retrieve from storage on miss");
DEFINE_uint32(async_max_inflight, 128, "reserved async inflight limit");
DEFINE_uint32(runtime, 300, "runtime in seconds when --time_based=true");
DEFINE_bool(time_based, false, "run by wall clock instead of fixed block count");
DEFINE_bool(bench_remote_only, false,
            "construct RemoteBlockCache directly and skip local cache/storage");
DEFINE_bool(bench_rdma_registered_buffers, false,
            "use one registered contiguous RDMA request/response buffer per "
            "worker so cache RDMA can advertise original buffers");
DEFINE_bool(json_result, false, "print final summary as one JSON object");
DEFINE_string(result_path, "", "optional path to write final JSON summary");
DEFINE_string(verify, "none",
              "range verification mode: none, markers, or full");

}  // namespace cache
}  // namespace dingofs
