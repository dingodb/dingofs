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
 * Created Date: 2025-06-02
 * Author: Jingli Chen (Wine93)
 */

#include "cache/config/benchmark.h"

#include "base/time/time.h"
#include "cache/config/common.h"

namespace dingofs {
namespace cache {

DEFINE_string(op, "put", "");
DEFINE_bool(local, true, "");
DEFINE_uint32(threads, 1, "");
DEFINE_uint32(blksize, 4194304, "");
DEFINE_uint32(blocks, 1, "");
DEFINE_bool(writeback, true, "");
DEFINE_bool(retrive, false, "");
DEFINE_string(ak, "", "");
DEFINE_string(sk, "", "");
DEFINE_string(endpoint, "", "");
DEFINE_string(bucket, "", "");

BenchmarkOption::BenchmarkOption()
    : start_s(base::time::TimeNow().seconds),
      op(FLAGS_op),
      local(FLAGS_local),
      threads(FLAGS_threads),
      blksize(FLAGS_blksize),
      blocks(FLAGS_blocks),
      page_size(64 * kKiB),
      writeback(FLAGS_writeback),
      retrive(FLAGS_retrive),
      ak(FLAGS_ak),
      sk(FLAGS_sk),
      endpoint(FLAGS_endpoint),
      bucket(FLAGS_bucket) {}

}  // namespace cache
}  // namespace dingofs
