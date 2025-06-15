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
DEFINE_uint32(threads, 1, "");
DEFINE_uint32(page_size, 65536, "");
DEFINE_uint32(op_blksize, 4194304, "");
DEFINE_uint32(op_blocks, 1, "");
DEFINE_bool(put_writeback, true, "");
DEFINE_bool(range_retrive, false, "");
DEFINE_uint64(fsid, 1, "");
DEFINE_uint64(ino, 0, "");
DEFINE_string(s3_ak, "", "");
DEFINE_string(s3_sk, "", "");
DEFINE_string(s3_endpoint, "", "");
DEFINE_string(s3_bucket, "", "");
DEFINE_uint32(stat_interval_s, 3, "");

}  // namespace cache
}  // namespace dingofs
