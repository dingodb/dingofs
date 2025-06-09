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

#ifndef DINGOFS_SRC_CACHE_CONFIG_BENCHMARK_H_
#define DINGOFS_SRC_CACHE_CONFIG_BENCHMARK_H_

#include <gflags/gflags_declare.h>

#include "cache/blockcache/block_cache.h"
#include "cache/config/block_cache.h"
#include "cache/config/remote_cache.h"

namespace dingofs {
namespace cache {

DECLARE_string(op);
DECLARE_uint32(threads);
DECLARE_uint32(page_size);
DECLARE_uint32(op_blksize);
DECLARE_uint32(op_blocks);
DECLARE_bool(put_writeback);
DECLARE_bool(range_retrive);
DECLARE_uint64(fsid);
DECLARE_uint64(ino);
DECLARE_string(s3_ak);
DECLARE_string(s3_sk);
DECLARE_string(s3_endpoint);
DECLARE_string(s3_bucket);
DECLARE_uint32(stat_interval_s);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CONFIG_BENCHMARK_H_
