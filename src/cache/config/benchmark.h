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

DECLARE_string(op);   // put/async_put/range/async_range
DECLARE_bool(local);  // put/async_put/range/async_range
DECLARE_uint32(threads);
DECLARE_uint32(blksize);
DECLARE_uint32(blocks);
DECLARE_bool(writeback);
DECLARE_bool(retrive);
DECLARE_string(ak);
DECLARE_string(sk);
DECLARE_string(endpoint);
DECLARE_string(bucket);

struct BenchmarkOption {
  BenchmarkOption();

  // job
  uint64_t start_s;
  bool local;
  std::string op;
  uint32_t threads;
  uint32_t blksize;
  uint32_t blocks;
  uint32_t page_size;
  bool writeback;
  bool retrive;

  // storage
  std::string ak;
  std::string sk;
  std::string endpoint;
  std::string bucket;

  BlockCacheOption block_cache_option;
  RemoteBlockCacheOption remote_block_cache_option;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CONFIG_BENCHMARK_H_
