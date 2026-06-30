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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_CLIENT_OPTIONS_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_CLIENT_OPTIONS_H_

#include <cstdint>
#include <string>

#include "cache/tools/bench/common/flags.h"
#include "cache/tools/bench/common/profiler.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace client {

enum class OperationType : uint8_t { kPut, kRange };
enum class KeyDist : uint8_t { kSeq, kUniform, kZipf };

struct Options {
  std::string op_str{"put"};
  OperationType operation{OperationType::kPut};
  std::string mds_addrs;
  uint32_t threads{4};
  uint32_t fs_id{1};
  uint64_t block_size{4 * 1024 * 1024};
  uint64_t range_offset{0};
  uint64_t range_length{0};  // 0 => block_size - range_offset
  bool writeback{false};
  bool retrieve_storage{true};
  uint32_t report_interval_s{3};

  // open-loop load generation
  double qps{0};
  uint32_t max_inflight{256};
  uint32_t duration_s{0};
  uint64_t ops{0};
  uint32_t warmup_s{0};

  // key distribution
  uint64_t keyspace{1024};
  std::string key_dist_str{"seq"};
  KeyDist key_dist{KeyDist::kSeq};
  double zipf_theta{0.99};

  ProfileOptions profile;

  uint64_t BytesPerOp() const {
    return operation == OperationType::kRange ? range_length : block_size;
  }
};

void RegisterFlags(FlagSet* fs, Options* o);
std::string Validate(Options* o);
std::string OperationName(OperationType op);
std::string KeyDistName(KeyDist dist);

}  // namespace client
}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_CLIENT_OPTIONS_H_
