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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_FS_OPTIONS_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_FS_OPTIONS_H_

#include <cstdint>
#include <string>

#include "cache/tools/bench/common/flags.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace fs {

enum class Rw : uint8_t { kRead, kWrite, kRandRead, kRandWrite, kRandRw };

struct Options {
  std::string dir;
  uint64_t bs{1024 * 1024};  // block size = whole-file size, <= 4MiB
  uint64_t nrfiles{1024};    // distinct block files (key space)
  std::string rw_str{"randread"};
  Rw rw{Rw::kRandRead};
  uint32_t rwmixread{70};
  uint32_t jobs{0};        // concurrent caller threads; 0 => iodepth
  uint32_t iodepth{128};   // caps in-flight AIO, sizes slab pool
  uint32_t runtime_s{10};
  uint64_t io_size{0};
  uint32_t warmup_s{0};
  bool prep{true};
  bool keep{false};
  uint32_t report_interval_s{3};

  bool DoesRead() const {
    return rw == Rw::kRead || rw == Rw::kRandRead || rw == Rw::kRandRw;
  }
  bool IsRandom() const {
    return rw == Rw::kRandRead || rw == Rw::kRandWrite || rw == Rw::kRandRw;
  }
};

void RegisterFlags(FlagSet* fs, Options* o);
std::string Validate(Options* o);
std::string RwName(Rw rw);

}  // namespace fs
}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_FS_OPTIONS_H_
