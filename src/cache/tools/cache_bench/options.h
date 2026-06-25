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

#ifndef DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_OPTIONS_H_
#define DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_OPTIONS_H_

#include <gflags/gflags_declare.h>

#include <cstdint>
#include <string>

#include "common/status.h"

namespace dingofs {
namespace cache {

DECLARE_uint32(threads);
DECLARE_string(op);
DECLARE_uint64(fsid);
DECLARE_string(block_size);
DECLARE_uint64(blocks);
DECLARE_string(range_offset);
DECLARE_string(range_length);
DECLARE_bool(writeback);
DECLARE_bool(retrieve_storage);
DECLARE_uint32(report_interval);

enum class OperationType { kPut, kRange };

struct Options {
  OperationType operation{OperationType::kPut};
  uint32_t threads{1};
  uint32_t fs_id{1};
  uint64_t block_size{4 * 1024 * 1024};
  uint64_t blocks_per_worker{1};
  uint64_t range_offset{0};
  uint64_t range_length{4 * 1024 * 1024};
  bool writeback{false};
  bool retrieve_storage{true};
  uint32_t report_interval_s{3};

  uint64_t TotalOps() const {
    return static_cast<uint64_t>(threads) * blocks_per_worker;
  }

  uint64_t BytesPerOp() const {
    return operation == OperationType::kRange ? range_length : block_size;
  }

  uint64_t TotalBytes() const { return TotalOps() * BytesPerOp(); }
};

Status LoadOptions(Options* options);

std::string OperationName(OperationType operation);
std::string Usage(const char* program);
std::string FormatBytes(uint64_t bytes);
std::string FormatDuration(double seconds);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_OPTIONS_H_
