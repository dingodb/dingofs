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

#include "cache/tools/cache_bench/options.h"

#include <gflags/gflags.h>

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>

namespace dingofs {
namespace cache {

DEFINE_uint32(threads, 1, "number of worker threads");
DEFINE_string(op, "put", "operation to run: put or range");
DEFINE_uint64(fsid, 1, "filesystem id used to fetch storage settings from mds");
DEFINE_string(block_size, "4MiB",
              "whole block size, e.g. 4mib, 4096kib, 4194304");
DEFINE_uint64(blocks, 1, "number of blocks processed by each worker");
DEFINE_string(range_offset, "0", "range-read offset inside each block");
DEFINE_string(range_length, "",
              "range-read length; empty means block_size - range_offset");
DEFINE_bool(writeback, false, "put to cache first, then write back to storage");
DEFINE_bool(retrieve_storage, true,
            "range reads may fall back to storage on cache miss");
DEFINE_uint32(report_interval, 3, "progress report interval in seconds");

namespace {

constexpr uint64_t kKiB = 1024;
constexpr uint64_t kMiB = 1024 * kKiB;
constexpr uint64_t kGiB = 1024 * kMiB;

std::string Trim(std::string value) {
  auto not_space = [](unsigned char c) { return !std::isspace(c); };
  value.erase(value.begin(),
              std::find_if(value.begin(), value.end(), not_space));
  value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(),
              value.end());
  return value;
}

std::string Lower(std::string value) {
  std::transform(
      value.begin(), value.end(), value.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

Status ParseSize(const std::string& input, uint64_t* value) {
  const std::string text = Trim(input);
  if (text.empty()) {
    return Status::InvalidParam("size is empty");
  }

  size_t pos = 0;
  while (pos < text.size() &&
         std::isdigit(static_cast<unsigned char>(text[pos]))) {
    ++pos;
  }
  if (pos == 0) {
    return Status::InvalidParam("size must start with a number: " + text);
  }

  uint64_t number = 0;
  try {
    number = std::stoull(text.substr(0, pos));
  } catch (const std::exception&) {
    return Status::InvalidParam("invalid size number: " + text);
  }

  const std::string suffix = Lower(Trim(text.substr(pos)));
  uint64_t multiplier = 1;
  if (suffix.empty() || suffix == "b") {
    multiplier = 1;
  } else if (suffix == "k" || suffix == "kb" || suffix == "kib") {
    multiplier = kKiB;
  } else if (suffix == "m" || suffix == "mb" || suffix == "mib") {
    multiplier = kMiB;
  } else if (suffix == "g" || suffix == "gb" || suffix == "gib") {
    multiplier = kGiB;
  } else {
    return Status::InvalidParam("unknown size suffix: " + suffix);
  }

  if (number > std::numeric_limits<uint64_t>::max() / multiplier) {
    return Status::InvalidParam("size is too large: " + text);
  }

  *value = number * multiplier;
  return Status::OK();
}

Status ParseOperation(const std::string& text, OperationType* operation) {
  const auto value = Lower(Trim(text));
  if (value == "put") {
    *operation = OperationType::kPut;
    return Status::OK();
  }
  if (value == "range") {
    *operation = OperationType::kRange;
    return Status::OK();
  }
  return Status::InvalidParam("op must be 'put' or 'range': " + text);
}

}  // namespace

Status LoadOptions(Options* options) {
  if (options == nullptr) {
    return Status::InvalidParam("options is null");
  }

  DINGOFS_RETURN_NOT_OK(ParseOperation(FLAGS_op, &options->operation));
  options->threads = FLAGS_threads;
  options->blocks_per_worker = FLAGS_blocks;
  options->writeback = FLAGS_writeback;
  options->retrieve_storage = FLAGS_retrieve_storage;
  options->report_interval_s = FLAGS_report_interval;

  if (FLAGS_fsid > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidParam("fsid exceeds uint32: " +
                                std::to_string(FLAGS_fsid));
  }
  options->fs_id = static_cast<uint32_t>(FLAGS_fsid);

  DINGOFS_RETURN_NOT_OK(ParseSize(FLAGS_block_size, &options->block_size));
  DINGOFS_RETURN_NOT_OK(ParseSize(FLAGS_range_offset, &options->range_offset));
  if (FLAGS_range_length.empty()) {
    if (options->range_offset >= options->block_size) {
      return Status::InvalidParam(
          "range_offset must be smaller than block_size");
    }
    options->range_length = options->block_size - options->range_offset;
  } else {
    DINGOFS_RETURN_NOT_OK(
        ParseSize(FLAGS_range_length, &options->range_length));
  }

  if (options->threads == 0) {
    return Status::InvalidParam("threads must be greater than 0");
  }
  if (options->blocks_per_worker == 0) {
    return Status::InvalidParam("blocks must be greater than 0");
  }
  if (options->block_size == 0) {
    return Status::InvalidParam("block_size must be greater than 0");
  }
  if (options->block_size > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidParam("block_size exceeds uint32: " +
                                std::to_string(options->block_size));
  }
  if (options->range_length == 0) {
    return Status::InvalidParam("range_length must be greater than 0");
  }
  if (options->range_offset > options->block_size ||
      options->range_length > options->block_size - options->range_offset) {
    return Status::InvalidParam(
        "range_offset + range_length must fit in block_size");
  }
  if (options->report_interval_s == 0) {
    return Status::InvalidParam("report_interval must be greater than 0");
  }
  if (options->TotalOps() / options->threads != options->blocks_per_worker) {
    return Status::InvalidParam("threads * blocks overflows uint64");
  }

  return Status::OK();
}

std::string OperationName(OperationType operation) {
  switch (operation) {
    case OperationType::kPut:
      return "put";
    case OperationType::kRange:
      return "range";
  }
  return "unknown";
}

std::string Usage(const char* program) {
  std::ostringstream os;
  os << "DingoFS cache benchmark\n\n"
     << "Usage:\n"
     << "  " << program
     << " --mds_addrs=HOST:PORT --fsid=ID --op=put [options]\n"
     << "  " << program
     << " --mds_addrs=HOST:PORT --fsid=ID --op=range [options]\n\n"
     << "Common options:\n"
     << "  --threads=N             Worker threads, default 1\n"
     << "  --blocks=N              Blocks per worker, default 1\n"
     << "  --block_size=SIZE       Block size, e.g. 4MiB or 4194304\n"
     << "  --report_interval=N     Progress interval in seconds, default 3\n\n"
     << "Put options:\n"
     << "  --writeback             Put to cache first, then write back to "
        "storage\n\n"
     << "Range options:\n"
     << "  --range_offset=SIZE     Offset inside each block, default 0\n"
     << "  --range_length=SIZE     Length per read, default block_size - "
        "offset\n"
     << "  --retrieve_storage      Fall back to storage on cache miss, default "
        "true\n\n"
     << "Examples:\n"
     << "  " << program << " --mds_addrs=127.0.0.1:7400 --fsid=1 --op=put "
     << "--threads=8 --blocks=100 --block_size=4MiB\n"
     << "  " << program << " --mds_addrs=127.0.0.1:7400 --fsid=1 --op=range "
     << "--threads=8 --blocks=100 --range_length=1MiB\n";
  return os.str();
}

std::string FormatBytes(uint64_t bytes) {
  const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
  double value = static_cast<double>(bytes);
  size_t unit = 0;
  while (value >= 1024.0 && unit + 1 < sizeof(units) / sizeof(units[0])) {
    value /= 1024.0;
    ++unit;
  }

  std::ostringstream os;
  if (unit == 0) {
    os << bytes << ' ' << units[unit];
  } else {
    os << std::fixed << std::setprecision(2) << value << ' ' << units[unit];
  }
  return os.str();
}

std::string FormatDuration(double seconds) {
  std::ostringstream os;
  if (seconds < 60.0) {
    os << std::fixed << std::setprecision(2) << seconds << " s";
  } else {
    const auto minutes = static_cast<uint64_t>(seconds / 60);
    const double rest = seconds - minutes * 60;
    os << minutes << "m " << std::fixed << std::setprecision(1) << rest << 's';
  }
  return os.str();
}

}  // namespace cache
}  // namespace dingofs
