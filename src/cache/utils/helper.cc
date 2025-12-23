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
 * Created Date: 2025-05-21
 * Author: Jingli Chen (Wine93)
 */

#include "cache/utils/helper.h"

#include <absl/strings/match.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <butil/file_util.h>
#include <butil/memory/aligned_memory.h>
#include <google/protobuf/util/json_util.h>

#include <numeric>
#include <string>

#include "utils/string.h"

namespace dingofs {
namespace cache {

static const uint64_t kIOAlignedBlockSize = 4096;
static const std::string kTempFileSuffix = ".tmp";

// string
std::string Helper::ToLowerCase(const std::string& str) {
  std::string result = str;
  for (char& c : result) {
    c = tolower(c);
  }
  return result;
}

std::string Helper::RedString(const std::string& str) {
  return absl::StrFormat("\x1B[31m%s\033[0m", str);
}

// sys conf
int Helper::GetProcessCores() { return sysconf(_SC_NPROCESSORS_ONLN); }

uint64_t Helper::GetSysPageSize() { return sysconf(_SC_PAGESIZE); }

uint64_t Helper::GetIOAlignedBlockSize() { return kIOAlignedBlockSize; }

bool Helper::IsAligned(uint64_t n, uint64_t m) { return n % m == 0; }

// time
int64_t Helper::TimestampNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::TimestampUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::Timestamp() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string Helper::NowTime() {
  return FormatMsTime(TimestampMs(), "%Y-%m-%d %H:%M:%S");
}

std::string Helper::FormatMsTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>
      tp((std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str()) << "."
     << timestamp % 1000;
  return ss.str();
}

// filepath
std::string Helper::ParentDir(const std::string& path) {
  size_t index = path.find_last_of('/');
  if (index == std::string::npos) {
    return "/";
  }

  std::string parent = path.substr(0, index);
  if (parent.empty()) {
    return "/";
  }
  return parent;
}

std::string Helper::Filename(const std::string& path) {
  size_t index = path.find_last_of('/');
  if (index == std::string::npos) {
    return path;
  }
  return path.substr(index + 1, path.length());
}

bool Helper::HasSuffix(const std::string& path, const std::string& suffix) {
  return absl::EndsWith(path, suffix);
}

std::string Helper::PathJoin(const std::vector<std::string>& subpaths) {
  return absl::StrJoin(subpaths, "/");
}

std::string Helper::TempFilepath(const std::string& filepath) {
  return absl::StrFormat("%s.%lld%s", filepath, TimestampNs(), kTempFileSuffix);
}

bool Helper::IsTempFilepath(const std::string& filepath) {
  return HasSuffix(filepath, kTempFileSuffix);
}

std::string Helper::StrMode(uint16_t mode) {
  static std::unordered_map<uint16_t, char> type2char = {
      {S_IFSOCK, 's'}, {S_IFLNK, 'l'}, {S_IFREG, '-'}, {S_IFBLK, 'b'},
      {S_IFDIR, 'd'},  {S_IFCHR, 'c'}, {S_IFIFO, 'f'}, {0, '?'},
  };

  std::string s("?rwxrwxrwx");
  s[0] = type2char[mode & (S_IFMT & 0xffff)];
  if (mode & S_ISUID) {
    s[3] = 's';
  }
  if (mode & S_ISGID) {
    s[6] = 's';
  }
  if (mode & S_ISVTX) {
    s[9] = 't';
  }

  for (auto i = 0; i < 9; i++) {
    if ((mode & (1 << i)) == 0) {
      if ((s[9 - i] == 's') || (s[9 - i] == 't')) {
        s[9 - i] &= 0xDF;
      } else {
        s[9 - i] = '-';
      }
    }
  }
  return s;
}

// validator
bool Helper::NonEmptyString(const char* /*name*/, const std::string& value) {
  return !value.empty();
}

// others
std::vector<uint64_t> Helper::NormalizeByGcd(
    const std::vector<uint64_t>& nums) {
  uint64_t gcd = 0;
  std::vector<uint64_t> out;
  for (const auto& num : nums) {
    out.push_back(num);
    gcd = std::gcd(gcd, num);
  }
  CHECK_NE(gcd, 0);

  for (auto& num : out) {
    num = num / gcd;
  }
  return out;
}

void Helper::DeleteBuffer(void* data) { delete[] static_cast<char*>(data); }

bool Helper::IsAligned(const IOBuffer& buffer) {
  auto aligned_block_size = GetIOAlignedBlockSize();
  const auto& iovec = buffer.Fetch();
  for (const auto& vec : iovec) {
    if (!IsAligned(reinterpret_cast<std::uintptr_t>(vec.iov_base),
                   aligned_block_size)) {
      return false;
    } else if (!IsAligned(vec.iov_len, aligned_block_size)) {
      return false;
    }
  }
  return true;
}

double Helper::Divide(uint64_t a, uint64_t b) {
  CHECK_NE(b, 0);
  return static_cast<double>(a) / static_cast<double>(b);
}

bool Helper::ProtoToJson(const google::protobuf::Message& message,
                         std::string& json) {
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  options.preserve_proto_field_names = true;
  return MessageToJsonString(message, &json, options).ok();
}

void Helper::SplitUniteCacheDir(
    const std::string& cache_dir, uint64_t default_cache_size_mb,
    std::vector<std::pair<std::string, uint64_t>>* cache_dirs) {
  std::vector<std::string> dirs = absl::StrSplit(cache_dir, ";");

  for (const auto& dir : dirs) {
    uint64_t cache_size_mb = default_cache_size_mb;
    std::vector<std::string> items = absl::StrSplit(dir, ":");
    if (items.size() > 2 ||
        (items.size() == 2 && !utils::Str2Int(items[1], &cache_size_mb))) {
      CHECK(false) << "Invalid cache dir: " << dir;
    } else if (cache_size_mb == 0) {
      CHECK(false) << "Cache size must greater than 0.";
    }

    cache_dirs->emplace_back(items[0], cache_size_mb);
  }
}

}  // namespace cache
}  // namespace dingofs
