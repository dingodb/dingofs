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

#include "common/status.h"

#include <cstdio>
#include <iterator>
#include <string>
#include <string_view>

#include "absl/base/optimization.h"
#include "fmt/core.h"
#include "fmt/format.h"

namespace dingofs {

Status::Status(Code code, int32_t p_errno, const StringSlice& msg,
               const StringSlice& msg2)
    : code_(code), errno_(p_errno) {
  const size_t len1 = msg.size();
  const size_t len2 = msg2.size();

  msg_.reserve(len1 + (len2 ? (2 + len2) : 0));
  msg_.insert(msg_.end(), msg.data(), msg.data() + len1);
  if (len2) {
    msg_.push_back(':');
    msg_.push_back(' ');
    msg_.insert(msg_.end(), msg2.data(), msg2.data() + len2);
  }
}

std::string Status::ToString() const {
  // Indexed by Code; must stay in the same order as the enum.
  static constexpr std::string_view kCodeNames[] = {
      "OK",           "Internal",        "Unknown",
      "Exist",        "NotExist",        "NoSpace",
      "BadFd",        "InvalidParam",    "NoPermission",
      "NotEmpty",     "NoFlush",         "NotSupport",
      "NameTooLong",  "MountPointExist", "MountFailed",
      "OutOfRange",   "NoData",          "IoError",
      "Stale",        "NoSys",           "NoPermitted",
      "NetError",     "NotFound",        "NotDirectory",
      "FileTooLarge", "EndOfFile",       "Abort",
      "CacheDown",    "CacheUnhealthy",  "CacheFull",
      "Stop",         "NotFit",          "Timeout",
      "OutOfMemory",  "Deleted",
  };
  static_assert(std::size(kCodeNames) == kDeleted + 1,
                "kCodeNames must cover every Code value");

  const size_t index = static_cast<size_t>(code_);
  std::string unknown_name;  // backing storage when code_ is out of range
  std::string_view type;
  if (ABSL_PREDICT_TRUE(index < std::size(kCodeNames))) {
    type = kCodeNames[index];
  } else {
    unknown_name = std::to_string(index);
    type = unknown_name;
  }

  const bool has_errno = (errno_ != kNone);
  const bool has_msg = !msg_.empty();

  // fmt::format_int converts on the stack, avoiding the heap-allocated
  // temporary that fmt::format would produce.
  fmt::format_int errno_str(errno_);

  // Exact size up front: at most one allocation on every path.
  std::string result;
  result.reserve(type.size() + (has_errno ? 10 + errno_str.size() : 0) +
                 (has_msg ? 2 + msg_.size() : 0));
  result.append(type);
  if (has_errno) {
    result.append(" (errno:");
    result.append(errno_str.data(), errno_str.size());
    result.append(") ");
  }

  if (has_msg) {
    result.append(": ");
    result.append(msg_.data(), msg_.size());
  }

  return result;
}

}  // namespace dingofs
