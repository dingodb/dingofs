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
#include <string>

#include "fmt/core.h"
#include "glog/logging.h"

namespace dingofs {

std::unique_ptr<const char[]> Status::CopyState(const char* s) {
  const size_t cch = std::strlen(s) + 1;  // +1 for the null terminator
  char* rv = new char[cch];
  std::strncpy(rv, s, cch);
  return std::unique_ptr<const char[]>(rv);
}

Status::Status(Code code, int32_t p_errno, const StringSlice& msg,
               const StringSlice& msg2)
    : code_(code), errno_(p_errno) {
  const uint32_t len1 = static_cast<uint32_t>(msg.size());
  const uint32_t len2 = static_cast<uint32_t>(msg2.size());
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);

  char* const result = new char[size + 1];  // +1 for null terminator
  memcpy(result, msg.data(), len1);

  if (len2) {
    result[len1] = ':';
    result[len1 + 1] = ' ';
    memcpy(result + len1 + 2, msg2.data(), len2);
  }
  result[size] = '\0';  // null terminator for C style string
  state_.reset(result);
}

std::string Status::ToString() const {
  if (state_ == nullptr) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code_) {
      case kOk:
        type = "OK";
        break;
      case kInternal:
        type = "Internal";
        break;
      case kUnknown:
        type = "Unknown";
        break;
      case kExist:
        type = "Exist";
        break;
      case kNotExist:
        type = "NotExist";
        break;
      case kNoSpace:
        type = "NoSpace";
        break;
      case kBadFd:
        type = "BadFd";
        break;
      case kInvaildParam:
        type = "InvaildParam";
        break;
      case kNoPermission:
        type = "NoPermission";
        break;
      case kNotEmpty:
        type = "NotEmpty";
        break;
      case kNoFlush:
        type = "NoFlush";
        break;
      case kNotSupport:
        type = "NotSupport";
        break;
      case kNameTooLong:
        type = "NameTooLong";
        break;
      case kMountMountExist:
        type = "MountMountExist";
        break;
      case kMountFailed:
        type = "MountFailed";
        break;
      case kOutOfRange:
        type = "OutOfRange";
        break;
      case kNoData:
        type = "NoData";
        break;
      case kIoError:
        type = "IoError";
        break;
      case kStale:
        type = "Stale";
        break;
      case kNoSys:
        type = "NoSys";
        break;
      case kNoPermitted:
        type = "NoPermitted";
        break;
      case kNetError:
        type = "NetError";
        break;
      case kNotFound:
        type = "NotFound";
        break;
      case kNotDirectory:
        type = "NotDirectory";
        break;
      case kFileTooLarge:
        type = "FileTooLarge";
        break;
      case kEndOfFile:
        type = "EndOfFile";
        break;
      case kAbort:
        type = "Abort";
        break;
      case kCacheDown:
        type = "CacheDown";
        break;
      case kCacheUnhealthy:
        type = "CacheUnhealthy";
        break;
      case kCacheFull:
        type = "CacheFull";
        break;
      case kNotFit:
        type = "NotFit";
        break;
      default:
        type = std::to_string(code_).c_str();
        LOG(ERROR) << fmt::format("Unknown code({}):", static_cast<int>(code_));
    }

    std::string result(type);
    if (errno_ != kNone) {
      result.append(fmt::format(" (errno:{}) ", errno_));
    }

    if (state_ != nullptr) {
      result.append(": ");
      result.append(state_.get());
    }

    return result;
  }
}

}  // namespace dingofs
