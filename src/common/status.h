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

#ifndef DINGOFS_COMMON_STATUS_H_
#define DINGOFS_COMMON_STATUS_H_

#include <cerrno>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "common/string_slice.h"

namespace dingofs {

/// @brief Return the given status if it is not @c OK.
#define DINGOFS_RETURN_NOT_OK(s)       \
  do {                                 \
    const ::dingofs::Status& _s = (s); \
    if (!_s.IsOK()) return _s;         \
  } while (0)

#undef DECLARE_ERROR_STATUS

#define DECLARE_ERROR_STATUS(NAME, CODE)                        \
  static Status NAME(const StringSlice& msg,                    \
                     const StringSlice& msg2 = StringSlice()) { \
    return Status(CODE, kNone, msg, msg2);                      \
  };                                                            \
  static Status NAME(int32_t p_errno, const StringSlice& msg,   \
                     const StringSlice& msg2 = StringSlice()) { \
    return Status(CODE, p_errno, msg, msg2);                    \
  }                                                             \
  bool Is##NAME() const { return code_ == (CODE); }

class Status {
 private:
  enum Code : uint8_t {
    kOk = 0,
    kInternal = 1,
    kUnknown = 2,
    kExist = 3,
    kNotExist = 4,
    kNoSpace = 5,
    kBadFd = 6,
    kInvaildParam = 7,
    kNoPermission = 8,
    kNotEmpty = 9,
    kNoFlush = 10,
    kNotSupport = 11,
    kNameTooLong = 12,
    kMountMountExist = 13,
    kMountFailed = 14,
    kOutOfRange = 15,
    kNoData = 16,
    kIoError = 17,
    kStale = 18,
    kNoSys = 19,
    kNoPermitted = 20,
    kNetError = 21,
    kNotFound = 22,
    kNotDirectory = 23,
    kFileTooLarge = 24,
    kEndOfFile = 25,
    kAbort = 26,
    kCacheDown = 27,
    kCacheUnhealthy = 28,
    kCacheFull = 29,
    kStop = 30,
    kNotFit = 31,
    kTimeout = 32,
  };
  static const int32_t kNone = 0;

 public:
  // Create a success status.
  Status() noexcept : code_(kOk), state_(nullptr) {}
  ~Status() = default;

  Status(const Status& rhs);
  Status& operator=(const Status& rhs);

  Status(Status&& rhs) noexcept;
  Status& operator=(Status&& rhs) noexcept;
  bool operator==(const Status& rhs) const { return code_ == rhs.code_; }
  bool operator!=(const Status& rhs) const { return code_ != rhs.code_; }

  bool ok() const { return code_ == kOk; }  // NOLINT
  static Status OK() { return Status(); }

  DECLARE_ERROR_STATUS(OK, kOk);
  DECLARE_ERROR_STATUS(Internal, kInternal);
  DECLARE_ERROR_STATUS(Unknown, kUnknown);
  DECLARE_ERROR_STATUS(Exist, kExist);
  DECLARE_ERROR_STATUS(NotExist, kNotExist);
  DECLARE_ERROR_STATUS(NoSpace, kNoSpace);
  DECLARE_ERROR_STATUS(BadFd, kBadFd);
  DECLARE_ERROR_STATUS(InvalidParam, kInvaildParam);
  DECLARE_ERROR_STATUS(NoPermission, kNoPermission);
  DECLARE_ERROR_STATUS(NotEmpty, kNotEmpty);
  DECLARE_ERROR_STATUS(NoFlush, kNoFlush);
  DECLARE_ERROR_STATUS(NotSupport, kNotSupport);
  DECLARE_ERROR_STATUS(NameTooLong, kNameTooLong);
  DECLARE_ERROR_STATUS(MountMountExist, kMountMountExist);
  DECLARE_ERROR_STATUS(MountFailed, kMountFailed);
  DECLARE_ERROR_STATUS(OutOfRange, kOutOfRange);
  DECLARE_ERROR_STATUS(NoData, kNoData);
  DECLARE_ERROR_STATUS(IoError, kIoError);
  DECLARE_ERROR_STATUS(Stale, kStale);
  DECLARE_ERROR_STATUS(NoSys, kNoSys);
  DECLARE_ERROR_STATUS(NoPermitted, kNoPermitted);
  DECLARE_ERROR_STATUS(NetError, kNetError);
  DECLARE_ERROR_STATUS(Timeout, kTimeout);
  DECLARE_ERROR_STATUS(NotFound, kNotFound);
  DECLARE_ERROR_STATUS(NotDirectory, kNotDirectory);
  DECLARE_ERROR_STATUS(FileTooLarge, kFileTooLarge);
  DECLARE_ERROR_STATUS(EndOfFile, kEndOfFile);
  DECLARE_ERROR_STATUS(Abort, kAbort);
  DECLARE_ERROR_STATUS(CacheDown, kCacheDown);
  DECLARE_ERROR_STATUS(CacheUnhealthy, kCacheUnhealthy);
  DECLARE_ERROR_STATUS(CacheFull, kCacheFull);
  DECLARE_ERROR_STATUS(Stop, kStop);
  DECLARE_ERROR_STATUS(NotFit, kNotFit);

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

  int32_t Errno() const { return errno_; }

  int ToSysErrNo() const {
    switch (code_) {
      case kOk:
        return 0;
      case kInternal:
        return EIO;
      case kUnknown:
        return EIO;
      case kExist:
        return EEXIST;
      case kNotExist:
        return ENOENT;
      case kNoSpace:
        return ENOSPC;
      case kBadFd:
        return EBADF;
      case kInvaildParam:
        return EINVAL;
      case kNoPermission:
        return EACCES;
      case kNotEmpty:
        return ENOTEMPTY;
      case kNoFlush:
        return EIO;
      case kNotSupport:
        return EOPNOTSUPP;
      case kNameTooLong:
        return ENAMETOOLONG;
      case kMountMountExist:
        return EIO;
      case kMountFailed:
        return EIO;
      case kOutOfRange:
        return ERANGE;
      case kNoData:
        return ENODATA;
      case kIoError:
        return EIO;
      case kStale:
        return ESTALE;
      case kNoSys:
        return ENOSYS;
      case kNoPermitted:
        return EPERM;
      case kNetError:
        return EIO;
      case kNotFound:
        return ENOENT;
      case kStop:
        return EIO;
      case kNotFit:
        return EIO;
      case kTimeout:
        return ETIMEDOUT;
      default:
        return EIO;
    }
  }

 private:
  Status(Code code, int32_t p_errno, const StringSlice& msg,
         const StringSlice& msg2);

  static std::unique_ptr<const char[]> CopyState(const char* s);

  Code code_;
  int32_t errno_;
  // A nullptr state_ (which is at least the case for OK) means the extra
  // message is empty.
  std::unique_ptr<const char[]> state_;
};

inline Status::Status(const Status& rhs)
    : code_(rhs.code_), errno_(rhs.errno_) {
  state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_.get());
}

inline Status& Status::operator=(const Status& rhs) {
  if (this != &rhs) {
    code_ = rhs.code_;
    errno_ = rhs.errno_;
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_.get());
  }
  return *this;
}

inline Status::Status(Status&& rhs) noexcept : Status() {
  *this = std::move(rhs);
}

inline Status& Status::operator=(Status&& rhs) noexcept {
  if (this != &rhs) {
    code_ = rhs.code_;
    errno_ = rhs.errno_;
    state_ = std::move(rhs.state_);
  }
  return *this;
}

#undef DECLARE_ERROR_STATUS

}  // namespace dingofs

#endif  // DINGOFS_COMMON_STATUS_H_