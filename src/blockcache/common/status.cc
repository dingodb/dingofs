/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "blockcache/common/status.h"

#include <cerrno>
#include <cstring>
#include <string>

namespace dingofs {
namespace blockcache {

Status ToStatus(int sys_code, const char* what) {
  const std::string message =
      std::string("Fail to ") + what + ": " + std::strerror(sys_code);
  switch (sys_code) {
    case ENOMEM:
      return Status::OutOfMemory(sys_code, message);
    case EINVAL:
    case EFAULT:
      return Status::InvalidParam(sys_code, message);
    case ENOENT:
      return Status::NotExist(sys_code, message);
    case EACCES:
    case EPERM:
      return Status::NoPermission(sys_code, message);
    case ENOSPC:
    case EDQUOT:
      return Status::NoSpace(sys_code, message);
    case EBADF:
      return Status::BadFd(sys_code, message);
    case EIO:
    case EROFS:
    case EFBIG:
    case ENOTDIR:
    case EISDIR:
    case ENAMETOOLONG:
    case EMFILE:
    case ENFILE:
      return Status::IoError(sys_code, message);
    case ENODEV:
    case ENETDOWN:
    case ENOTCONN:
    case ECONNRESET:
    case ECONNABORTED:
    case ECONNREFUSED:
      return Status::NetError(sys_code, message);
    case EPROTO:
    case EPROTONOSUPPORT:
      return Status::NotSupport(sys_code, message);
    case EMSGSIZE:
      return Status::OutOfRange(sys_code, message);
    default:
      return Status::Internal(sys_code, message);
  }
}

Status ToStatus(pb::error::Errno errno_code) {
  switch (errno_code) {
    case pb::error::OK:
      return Status::OK();
    case pb::error::ENOT_FOUND:
      return Status::NotFound("not found");
    case pb::error::EILLEGAL_PARAMTETER:
      return Status::InvalidParam("invalid param");
    case pb::error::ENOT_SUPPORT:
      return Status::NotSupport("not support");
    case pb::error::EOUT_OF_RANGE:
      return Status::OutOfRange("out of range");
    case pb::error::ECACHE_FULL:
      return Status::CacheFull("cache full");
    case pb::error::ECACHE_DOWN:
      return Status::CacheDown("cache down");
    case pb::error::ECACHE_UNHEALTHY:
      return Status::CacheUnhealthy("cache unhealthy");
    case pb::error::ECACHE_IO_ERROR:
      return Status::IoError("cache io error");
    case pb::error::ECACHE_BUSY:
      return Status::OutOfMemory("cache busy");
    default:
      return Status::Internal("errno " + std::to_string(errno_code));
  }
}

Status ToStatus(ibv_wc_status status, const char* what) {
  if (status == IBV_WC_SUCCESS) {
    return Status::OK();
  }
  const std::string message =
      std::string("Fail to ") + what + ": " + ibv_wc_status_str(status);
  return Status::IoError(static_cast<int32_t>(status), message);
}

pb::error::Errno ToErrno(const Status& status) {
  if (status.ok()) return pb::error::OK;
  if (status.IsNotFound() || status.IsNotExist()) return pb::error::ENOT_FOUND;
  if (status.IsInvalidParam()) return pb::error::EILLEGAL_PARAMTETER;
  if (status.IsNotSupport()) return pb::error::ENOT_SUPPORT;
  if (status.IsOutOfRange()) return pb::error::EOUT_OF_RANGE;
  if (status.IsCacheFull()) return pb::error::ECACHE_FULL;
  if (status.IsCacheDown() || status.IsStop()) return pb::error::ECACHE_DOWN;
  if (status.IsCacheUnhealthy()) return pb::error::ECACHE_UNHEALTHY;
  if (status.IsIoError()) return pb::error::ECACHE_IO_ERROR;
  if (status.IsOutOfMemory()) return pb::error::ECACHE_BUSY;
  return pb::error::EINTERNAL;
}

}  // namespace blockcache
}  // namespace dingofs
