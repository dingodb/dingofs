/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Dingofs
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#include "client/vfs_old/filesystem/error.h"

#include <map>
#include <ostream>
#include <utility>

#include "common/status.h"

namespace dingofs {
namespace client {
namespace filesystem {

using pb::metaserver::MetaStatusCode;

static const std::map<DINGOFS_ERROR, std::pair<int, std::string>> errors = {
    {DINGOFS_ERROR::OK, {0, "OK"}},
    {DINGOFS_ERROR::INTERNAL, {EIO, "internal error"}},
    {DINGOFS_ERROR::UNKNOWN, {-1, "unknown"}},
    {DINGOFS_ERROR::EXISTS, {EEXIST, "inode or dentry already exist"}},
    {DINGOFS_ERROR::NOTEXIST, {ENOENT, "inode or dentry not exist"}},
    {DINGOFS_ERROR::NO_SPACE, {ENOSPC, "no space to alloc"}},
    {DINGOFS_ERROR::BAD_FD, {EBADF, "bad file number"}},
    {DINGOFS_ERROR::INVALIDPARAM, {EINVAL, "invalid argument"}},
    {DINGOFS_ERROR::NOPERMISSION, {EACCES, "permission denied"}},
    {DINGOFS_ERROR::NOTEMPTY, {ENOTEMPTY, "directory not empty"}},
    {DINGOFS_ERROR::NOFLUSH, {-1, "no flush"}},
    {DINGOFS_ERROR::NOTSUPPORT, {EOPNOTSUPP, "operation not supported"}},
    {DINGOFS_ERROR::NAMETOOLONG, {ENAMETOOLONG, "file name too long"}},
    {DINGOFS_ERROR::MOUNT_POINT_EXIST, {-1, "mount point already exist"}},
    {DINGOFS_ERROR::MOUNT_FAILED, {-1, "mount failed"}},
    {DINGOFS_ERROR::OUT_OF_RANGE, {ERANGE, "out of range"}},
    {DINGOFS_ERROR::NODATA, {ENODATA, "no data available"}},
    {DINGOFS_ERROR::IO_ERROR, {EIO, "I/O error"}},
    {DINGOFS_ERROR::STALE, {ESTALE, "stale file handler"}},
    {DINGOFS_ERROR::NOSYS, {ENOSYS, "invalid system call"}},
    {DINGOFS_ERROR::NOPERMITTED, {EPERM, "Operation not permitted"}}

};

std::string StrErr(DINGOFS_ERROR code) {
  auto it = errors.find(code);
  if (it != errors.end()) {
    return it->second.second;
  }
  return "unknown";
}

int SysErr(DINGOFS_ERROR code) {
  int syscode = -1;
  auto it = errors.find(code);
  if (it != errors.end()) {
    syscode = it->second.first;
  }
  return (syscode == -1) ? EIO : syscode;
}

std::ostream& operator<<(std::ostream& os, DINGOFS_ERROR code) {
  os << static_cast<int>(code) << "[" << [code]() {
    auto it = errors.find(code);
    if (it != errors.end()) {
      return it->second.second;
    }

    return std::string{"Unknown"};
  }() << "]";

  return os;
}

DINGOFS_ERROR ToFSError(MetaStatusCode code) {
  static std::map<MetaStatusCode, DINGOFS_ERROR> errs = {
      {MetaStatusCode::OK, DINGOFS_ERROR::OK},
      {MetaStatusCode::NOT_FOUND, DINGOFS_ERROR::NOTEXIST},
      {MetaStatusCode::PARAM_ERROR, DINGOFS_ERROR::INVALIDPARAM},
      {MetaStatusCode::INODE_EXIST, DINGOFS_ERROR::EXISTS},
      {MetaStatusCode::DENTRY_EXIST, DINGOFS_ERROR::EXISTS},
      {MetaStatusCode::SYM_LINK_EMPTY, DINGOFS_ERROR::INTERNAL},
      {MetaStatusCode::RPC_ERROR, DINGOFS_ERROR::INTERNAL},
  };

  auto it = errs.find(code);
  if (it != errs.end()) {
    return it->second;
  }
  return DINGOFS_ERROR::UNKNOWN;
}

Status DingofsErrorToStatus(DINGOFS_ERROR code) {
  int err_num = SysErr(code);
  switch (err_num) {
    case 0:
      return Status::OK();
    case -1:
      return Status::IoError(StrErr(code));
    case EEXIST:
      return Status::Exist(StrErr(code));
    case ENOENT:
      return Status::NotExist(StrErr(code));
    case ENOSPC:
      return Status::NoSpace(StrErr(code));
    case EBADF:
      return Status::BadFd(StrErr(code));
    case EINVAL:
      return Status::InvalidParam(StrErr(code));
    case EACCES:
      return Status::NoPermission(StrErr(code));
    case ENOTEMPTY:
      return Status::NotEmpty(StrErr(code));
    case EOPNOTSUPP:
      return Status::NotSupport(StrErr(code));
    case ENAMETOOLONG:
      return Status::NameTooLong(StrErr(code));
    case ERANGE:
      return Status::OutOfRange(StrErr(code));
    case ENODATA:
      return Status::NoData(StrErr(code));
    case EIO:
      return Status::IoError(StrErr(code));
    case ESTALE:
      return Status::Stale(StrErr(code));
    case ENOSYS:
      return Status::NoSys(StrErr(code));
    case EPERM:
      return Status::NoPermitted(StrErr(code));
    default:
      return Status::Unknown(StrErr(code));
  }
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs
