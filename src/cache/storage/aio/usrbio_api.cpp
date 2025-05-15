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
 * Created Date: 2025-04-09
 * Author: Jingli Chen (Wine93)
 */

#include "cache/storage/aio/usrbio_api.h"

#include "cache/utils/helper.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace storage {

namespace hf3fs {

using dingofs::cache::utils::Errorf;

Status extract_mount_point(const std::string& path, std::string* mountpoint) {
  char mp[4096];
  int n = hf3fs_extract_mount_point(mp, sizeof(mp), path.c_str());
  if (n <= 0) {
    LOG(ERROR) << "hf3fs_extract_mount_point(" << mountpoint
               << ") failed: rc = " << n;
    return Status::Internal("hf3fs_extract_mount_point() failed");
  }

  // TODO(Wine93): check filesystem type
  *mountpoint = std::string(mp, n);
  return Status::OK();
}

Status iorcreate(ior* ior, const std::string& mountpoint, uint32_t iodepth,
                 bool for_read) {
  int rc =
      hf3fs_iorcreate4(ior, mountpoint.c_str(), iodepth, for_read, 0, 0, -1, 0);
  if (rc != 0) {
    LOG(ERROR) << Errorf(-rc, "hf3fs_iorcreate4(%s,%s)", mountpoint,
                         (for_read ? "read" : "write"));
    return Status::Internal("hf3fs_iorcreate4() failed");
  }
  return Status::OK();
}

void iovdestory(ior* ior) { hf3fs_iordestroy(ior); }

Status iovcreate(iov* iov, const std::string& mountpoint, size_t total_size) {
  auto rc = hf3fs_iovcreate(iov, mountpoint.c_str(), total_size, 0, -1);
  if (rc != 0) {
    LOG(ERROR) << Errorf(-rc, "hf3fs_iovcreate(%s,%d)", mountpoint, total_size);
    return Status::Internal("hf3fs_iovcreate() failed");
  }
  return Status::OK();
}

void iovdestory(iov* iov) { hf3fs_iovdestroy(iov); }

Status reg_fd(int fd) {
  int rc = hf3fs_reg_fd(fd, 0);
  if (rc > 0) {
    LOG(ERROR) << Errorf(rc, "hf3fs_reg_fd(fd=%d)", fd);
    return Status::Internal("hf3fs_reg_fd() failed");
  }
  return Status::OK();
}

void dereg_fd(int fd) { hf3fs_dereg_fd(fd); }

Status prep_io(ior* ior, iov* iov, bool for_read, void* buffer, int fd,
               off_t offset, size_t length, void* userdata) {
  int rc =
      hf3fs_prep_io(ior, iov, for_read, buffer, fd, offset, length, userdata);
  if (rc < 0) {
    LOG(ERROR) << Errorf(-rc, "hf3fs_prep_io(fd=%d,offset=%d,length=%d)", fd,
                         offset, length);
    return Status::Internal("hf3fs_prep_io() failed");
  }
  return Status::OK();
}

Status submit_ios(ior* ior) {
  int rc = hf3fs_submit_ios(ior);
  if (rc != 0) {
    LOG(ERROR) << Errorf(-rc, "hf3fs_submit_ios()");
    return Status::Internal("hf3fs_submit_ios() failed");
  }
  return Status::OK();
}

int wait_for_ios(ior* ior, cqe* cqes, uint32_t io_depth, uint32_t timeout_ms) {
  timespec ts;
  ts.tv_sec = timeout_ms / 1000;
  ts.tv_nsec = (timeout_ms % 1000) * 1000000L;

  int n = hf3fs_wait_for_ios(ior, cqes, io_depth, 1, &ts);
  if (n < 0) {
    LOG(ERROR) << Errorf(-n, "hf3fs_wait_for_ios(%d,%d,%d)", io_depth, 1,
                         timeout_ms);
  }
  return n;
}

}  // namespace hf3fs

}  // namespace storage
}  // namespace cache
}  // namespace dingofs
