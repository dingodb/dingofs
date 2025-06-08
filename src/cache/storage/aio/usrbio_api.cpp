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

#ifdef WITH_LIBUSRBIO

#include "cache/storage/aio/usrbio_api.h"

#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {

Status USRBIOApi::ExtractMountPoint(const std::string& path,
                                    std::string* mountpoint) {
  char mp[4096];
  int n = hf3fs_extract_mount_point(mp, sizeof(mp), path.c_str());
  if (n <= 0) {
    LOG(ERROR) << "hf3fs_extract_mount_point(" << path
               << ") failed: rc = " << n;
    return Status::Internal("hf3fs_extract_mount_point() failed");
  }

  *mountpoint = std::string(mp, n);
  return Status::OK();
}

Status USRBIOApi::IORCreate(IOR* ior, const std::string& mountpoint,
                            uint32_t iodepth, bool for_read) {
  int rc =
      hf3fs_iorcreate4(ior, mountpoint.c_str(), iodepth, for_read, 0, 0, -1, 0);
  if (rc != 0) {
    LOG(ERROR) << Helper::Errorf(-rc, "hf3fs_iorcreate4(%s,%d,%s)", mountpoint,
                                 iodepth, for_read);
    return Status::Internal("hf3fs_iorcreate4() failed");
  }
  return Status::OK();
}

void USRBIOApi::IORDestroy(IOR* ior) { hf3fs_iordestroy(ior); }

Status USRBIOApi::IOVCreate(IOV* iov, const std::string& mountpoint,
                            size_t buffer_size) {
  auto rc = hf3fs_iovcreate(iov, mountpoint.c_str(), buffer_size, 0, -1);
  if (rc != 0) {
    LOG(ERROR) << Helper::Errorf(-rc, "hf3fs_iovcreate(%s,%d)", mountpoint,
                                 buffer_size);
    return Status::Internal("hf3fs_iovcreate() failed");
  }
  return Status::OK();
}

void USRBIOApi::IOVDestroy(IOV* iov) { hf3fs_iovdestroy(iov); }

Status USRBIOApi::RegFd(int fd) {
  int rc = hf3fs_reg_fd(fd, 0);
  if (rc > 0) {
    LOG(ERROR) << Helper::Errorf(rc, "hf3fs_reg_fd(fd=%d)", fd);
    return Status::Internal("hf3fs_reg_fd() failed");
  }
  return Status::OK();
}

void USRBIOApi::DeRegFd(int fd) { hf3fs_dereg_fd(fd); }

Status USRBIOApi::PrepIO(IOR* ior, IOV* iov, bool for_read, void* buffer,
                         int fd, off_t offset, size_t length, void* userdata) {
  int rc =
      hf3fs_prep_io(ior, iov, for_read, buffer, fd, offset, length, userdata);
  if (rc < 0) {
    LOG(ERROR) << Helper::Errorf(-rc, "hf3fs_prep_io(%s,%d,%d,%d)", for_read,
                                 fd, offset, length);
    return Status::Internal("hf3fs_prep_io() failed");
  }
  return Status::OK();
}

Status USRBIOApi::SubmitIOs(IOR* ior) {
  int rc = hf3fs_submit_ios(ior);
  if (rc != 0) {
    LOG(ERROR) << Helper::Errorf(-rc, "hf3fs_submit_ios()");
    return Status::Internal("hf3fs_submit_ios() failed");
  }
  return Status::OK();
}

int USRBIOApi::WaitForIOs(IOR* ior, Cqe* cqes, uint32_t iodepth,
                          uint64_t timeout_ms) {
  auto ts = butil::milliseconds_to_timespec(timeout_ms);
  int n = hf3fs_wait_for_ios(ior, cqes, iodepth, 1, &ts);
  if (n < 0) {
    LOG(ERROR) << Helper::Errorf(-n, "hf3fs_wait_for_ios(%d,%d,%lld)", iodepth,
                                 1, timeout_ms);
  }
  return n;
}

}  // namespace cache
}  // namespace dingofs

#endif  // WITH_LIBUSRBIO
