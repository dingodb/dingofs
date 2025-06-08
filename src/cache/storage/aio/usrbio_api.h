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

#ifndef DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_API_H_
#define DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_API_H_

#include <hf3fs_usrbio.h>

#include "cache/common/common.h"

namespace dingofs {
namespace cache {

#ifdef WITH_LIBUSRBIO

// wrapper for usrbio api
class USRBIOApi {
 public:
  using IOR = struct hf3fs_ior;
  using IOV = struct hf3fs_iov;
  using Cqe = struct hf3fs_cqe;

  static Status ExtractMountPoint(const std::string& path,
                                  std::string* mountpoint);

  static Status IORCreate(IOR* ior, const std::string& mountpoint,
                          uint32_t iodepth, bool for_read);
  static void IORDestroy(IOR* ior);

  static Status IOVCreate(IOV* iov, const std::string& mountpoint,
                          size_t buffer_size);
  static void IOVDestroy(IOV* iov);

  static Status RegFd(int fd);
  static void DeRegFd(int fd);

  static Status PrepIO(IOR* ior, IOV* iov, bool for_read, void* buffer, int fd,
                       off_t offset, size_t length, void* userdata);
  static Status SubmitIOs(IOR* ior);
  static int WaitForIOs(IOR* ior, Cqe* cqes, uint32_t iodepth,
                        uint64_t timeout_ms);
};

#endif  // WITH_LIBUSRBIO

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_AIO_USRBIO_API_H_

#endif  // WITH_LIBUSRBIO
