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

#ifndef DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_API_H_
#define DINGOFS_SRC_CACHE_STORAGE_AIO_USRBIO_API_H_

#include <bthread/mutex.h>
#include <butil/iobuf.h>
#include <hf3fs_usrbio.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "cache/common/common.h"
#include "cache/common/types.h"

namespace dingofs {
namespace cache {
namespace storage {

// wrapper for usrbio api
namespace hf3fs {

using ior = struct hf3fs_ior;
using iov = struct hf3fs_iov;
using cqe = struct hf3fs_cqe;

Status extract_mount_point(const std::string& path, std::string* mountpoint);

Status iorcreate(ior* ior, const std::string& mountpoint, uint32_t iodepth,
                 bool for_read);

Status iordestroy(ior* ior);

Status iovcreate(iov* iov, const std::string& mountpoint, size_t total_size);

void iovdestory(iov* iov);

Status reg_fd(int fd);

void dereg_fd(int fd);

Status prep_io(ior* ior, iov* iov, bool for_read, void* buffer, int fd,
               off_t offset, size_t length, void* userdata);

Status submit_ios(ior* ior);

int wait_for_ios(ior* ior, cqe* cqes, uint32_t io_depth, uint32_t timeout_ms);

};  // namespace hf3fs

}  // namespace storage
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_AIO_USRBIO_API_H_
