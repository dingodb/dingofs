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

#ifndef DINGOFS_SRC_CLIENT_VFS_COMMON_READ_BUF_VIEW_H_
#define DINGOFS_SRC_CLIENT_VFS_COMMON_READ_BUF_VIEW_H_

#include <cstddef>
#include <cstdint>

namespace dingofs {
namespace client {
namespace vfs {

// Non-owning writable window into a caller-allocated (read mempool) buffer.
// The read lower layers fill `base + offset` for `len` bytes instead of
// allocating + bubbling up. RDMA-friendly: addr = base + offset, len, and (with
// a single arena MR) a global rkey -- no per-buffer registration needed.
struct ReadBufView {
  uint8_t* base{nullptr};
  size_t offset{0};
  size_t len{0};

  uint8_t* data() const { return base + offset; }
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_COMMON_READ_BUF_VIEW_H_
