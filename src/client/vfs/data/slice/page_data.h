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

#ifndef DINGOFS_CLIENT_VFS_DATA_SLICE_PAGE_DATA_H_
#define DINGOFS_CLIENT_VFS_DATA_SLICE_PAGE_DATA_H_

#include <cstdint>
#include <memory>

#include "client/vfs/hub/vfs_hub.h"

namespace dingofs {
namespace client {
namespace vfs {

struct PageData {
  VFSHub* vfs_hub_{nullptr};
  const uint32_t index{0};
  const int32_t page_size{0};  // size of the page, used for validation
  int32_t data_offset{0};      // validate data offset in page
  int32_t data_len{0};
  char* page{nullptr};  // not owned, mem managed by page allocator

  explicit PageData(VFSHub* vfs_hub, uint32_t p_index, int32_t p_page_size,
                    char* p_page, int32_t p_data_offset)
      : vfs_hub_(vfs_hub),
        index(p_index),
        page_size(p_page_size),
        page(p_page),
        data_offset(p_data_offset) {}

  ~PageData() = default;

  void Write(ContextSPtr ctx, const char* buf, int32_t size,
             int32_t page_offset);

  int32_t DataEnd() const { return data_offset + data_len; }

  std::string ToString() const;
};

using PageDataUPtr = std::unique_ptr<PageData>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_SLICE_PAGE_DATA_H_