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

namespace dingofs {
namespace client {
namespace vfs {

struct PageData {
  const uint64_t index{0};
  const uint64_t page_size{0};  // size of the page, used for validation
  uint64_t data_offset{0};      // validate data offset in page
  uint64_t data_len{0};
  char* page{nullptr};  // not owned, mem managed by page allocator

  explicit PageData(uint64_t p_index, uint64_t p_page_size, char* p_page,
                    uint64_t p_data_offset)
      : index(p_index),
        page_size(p_page_size),
        page(p_page),
        data_offset(p_data_offset) {}

  ~PageData() = default;

  void Write(const char* buf, uint64_t size, uint64_t page_offset);

  uint64_t DataEnd() const { return data_offset + data_len; }

  std::string ToString() const ;
};

using PageDataUPtr = std::unique_ptr<PageData>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_SLICE_PAGE_DATA_H_