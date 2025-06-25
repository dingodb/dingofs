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

#include "client/vfs/data/common.h"

#include <fmt/format.h>

namespace dingofs {
namespace client {
namespace vfs {

std::string FileRange::ToString() const {
  return fmt::format("[{}-{}]", offset, End());
}

std::string SliceReadReq::ToString() const {
  return fmt::format("(read_range: [{}-{}], len: {}, slice: {})", file_offset,
                     End(), len,
                     slice.has_value() ? Slice2Str(slice.value()) : "null");
}

std::string BlockDesc::ToString() const {
  return fmt::format(
      "(range:[{}-{}], len: {}, zero: {}, version: {}, slice_id: {}, "
      "block_index: {})",
      file_offset, End(), block_len, zero, version, slice_id, index);
}

std::string BlockReadReq::ToString() const {
  return fmt::format("(block_req_range: [{}-{}], len: {}, block: {})",
                     block_offset, End(), len, block.ToString());
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs