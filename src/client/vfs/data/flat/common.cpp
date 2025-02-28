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

#include "client/vfs/data/flat/common.h"

#include <sstream>

namespace dingofs {
namespace client {
namespace vfs {


std::string SliceRange::ToString() const {
  std::ostringstream oss;
  oss << "{file_offset:" << file_offset << ", len:" << len
      << ", slice_id:" << slice_id << "}";
  return oss.str();
}


std::string FileSlice::ToString() const {
  std::ostringstream oss;
  oss << "{file_offset: " << file_offset << ", len: " << len
      << ", block_offset: " << block_offset
      << ", block_key: " << block_key.Filename() << ", block_len: " << block_len
      << ", zero: " << zero << "}";
  return oss.str();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs