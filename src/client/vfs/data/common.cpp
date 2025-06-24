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

#include <sstream>

namespace dingofs {
namespace client {
namespace vfs {

std::string FileRange::ToString() const {
  std::ostringstream os;
  os << "[" << offset << "-" << End() << "]";
  return os.str();
}

std::string SliceReadReq::ToString() const {
  std::ostringstream oss;
  oss << "{ read_range: [" << file_offset << "-" << End() << "]"
      << ", len: " << len
      << ", slice: " << (slice.has_value() ? Slice2Str(slice.value()) : "null")
      << " }";
  return oss.str();
}

std::string BlockDesc::ToString() const {
  std::ostringstream os;
  os << "{ range:[" << file_offset << "-" << End() << "]"
     << ", len: " << block_len << ", zero: " << zero << ", version: " << version
     << ", slice_id: " << slice_id << ", block_index: " << index << " }";
  return os.str();
}

std::string BlockReadReq::ToString() const {
  std::ostringstream oss;
  oss << "{ block_req_range: [" << block_offset << "-" << End()
      << "], len: " << len << ", block: " << block.ToString() << " }";
  return oss.str();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs