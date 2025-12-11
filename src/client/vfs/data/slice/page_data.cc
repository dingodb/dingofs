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

#include "client/vfs/data/slice/page_data.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstdint>

#include "client/common/const.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("PageData::" + std::string(__FUNCTION__))

std::string PageData::ToString() const {
  return fmt::format(
      "(index: {}, page_size: {}, page_range: [{}-{}], data_len: {}, page: "
      "{})",
      index, page_size, data_offset, DataEnd(), data_len,
      Helper::Char2Addr(page));
}

void PageData::Write(ContextSPtr ctx, const char* buf, uint64_t size,
                     uint64_t page_offset) {
  auto* tracer = vfs_hub_->GetTracer();
  auto span = tracer->StartSpanWithContext(kVFSDataMoudule, METHOD_NAME(), ctx);

  uint64_t write_page_end = page_offset + size;
  VLOG(8) << fmt::format("{} Write Start page_range: [{}-{}], size: {}",
                         ToString(), page_offset, write_page_end, size);

  CHECK_LE(page_offset + size, page_size);
  CHECK(page_offset == DataEnd() || write_page_end == data_offset)
      << fmt::format("{} Illegal write page_range: [{}-{}], size: {}",
                     ToString(), page_offset, write_page_end, size);

  CHECK_NOTNULL(page);

  memcpy(page + page_offset, buf, size);

  uint64_t old_data_offset = data_offset;
  uint64_t old_data_len = data_len;

  data_offset = std::min(data_offset, page_offset);
  data_len += size;

  VLOG(8) << fmt::format(
      "{} Write End page_range old_data_offset: {}, old_data_len: {}",
      ToString(), old_data_offset, old_data_len);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
