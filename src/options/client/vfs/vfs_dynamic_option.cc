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

#include "options/client/vfs/vfs_dynamic_option.h"

namespace dingofs {
namespace client {

namespace {
bool PassDouble(const char*, double) { return true; }
bool PassUint64(const char*, uint64_t) { return true; }
bool PassInt32(const char*, int32_t) { return true; }
bool PassUint32(const char*, uint32_t) { return true; }
bool PassBool(const char*, bool) { return true; }

bool ValidateDelimiterLength(const char* flag_name, const std::string& value) {
  (void)flag_name;
  if (value.length() != 1) {
    return false;
  }
  return true;
}
};  // namespace

// access log
DEFINE_bool(meta_logging, true, "enable meta system log");
DEFINE_validator(meta_logging, &PassBool);

DEFINE_int32(flush_bg_thread, 16, "Number of background flush threads");
DEFINE_validator(flush_bg_thread, &PassInt32);

// begin used in inode_blocks_service
DEFINE_uint32(format_file_offset_width, 20, "Width of file offset in format");
DEFINE_validator(format_file_offset_width, &PassUint32);

DEFINE_uint32(format_len_width, 15, "Width of length in format");
DEFINE_validator(format_len_width, &PassUint32);

DEFINE_uint32(format_block_offset_width, 15, "Width of block offset in format");
DEFINE_validator(format_block_offset_width, &PassUint32);

DEFINE_uint32(format_block_name_width, 100, "Width of block name in format");
DEFINE_validator(format_block_name_width, &PassUint32);

DEFINE_uint32(format_block_len_width, 15, "Width of block length in format");
DEFINE_validator(format_block_len_width, &PassUint32);

DEFINE_string(format_delimiter, "|", "Delimiter used in format");
DEFINE_validator(format_delimiter, &ValidateDelimiterLength);
// end used in inode_blocks_service

}  // namespace client
}  // namespace dingofs
