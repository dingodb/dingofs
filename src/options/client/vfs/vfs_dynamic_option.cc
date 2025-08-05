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

#include <gflags/gflags.h>

#include "options/gflag_validator.h"

namespace dingofs {
namespace client {

namespace {
bool ValidateDelimiterLength(const char* flag_name, const std::string& value) {
  (void)flag_name;
  if (value.length() != 1) {
    return false;
  }
  return true;
}
};  // namespace

// vfs meta access log
DEFINE_bool(vfs_meta_logging, true, "enable vfs meta system log");
DEFINE_validator(vfs_meta_logging, &PassBool);

DEFINE_int64(vfs_meta_log_threshold_us, 1000, "access log threshold");
DEFINE_validator(vfs_meta_log_threshold_us, &PassInt64);

DEFINE_int32(vfs_flush_bg_thread, 16, "Number of background flush threads");
DEFINE_validator(vfs_flush_bg_thread, &PassInt32);

DEFINE_int32(vfs_read_executor_thread, 8, "Number of read executor threads");
DEFINE_validator(vfs_read_executor_thread, &PassInt32);

DEFINE_uint32(vfs_periodic_flush_interval_ms, 10 * 1000,
              "Periodic flush interval in milliseconds");
DEFINE_validator(vfs_periodic_flush_interval_ms, &PassUint32);

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
