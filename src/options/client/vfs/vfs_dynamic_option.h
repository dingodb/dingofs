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

#ifndef DINGOFS_SRC_CLIENT_OPTIONS_VFS_DYNAMIC_OPTION_H_
#define DINGOFS_SRC_CLIENT_OPTIONS_VFS_DYNAMIC_OPTION_H_

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

namespace dingofs {
namespace client {

// meta system log
DECLARE_bool(vfs_meta_logging);

DECLARE_int32(flush_bg_thread);

// begin used in inode_blocks_service
DECLARE_uint32(format_file_offset_width);
DECLARE_uint32(format_len_width);
DECLARE_uint32(format_block_offset_width);
DECLARE_uint32(format_block_name_width);
DECLARE_uint32(format_block_len_width);
DECLARE_string(format_delimiter);
// end used in inode_blocks_service

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_OPTIONS_FUSE_OPTION_H_