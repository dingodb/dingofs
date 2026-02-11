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

#ifndef DINGOFS_CLIENT_COMMON_CONST_H_
#define DINGOFS_CLIENT_COMMON_CONST_H_

#include <climits>
#include <cstdint>
#include <string>

namespace dingofs {
namespace client {

// module name
static const std::string kVFSMoudule = "vfs";
static const std::string kVFSWrapperMoudule = "vfs_wrapper";
static const std::string kVFSDataMoudule = "vfs_data";

// ioctl related constants
static const uint8_t kFlagImmutable = (1 << 0);
static const uint8_t kFlagAppend = (1 << 1);
static const uint8_t kFlagNoDump = (1 << 2);
static const uint8_t kFlagNoAtime = (1 << 3);
static const uint8_t kFlagSync = (1 << 4);

// reserve 24 for libfuse kernel
static constexpr size_t kFuseReserveIovSize = 24;
static constexpr size_t kFuseMaxIovSize = IOV_MAX - kFuseReserveIovSize;

// used in inode_blocks_service
static constexpr size_t kFormatFileOffsetWidth = 20;
static constexpr size_t kFormatLenWidth = 15;
static constexpr size_t kFormatBlockOffsetWidth = 15;
static constexpr size_t kFormatBlockNameWidth = 100;
static constexpr size_t kFormatBlockLenWidth = 15;
static const std::string kFormatDelimiter = "|";

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_COMMON _CONST_H_
