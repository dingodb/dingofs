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

#include "options/client/fuse/fuse_dynamic_option.h"

namespace dingofs {
namespace client {

namespace {
bool PassDouble(const char*, double) { return true; }
bool PassUint64(const char*, uint64_t) { return true; }
bool PassInt32(const char*, int32_t) { return true; }
bool PassUint32(const char*, uint32_t) { return true; }
bool PassBool(const char*, bool) { return true; }
};  // namespace

// fuse module
DEFINE_bool(fuse_file_info_direct_io, false, "use direct io for file");
DEFINE_validator(fuse_file_info_direct_io, &PassBool);

DEFINE_bool(fuse_file_info_keep_cache, false, "keep file page cache");
DEFINE_validator(fuse_file_info_keep_cache, &PassBool);

// smooth upgrade
DEFINE_uint32(fuse_fd_get_max_retries, 100,
              "the max retries that get fuse fd from old dingo-fuse during "
              "smooth upgrade");
DEFINE_validator(fuse_fd_get_max_retries, &PassUint32);
DEFINE_uint32(
    fuse_fd_get_retry_interval_ms, 100,
    "the interval in millseconds that get fuse fd from old dingo-fuse "
    "during smooth upgrade");
DEFINE_validator(fuse_fd_get_retry_interval_ms, &PassUint32);
DEFINE_uint32(
    fuse_check_alive_max_retries, 600,
    "the max retries that check old dingo-fuse is alive during smooth upgrade");
DEFINE_validator(fuse_check_alive_max_retries, &PassUint32);
DEFINE_uint32(fuse_check_alive_retry_interval_ms, 1000,
              "the interval in millseconds that check old dingo-fuse is alive "
              "during smooth upgrade");
DEFINE_validator(fuse_check_alive_retry_interval_ms, &PassUint32);

}  // namespace client
}  // namespace dingofs
