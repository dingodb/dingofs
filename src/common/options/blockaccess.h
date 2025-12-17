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

#ifndef DINGOFS_COMMON_OPTIONS_BLOCK_ACCESS_OPTION_H_
#define DINGOFS_COMMON_OPTIONS_BLOCK_ACCESS_OPTION_H_

#include <gflags/gflags_declare.h>

namespace dingofs {
namespace blockaccess {

DECLARE_int32(rados_op_timeout);

// aws option
DECLARE_string(s3_region);
DECLARE_int32(s3_loglevel);
DECLARE_bool(s3_verify_ssl);
DECLARE_int32(s3_max_connections);
DECLARE_int32(s3_connect_timeout);
DECLARE_int32(s3_request_timeout);
DECLARE_bool(s3_use_crt_client);
DECLARE_bool(s3_use_thread_pool);
DECLARE_int32(s3_async_thread_num);
DECLARE_bool(s3_use_thread_pool);
DECLARE_bool(s3_use_virtual_address);
DECLARE_bool(s3_enable_telemetry);

DECLARE_bool(use_fake_block_access);

// throttle options
DECLARE_uint32(iops_total_limit);
DECLARE_uint32(iops_read_limit);
DECLARE_uint32(iops_write_limit);
DECLARE_uint32(io_bandwidth_total_mb);
DECLARE_uint32(io_bandwidth_read_mb);
DECLARE_uint32(io_bandwidth_write_mb);
DECLARE_uint32(io_max_inflight_async_bytes);

}  // namespace blockaccess
}  // namespace dingofs
#endif  // DINGOFS_COMMON_OPTIONS_BLOCK_ACCESS_OPTION_H_