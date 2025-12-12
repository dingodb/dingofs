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

#include "common/options/blockaccess.h"

#include <gflags/gflags.h>

namespace dingofs {
namespace blockaccess {

// rados option
DEFINE_int32(rados_op_timeout, 120, "rados operation timeout in seconds");

// aws option
DEFINE_string(s3_region, "us-east-1", "aws s3 region");
DEFINE_int32(s3_loglevel, 4, "aws sdk log level");
DEFINE_bool(s3_verify_ssl, false, "whether to verify ssl");
DEFINE_int32(s3_max_connections, 32, "max connections to s3");
DEFINE_int32(s3_connect_timeout, 60000, "S3 connect timeout in milliseconds");
DEFINE_int32(s3_request_timeout, 10000, "S3 request timeout in milliseconds");
DEFINE_bool(s3_use_crt_client, true, "whether to use crt client");
DEFINE_bool(s3_use_thread_pool, true, "whether to use thread pool");
DEFINE_int32(s3_async_thread_num, 16, "async thread num in thread pool");
DEFINE_bool(s3_use_virtual_address, false, "whether to use virtual address");
DEFINE_bool(s3_enable_telemetry, false, "whether to enable telemetry");

// throttle options
DEFINE_uint32(iops_total_limit, 0, "total iops limit(0 means no limit)");
DEFINE_uint32(iops_read_limit, 0, "read iops limit(0 means no limit)");
DEFINE_uint32(iops_write_limit, 0, "write iops limit(0 means no limit)");
DEFINE_uint32(io_bandwidth_total_mb, 0,
              "total io bandwidth limit in MB(0 means no limit)");
DEFINE_uint32(io_bandwidth_read_mb, 0,
              "read io bandwidth limit in MB(0 means no limit)");
DEFINE_uint32(io_bandwidth_write_mb, 0,
              "write io bandwidth limit in MB(0 means no limit)");
DEFINE_uint32(io_max_inflight_async_bytes, 0,
              "max inflight async bytes(0 means no limit)");

}  // namespace blockaccess
}  // namespace dingofs