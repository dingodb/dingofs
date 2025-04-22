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

/*
 * Project: DingoFS
 * Created Date: 2025-05-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_OPTIONS_CLIENT_S3_H_
#define DINGOFS_SRC_OPTIONS_CLIENT_S3_H_

#include "options/options.h"

namespace dingofs {
namespace options {
namespace client {

class S3GlobalOption : public BaseOption {
  BIND_int32(log_level, 4, "");
  BIND_string(log_prefix, "/tmp/", "");
  BIND_bool(telemetry_enable, false, "");
};

class S3BucketOption : public BaseOption {
  BIND_string(bucket_name, "", "");
  BIND_string(ak, "", "");
  BIND_string(sk, "", "");
  BIND_string(endpoint, "", "");
  BIND_int32(block_size, 4194304, "");
  BIND_int32(chunk_size, 67108864, "");
  BIND_string(object_prefix, "", "");
};

class S3RequestOption : public BaseOption {
  BIND_string(region, "us-east-1", "");
  BIND_bool(use_virtual_addressing, false, "");
  BIND_bool(verify_ssl, false, "");
  BIND_int32(max_connections, 32, "");
  BIND_int32(connect_timeout_ms, 60000, "");
  BIND_int32(request_timeout_ms, 10000, "");
  BIND_bool(use_crt_client, false, "");
  BIND_bool(use_thread_pool, true, "only work when use_crt_client is false");
  BIND_int32(async_thread_num, true, "only work when use_crt_client is false");
  BIND_uint64(max_async_request_inflight_bytes, 0, "");
};

class S3ThrottleOption : public BaseOption {
  BIND_uint64(iops_total_limit, 0, "");
  BIND_uint64(iops_read_limit, 0, "");
  BIND_uint64(iops_write_limit, 0, "");
  BIND_uint64(bps_total_mb, 0, "");
  BIND_uint64(bps_read_mb, 0, "");
  BIND_uint64(bps_write_mb, 0, "");
};

class S3Option : public BaseOption {
  BIND_suboption(global_option, "global", S3GlobalOption);
  BIND_suboption(bucket_option, "bucket", S3BucketOption);
  BIND_suboption(request_option, "request", S3RequestOption);
  BIND_suboption(throttle_option, "throttle", S3ThrottleOption);
};

// do nothing

}  // namespace client
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CLIENT_S3_H_
