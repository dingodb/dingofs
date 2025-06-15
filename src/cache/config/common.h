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
 * Created Date: 2025-06-02
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CONFIG_COMMON_H_
#define DINGOFS_SRC_CACHE_CONFIG_COMMON_H_

#include <gflags/gflags_declare.h>

#include "stub/common/config.h"

namespace dingofs {
namespace cache {

DECLARE_string(logdir);
DECLARE_int32(loglevel);

DECLARE_string(mds_rpc_addrs);
DECLARE_uint64(mds_rpc_retry_total_ms);
DECLARE_uint64(mds_rpc_max_timeout_ms);
DECLARE_uint64(mds_rpc_timeout_ms);
DECLARE_uint64(mds_rpc_retry_interval_us);
DECLARE_uint64(mds_rpc_max_failed_times_before_change_addr);
DECLARE_uint64(mds_rpc_normal_retry_times_before_trigger_wait);
DECLARE_uint64(mds_rpc_wait_sleep_ms);

stub::common::MdsOption NewMdsOption();

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CONFIG_COMMON_H_
