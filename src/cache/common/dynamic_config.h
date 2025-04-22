/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_DYNAMIC_CONFIG_H_
#define DINGOFS_SRC_CACHE_COMMON_DYNAMIC_CONFIG_H_

#include <gflags/gflags.h>

namespace dingofs {
namespace cache {
namespace common {

#define USING_CACHE_FLAG(name) using ::dingofs::cache::common::FLAGS_##name;

/**
 * You can modify the config on the fly, e.g.
 *
 * curl -s http://127.0.0.1:9301/flags/trace_logging?setvalue=true
 */

// trace logging
DECLARE_bool(trace_logging);

// block cache
DECLARE_bool(block_cache_stage_bandwidth_throttle_enable);
DECLARE_uint64(block_cache_stage_bandwidth_throttle_mb);

// disk cache
DECLARE_bool(drop_page_cache);

// disk cache manager
DECLARE_uint64(disk_cache_expire_second);
DECLARE_uint64(disk_cache_cleanup_expire_interval_millsecond);
DECLARE_double(disk_cache_free_space_ratio);

// disk state machine
DECLARE_int32(disk_state_tick_duration_second);
DECLARE_int32(disk_state_normal2unstable_io_error_num);
DECLARE_int32(disk_state_unstable2normal_io_succ_num);
DECLARE_int32(disk_state_unstable2down_second);

// disk state health checker
DECLARE_int32(disk_check_duration_millsecond);

}  // namespace common
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_DYNAMIC_CONFIG_H_
