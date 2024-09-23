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

#ifndef CURVEFS_SRC_CLIENT_COMMON_DYNAMIC_CONFIG_H_
#define CURVEFS_SRC_CLIENT_COMMON_DYNAMIC_CONFIG_H_

#include <gflags/gflags.h>

namespace curvefs {
namespace client {
namespace common {

#define USING_FLAG(name) using ::curvefs::client::common::FLAGS_##name;

/**
 * You can modify the config on the fly, e.g.
 *
 * curl -s http://127.0.0.1:9000/flags/block_cache_logging?setvalue=true
 */

// block cache logging
DECLARE_bool(block_cache_logging);

// disk cache
DECLARE_bool(drop_page_cache);

// disk cache manager
DECLARE_uint64(disk_cache_expire_second);
DECLARE_double(disk_cache_free_space_ratio);

// disk state machine
DECLARE_int32(disk_state_tick_duration_second);
DECLARE_int32(disk_state_normal2unstable_io_error_num);
DECLARE_int32(disk_state_unstable2normal_io_succ_num);
DECLARE_int32(disk_state_unstable2down_second);

// disk state health checker
DECLARE_int32(disk_check_duration_millsecond);

}  // namespace common
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_COMMON_DYNAMIC_CONFIG_H_
