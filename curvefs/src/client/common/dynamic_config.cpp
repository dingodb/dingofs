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

#include "curvefs/src/client/common/dynamic_config.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace curvefs {
namespace client {
namespace common {

namespace {
bool PassDouble(const char*, double) { return true; }
bool PassUint64(const char*, uint64_t) { return true; }
bool PassInt32(const char*, int32_t) { return true; }
bool PassBool(const char*, bool) { return true; }
};  // namespace

// block cache logging
DEFINE_bool(block_cache_logging, true, "enable block cache log");

DEFINE_validator(block_cache_logging, &PassBool);

// disk cache manager
DEFINE_uint64(disk_cache_expire_second, 0,
              "cache expire time, 0 means never expired");
DEFINE_double(disk_cache_free_space_ratio, 0.1, "disk free space ratio");

DEFINE_validator(disk_cache_expire_second, &PassUint64);
DEFINE_validator(disk_cache_free_space_ratio, &PassDouble);

// disk state machine
DEFINE_int32(disk_state_tick_duration_second, 60,
             "tick duration in seconds for disk state machine");
DEFINE_int32(disk_state_normal2unstable_io_error_num, 3,
             "io error number to transit from normal to unstable");
DEFINE_int32(disk_state_unstable2normal_io_succ_num, 10,
             "io success number to transit from unstable to normal");
DEFINE_int32(disk_state_unstable2down_second, 30 * 60,
             "second to transit from unstable to down");

DEFINE_validator(disk_state_tick_duration_second, &PassInt32);
DEFINE_validator(disk_state_normal2unstable_io_error_num, &PassInt32);
DEFINE_validator(disk_state_unstable2normal_io_succ_num, &PassInt32);
DEFINE_validator(disk_state_unstable2down_second, &PassInt32);

// disk state health checker
DEFINE_int32(disk_check_duration_millsecond, 1 * 1000,
             "disk health check duration in millsecond");

DEFINE_validator(disk_check_duration_millsecond, &PassInt32);

}  // namespace common
}  // namespace client
}  // namespace curvefs
