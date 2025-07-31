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
 * Created Date: 2025-08-03
 * Author: Jingli Chen (Wine93)
 */

#include "mds/cachegroup/config.h"

#include <brpc/reloadable_flags.h>

namespace dingofs {
namespace mds {
namespace cachegroup {

DEFINE_uint32(heartbeat_interval_s, 3,
              "Interval for cache group member heartbeat in seconds");
DEFINE_validator(heartbeat_interval_s, brpc::PassValidate);

DEFINE_uint32(heartbeat_miss_timeout_s, 10,
              "Timeout for missing heartbeat in seconds");
DEFINE_validator(heartbeat_miss_timeout_s, brpc::PassValidate);

DEFINE_uint32(heartbeat_offline_timeout_s, 30,
              "Timeout for member to be considered offline in seconds");
DEFINE_validator(heartbeat_offline_timeout_s, brpc::PassValidate);

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
