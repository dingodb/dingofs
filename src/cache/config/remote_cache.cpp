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

#include "cache/config/remote_cache.h"

#include <gflags/gflags.h>

#include <vector>

#include "cache/config/common.h"

namespace dingofs {
namespace cache {

DEFINE_string(group_name, "", "");
DEFINE_uint32(load_members_interval_ms, 1000, "");

DEFINE_uint32(remote_put_rpc_timeout_ms, 3000, "");
DEFINE_uint32(remote_range_timeout_ms, 3000, "");
DEFINE_uint32(remote_cache_rpc_timeout_ms, 3000, "");
DEFINE_uint32(remote_prefetch_rpc_timeout_ms, 3000, "");

RemoteAccessOption::RemoteAccessOption()
    : remote_put_rpc_timeout_ms(FLAGS_remote_put_rpc_timeout_ms),
      remote_range_rpc_timeout_ms(FLAGS_remote_range_rpc_timeout_ms),
      remote_cache_rpc_timeout_ms(FLAGS_remote_cache_rpc_timeout_ms),
      remote_prefetch_rpc_timeout_ms(FLAGS_remote_prefetch_rpc_timeout_ms) {}

RemoteBlockCacheOption::RemoteBlockCacheOption()
    : group_name(FLAGS_group_name),
      load_members_interval_ms(FLAGS_load_members_interval_ms),
      mds_addrs(std::vector<std::string>{FLAGS_mds_addrs}) {}

}  // namespace cache
}  // namespace dingofs
