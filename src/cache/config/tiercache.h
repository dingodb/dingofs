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
 * Created Date: 2025-06-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CONFIG_TIERCACHE_H_
#define DINGOFS_SRC_CACHE_CONFIG_TIERCACHE_H_

#include <gflags/gflags_declare.h>

#include "stub/common/config.h"

namespace dingofs {
namespace cache {

// Sets the cache group name to use, e.g. "default", "group1" and etc.
DECLARE_string(cache_group);

// Sets the interval to load cache group members in milliseconds.
DECLARE_uint32(load_members_interval_ms);

// Sets whether the data blocks uploaded to the storage are
// simultaneously sent to the cache group node.
DECLARE_bool(fill_group_cache);

// Sets the rpc timeout for remote put operation in milliseconds.
DECLARE_uint32(put_rpc_timeout_ms);

// Sets the rpc timeout for remote range operation in milliseconds.
DECLARE_uint32(range_rpc_timeout_ms);

// Sets the rpc timeout for remote cache operation in milliseconds.
DECLARE_uint32(cache_rpc_timeout_ms);

// Sets the rpc timeout for remote prefetch operation in milliseconds.
DECLARE_uint32(prefetch_rpc_timeout_ms);

// Sets the maximum number of retry times for remote cache rpc operations.
DECLARE_uint32(cache_rpc_max_retry_times);

struct RemoteBlockCacheOption {
  RemoteBlockCacheOption();

  std::string cache_group;
  uint32_t load_members_interval_ms;
  stub::common::MdsOption mds_option;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CONFIG_TIERCACHE_H_
