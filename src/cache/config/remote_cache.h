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
 * Created Date: 2025-05-09
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CONFIG_REMOTE_CACHE_H_
#define DINGOFS_SRC_CACHE_CONFIG_REMOTE_CACHE_H_

#include <gflags/gflags.h>

#include "cache/config/common.h"

namespace dingofs {
namespace cache {

DECLARE_string(cache_group);
DECLARE_uint32(load_members_interval_ms);

DECLARE_uint32(remote_put_rpc_timeout_ms);
DECLARE_uint32(remote_range_rpc_timeout_ms);
DECLARE_uint32(remote_cache_rpc_timeout_ms);
DECLARE_uint32(remote_prefetch_rpc_timeout_ms);

struct RemoteNodeOption {
  RemoteNodeOption();

  uint32_t remote_put_rpc_timeout_ms;
  uint32_t remote_range_rpc_timeout_ms;
  uint32_t remote_cache_rpc_timeout_ms;
  uint32_t remote_prefetch_rpc_timeout_ms;
};

struct RemoteBlockCacheOption {
  RemoteBlockCacheOption();

  std::string cache_group;
  uint32_t load_members_interval_ms;
  stub::common::MdsOption mds_option;

  // remote access
  RemoteNodeOption remote_node_option;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CONFIG_REMOTE_CACHE_H_
