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

#ifndef DINGOFS_SRC_OPTIONS_CACHE_CACHE_GROUP_NODE_H_
#define DINGOFS_SRC_OPTIONS_CACHE_CACHE_GROUP_NODE_H_

#include "options/cache/block_cache.h"
#include "options/client/rpc.h"
#include "options/options.h"

namespace dingofs {
namespace options {
namespace cache {

class CacheGroupNodeOption : public BaseOption {
  BIND_string(group_name, "default", "");
  BIND_string(listen_ip, "127.0.0.1", "");
  BIND_uint32(listen_port, 9301, "");
  BIND_uint32(group_weight, 100, "");
  BIND_uint32(max_range_size_kb, 256, "");
  BIND_string(metadata_filepath, "/var/log/cache_group_meta", "");
  BIND_uint32(load_members_interval_ms, 1000, "");

  BIND_suboption(mds_rpc_option, "mds_rpc", client::MDSRPCOption);
  BIND_suboption(block_cache_option, "block_cache", BlockCacheOption);
};

}  // namespace cache
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CACHE_CACHE_GROUP_NODE_H_
