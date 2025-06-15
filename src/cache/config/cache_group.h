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

#ifndef DINGOFS_SRC_CACHE_CONFIG_DINGO_CACHE_H_
#define DINGOFS_SRC_CACHE_CONFIG_DINGO_CACHE_H_

#include <gflags/gflags.h>

#include "cache/config/block_cache.h"
#include "cache/config/common.h"

namespace dingofs {
namespace cache {

DECLARE_string(group_name);
DECLARE_string(listen_ip);
DECLARE_uint32(listen_port);
DECLARE_uint32(group_weight);
DECLARE_uint32(max_range_size_kb);
DECLARE_string(metadata_filepath);
DECLARE_uint32(send_heartbeat_interval_ms);

struct CacheGroupNodeOption {
  CacheGroupNodeOption();

  std::string group_name;
  std::string listen_ip;
  uint32_t listen_port;
  uint32_t group_weight;
  std::string metadata_filepath;
  stub::common::MdsOption mds_option;

  BlockCacheOption block_cache_option;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CONFIG_DINGO_CACHE_H_
