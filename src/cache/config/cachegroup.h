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
 * Created Date: 2025-06-20
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CONFIG_CACHEGROUP_H_
#define DINGOFS_SRC_CACHE_CONFIG_CACHEGROUP_H_

#include <gflags/gflags_declare.h>

#include "cache/config/blockcache.h"
#include "stub/common/config.h"

namespace dingofs {
namespace cache {

// Speficied logging directory for glog and access log.
DECLARE_string(logdir);

// Sets glog logging level: 0, 1, 2 and etc
DECLARE_int32(loglevel);

// Sets which group this cache node belongs to.
DECLARE_string(group_name);

// Sets the IP address to listen on for this cache group node.
DECLARE_string(listen_ip);

// Sets the port to listen on for this cache group node.
DECLARE_uint32(listen_port);

// Sets the weight of this cache group node, used for consistent hashing.
DECLARE_uint32(group_weight);

// Retrive the whole block if length of range request is larger than this value.
DECLARE_uint32(max_range_size_kb);

// Set the filepath to store metadata of cache group node.
DECLARE_string(metadata_filepath);

// Sets the interval to send heartbeat to MDS in milliseconds.
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

#endif  // DINGOFS_SRC_CACHE_CONFIG_CACHEGROUP_H_
