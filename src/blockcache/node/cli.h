/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_BLOCKCACHE_NODE_CLI_H_
#define DINGOFS_BLOCKCACHE_NODE_CLI_H_

#include <vector>

#include "blockcache/utils/flags.h"

namespace dingofs {
namespace blockcache {

inline const std::vector<FlagSection> kNodeSections = {
    {"RUNTIME OPTIONS", {}, {"blockcache/core/"}},
    {"NODE OPTIONS",
     {"conf"},
     {"blockcache/node/", "blockcache/common/mds_client"}},
    {"NETWORK OPTIONS", {}, {"blockcache/net/", "blockcache/infiniband/"}},
    {"BLOCK CACHE OPTIONS", {}, {"blockcache/block/"}},
    {"CACHE STORE OPTIONS", {}, {"blockcache/store/"}},
    {"OBJECT STORAGE OPTIONS",
     {},
     {"blockcache/object/", "common/options/blockaccess"}},
    {"OTHER OPTIONS", {"log_dir", "log_level", "log_v"}},
};

inline const FlagParser::Usage kNodeUsage = {
    .program = "dingo-cache",
    .usage = "  dingo-cache [OPTIONS] --id <uuid> --listen_ip <ip>",
    .examples =
        "  $ dingo-cache --id=85a4b352-... --listen_ip=10.0.0.2\n"
        "  $ dingo-cache --conf cache.conf --daemonize\n",
    .sections = kNodeSections,
    .essential = {"id", "listen_ip", "listen_port", "use_rdma",
                  "cache_rdma_device", "cache_rdma_port_num",
                  "rdma_message_bytes", "rdma_max_inflight_rpcs",
                  "rdma_max_connections", "shards", "pin_cpu", "daemonize",
                  "conf", "mds_addrs", "group_name", "cache_dir",
                  "cache_size_mb", "buffer_pool_mb", "log_dir"},
    .required = {"id", "listen_ip"},
    .uuid_flag = "id",
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NODE_CLI_H_
